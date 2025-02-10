package master

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	pb "github.com/Yaxhveer/distributed-fs/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	chunkSize         = 1024 * 1024 // 1MB chunks
	replicationFactor = 3           // 3 replicas
	heartbeatInterval = 10 * time.Second
	maxRetry          = 3
)

type MasterNode struct {
	pb.UnimplementedMasterServiceServer
	dataNodes []*node
	metadata  map[string][]string
	mutex     sync.Mutex
	address   string
}

type node struct {
	nodeId string
	addr   string
}

func NewMasterNode(addr string) *MasterNode {
	master := &MasterNode{
		dataNodes: make([]*node, 0),
		metadata:  make(map[string][]string),
		address:   addr,
	}
	go master.heartbeatRoutine()
	return master
}

func InitialiseMasterNode(addr string) {
	server := grpc.NewServer()
	master := NewMasterNode(addr)
	pb.RegisterMasterServiceServer(server, master)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("Master Node running on port %s", addr)

	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (m *MasterNode) heartbeatRoutine() {
	for {
		time.Sleep(heartbeatInterval)

		activeNodes := []*node{}
		for _, node := range m.dataNodes {
			address := node.addr

			err := retryWithBackoff(func() error {
				conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return fmt.Errorf("Failed to connect to DataNode: %v", err)
				}
				defer conn.Close()

				client := pb.NewDataNodeServiceClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				res, err := client.Heartbeat(ctx, &pb.DataNodeInfo{Address: address})
				if err != nil || !res.Success {
					return fmt.Errorf("Heartbeat failed for DataNode: %v", err)
				}
				return nil
			})

			if err == nil {
				log.Printf("DataNode %s is alive", address)
				activeNodes = append(activeNodes, node)
			} else {
				log.Printf("DataNode at %s is unresponsive. Removing it.", address)
			}
		}
		m.mutex.Lock()
		m.dataNodes = activeNodes
		m.mutex.Unlock()
	}
}

func (m *MasterNode) RegisterDataNode(ctx context.Context, info *pb.DataNodeInfo) (*pb.Status, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	nodeID := generateHash([]byte(info.Address))

	for _, existingNode := range m.dataNodes {
		if existingNode.nodeId == nodeID {
			log.Printf("Node %s is already registered.", info.Address)
			return &pb.Status{Message: "Node Already Registered", Success: true}, nil
		}
	}

	newNode := &node{
		nodeId: nodeID,
		addr:   info.Address,
	}
	m.dataNodes = append(m.dataNodes, newNode)
	m.sortNodes()
	log.Printf("Registered Data Node at %s with ID %s", info.Address, nodeID)

	successor := m.dataNodes[m.FindSuccessor([]byte(nodeID))]
	go m.migrateChunks(successor, newNode)

	return &pb.Status{Message: "Node Registered", Success: true}, nil
}

func (m *MasterNode) migrateChunks(fromNode *node, toNode *node) {
	var resp *pb.ChunkList
	err := retryWithBackoff(func() error {
		conn, err := grpc.NewClient(fromNode.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("Failed to connect to DataNode %s: %v", fromNode.addr, err)
		}
		defer conn.Close()

		client := pb.NewDataNodeServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err = client.ListChunks(ctx, &pb.DataNodeInfo{Address: fromNode.addr})
		if err != nil {
			return fmt.Errorf("Failed to get chunks from node %s: %v", fromNode.addr, err)
		}
		return nil
	})
	if err != nil {
		log.Printf("Failed to get chunk list from node %s: %v", fromNode.addr, err)
		return
	}

	var wg sync.WaitGroup
	for _, chunkID := range resp.ChunkIds {
		if chunkID < toNode.nodeId {
			wg.Add(1)
			go func(chunkID string) {
				defer wg.Done()
				err := retryWithBackoff(func() error {
					conn, err := grpc.NewClient(toNode.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						return fmt.Errorf("Failed to connect to DataNode %s: %v", toNode.addr, err)
					}
					defer conn.Close()
					client := pb.NewDataNodeServiceClient(conn)
					err = m.transferChunk(client, chunkID, fromNode, toNode)
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					log.Printf("Failed to connect: %v", err)
				}
			}(chunkID)
		}
	}

	wg.Wait()
	log.Printf("Finished migrating chunks from %s to %s", fromNode.addr, toNode.addr)
}

func (m *MasterNode) transferChunk(client pb.DataNodeServiceClient, chunkID string, fromNode *node, toNode *node) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	chunk, err := client.RetrieveChunk(ctx, &pb.ChunkRequest{ChunkId: chunkID})
	if err != nil {
		return fmt.Errorf("Failed to retrieve chunk %s from node %s: %v", chunkID, fromNode.addr, err)
	}

	err = sendChunkToDataNode(toNode.addr, chunkID, chunk.Data)
	if err != nil {
		return fmt.Errorf("Failed to store chunk %s on node %s: %v", chunkID, toNode.addr, err)
	}
	log.Printf("Migrated chunk %s from %s to %s", chunkID, fromNode.addr, toNode.addr)
	return nil
}

func (m *MasterNode) UploadFile(ctx context.Context, req *pb.FileUploadRequest) (*pb.Status, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.dataNodes) == 0 {
		return nil, fmt.Errorf("No active DataNodes available")
	}

	filename := req.Filename
	fileData := req.Data
	numChunks := (len(fileData) + chunkSize - 1) / chunkSize

	var chunkIDs []string
	var wg sync.WaitGroup

	for i := 0; i < numChunks; i++ {
		start, end := i*chunkSize, (i+1)*chunkSize
		if end > len(fileData) {
			end = len(fileData)
		}

		chunkData := fileData[start:end]
		chunkID := generateHash(chunkData)
		nodeIndex := m.FindSuccessor([]byte(chunkID))

		chunkIDs = append(chunkIDs, string(chunkID))

		// Replicate chunk across nodes
		for j := 0; j < replicationFactor; j++ {
			wg.Add(1)
			go func(replicaIndex int) {
				defer wg.Done()
				currNodeIndex := (nodeIndex + replicaIndex) % len(m.dataNodes)
				addr := m.dataNodes[currNodeIndex].addr
				err := sendChunkToDataNode(addr, string(chunkID), chunkData)
				if err != nil {
					log.Printf("Failed to upload chunk %s: %v", chunkID, err)
				}
			}(j)
		}
	}
	wg.Wait()

	m.metadata[filename] = chunkIDs
	log.Printf("Stored file %s in chunks: %v", filename, chunkIDs)

	return &pb.Status{Message: "File Uploaded", Success: true}, nil
}

func (m *MasterNode) GetFile(ctx context.Context, req *pb.FileGetRequest) (*pb.FileGetResponse, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.dataNodes) == 0 {
		return nil, fmt.Errorf("No active DataNodes available")
	}

	filename := req.Filename
	chunkIDs, exists := m.metadata[filename]
	if !exists {
		return nil, fmt.Errorf("File not found")
	}

	var data []byte
	for _, chunkID := range chunkIDs {
		nodeIndex := m.FindSuccessor([]byte(chunkID))
		addr := m.dataNodes[nodeIndex].addr
		chunk, err := retrieveChunkFromDataNode(addr, chunkID)
		if err != nil {
			return nil, err
		}
		data = append(data, chunk.Data...)
	}

	return &pb.FileGetResponse{Filename: filename, Data: data}, nil
}

func (m *MasterNode) sortNodes() {
	sort.Slice(m.dataNodes, func(i, j int) bool {
		return m.dataNodes[i].nodeId < m.dataNodes[j].nodeId
	})
}

func (m *MasterNode) FindSuccessor(key []byte) int {
	keyHex := hex.EncodeToString(key)

	for index, node := range m.dataNodes {
		if keyHex <= node.nodeId {
			return index
		}
	}
	if len(m.dataNodes) > 0 {
		return 0
	}
	return -1
}

func generateHash(key []byte) string {
	h := sha1.New()
	h.Write(key)
	return hex.EncodeToString(h.Sum(nil))
}

func retryWithBackoff(operation func() error) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetry; attempt++ {
		lastErr = operation()
		if lastErr == nil {
			return nil
		}
		log.Printf("Attempt %d failed: %v", attempt, lastErr)
		time.Sleep(time.Second * time.Duration(attempt))
	}
	return lastErr
}

func sendChunkToDataNode(address, chunkID string, chunkData []byte) error {
	return retryWithBackoff(func() error {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("Failed to connect to DataNode: %v", err)
		}
		defer conn.Close()

		client := pb.NewDataNodeServiceClient(conn)
		status, err := client.StoreChunk(context.Background(), &pb.Chunk{ChunkId: chunkID, Data: chunkData})
		if err != nil || !status.Success {
			return fmt.Errorf("Failed to store chunk %s: %v", chunkID, err)
		}
		log.Printf("Successfully stored chunk %s on DataNode %s", chunkID, address)
		return nil
	})
}

func retrieveChunkFromDataNode(address, chunkID string) (*pb.Chunk, error) {
	var chunk *pb.Chunk
	err := retryWithBackoff(func() error {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("Failed to connect to DataNode: %v", err)
		}
		defer conn.Close()

		client := pb.NewDataNodeServiceClient(conn)
		chunk, err = client.RetrieveChunk(context.Background(), &pb.ChunkRequest{ChunkId: chunkID})
		if err != nil {
			return fmt.Errorf("Failed to retrieve chunk %s: %v", chunkID, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return chunk, nil
}
