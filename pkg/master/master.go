package master

import (
	"bytes"
	"context"
	"crypto/sha1"
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
	heartbeatTimeout  = 5 * time.Second
)

type MasterNode struct {
	pb.UnimplementedMasterServiceServer
	dataNodes     [][]byte            // hash
	dataNodesAddr map[string]string   // hash -> addr
	metadata      map[string][]string // filename -> chunkIds
	mutex         sync.Mutex
	address       string
}

func NewMasterNode(addr string) *MasterNode {
	master := &MasterNode{
		dataNodes:     make([][]byte, 0),
		dataNodesAddr: make(map[string]string),
		metadata:      make(map[string][]string),
		address:       addr,
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
	log.Println("Master Node running on port:", addr)

	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// Heartbeat routine that pings DataNodes
func (m *MasterNode) heartbeatRoutine() {
	for {
		time.Sleep(heartbeatInterval)
		m.mutex.Lock()

		activeNodes := [][]byte{}
		for nodeID, address := range m.dataNodesAddr {
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Failed to connect to DataNode at %s: %v. Removing it.", address, err)
				delete(m.dataNodesAddr, nodeID)
				continue
			}

			client := pb.NewDataNodeServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
			res, err := client.Heartbeat(ctx, &pb.DataNodeInfo{Address: address})
			cancel()
			conn.Close()

			if err != nil || !res.Success {
				log.Printf("DataNode at %s is unresponsive. Removing it.", address)
				delete(m.dataNodesAddr, nodeID)
			} else {
				log.Printf("DataNode %s is alive", address)
				activeNodes = append(activeNodes, []byte(nodeID))
			}
		}
		m.dataNodes = activeNodes
		m.mutex.Unlock()
	}
}

func (m *MasterNode) RegisterDataNode(ctx context.Context, info *pb.DataNodeInfo) (*pb.Status, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	nodeID := generateHash([]byte(info.Address))
	m.dataNodes = append(m.dataNodes, nodeID)
	m.dataNodesAddr[string(nodeID)] = info.Address
	m.sortNodes()
	log.Printf("Registered Data Node at %s", info.Address)

	return &pb.Status{Message: "Node Registered", Success: true}, nil
}


// Store file (split into chunks and distribute)
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
		nodeIndex := m.FindSuccessor(chunkID)

		chunkIDs = append(chunkIDs, string(chunkID))

		// Replicate chunk across nodes
		for j := 0; j < replicationFactor; j++ {
			wg.Add(1)
			go func(replicaIndex int) {
				defer wg.Done()
				currNodeIndex := (nodeIndex + replicaIndex) % len(m.dataNodes)
				addr, exists := m.dataNodesAddr[string(m.dataNodes[currNodeIndex])]
				if !exists {
					return
				}
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

// Retrieve file
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
		addr, exists := m.dataNodesAddr[string(m.dataNodes[nodeIndex])]
		if !exists {
			return nil, fmt.Errorf("dataNode with file chunk does not exist")
		}
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
		return bytes.Compare(m.dataNodes[i], m.dataNodes[j]) < 0
	})
}

func (m *MasterNode) FindSuccessor(key []byte) int {
	for index, nodeID := range m.dataNodes {
		if bytes.Compare(key, nodeID) <= 0 {
			return index
		}
	}
	if len(m.dataNodes) > 0 {
		return 0
	}
	return -1
}


func generateHash(key []byte) []byte {
	h := sha1.New()
	h.Write(key)
	return h.Sum(nil)
}

func sendChunkToDataNode(address, chunkID string, chunkData []byte) error {
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
	return nil
}

func retrieveChunkFromDataNode(address, chunkID string) (*pb.Chunk, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to DataNode: %v", err)
	}
	defer conn.Close()

	client := pb.NewDataNodeServiceClient(conn)
	return client.RetrieveChunk(context.Background(), &pb.ChunkRequest{ChunkId: chunkID})
}
