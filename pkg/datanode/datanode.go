package datanode

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/Yaxhveer/distributed-fs/pkg/proto"
	"google.golang.org/grpc"
)

type DataNode struct {
	pb.UnimplementedDataNodeServiceServer
	address string
	storage map[string][]byte
	mutex   sync.RWMutex
}

func NewDataNode(addr string) *DataNode {
	return &DataNode{
		address: addr,
		storage: make(map[string][]byte),
	}
}

func InitialiseDataNode(addr string) {
	server := grpc.NewServer()
	node := NewDataNode(addr)
	pb.RegisterDataNodeServiceServer(server, node)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Println("DataNode running on port:", addr)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (dn *DataNode) StoreChunk(ctx context.Context, chunk *pb.Chunk) (*pb.Status, error) {
	dn.mutex.Lock()
	defer dn.mutex.Unlock()

	dn.storage[chunk.ChunkId] = chunk.Data
	log.Printf("Stored chunk: %s, in datanode: %s\n", chunk.ChunkId, dn.address)
	return &pb.Status{Message: "Chunk Stored", Success: true}, nil
}

func (dn *DataNode) RetrieveChunk(ctx context.Context, req *pb.ChunkRequest) (*pb.Chunk, error) {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()

	data, exists := dn.storage[req.ChunkId]
	if !exists {
		return nil, fmt.Errorf("chunk %s not found", req.ChunkId)
	}
	return &pb.Chunk{ChunkId: req.ChunkId, Data: data}, nil
}

func (dn *DataNode) Heartbeat(ctx context.Context, req *pb.DataNodeInfo) (*pb.Status, error) {
	log.Printf("Received heartbeat from Master for datanode %s\n", dn.address)
	return &pb.Status{Message: "Alive", Success: true}, nil
}
