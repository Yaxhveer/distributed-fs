package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Yaxhveer/distributed-fs/pkg/datanode"
	"github.com/Yaxhveer/distributed-fs/pkg/master"

	pb "github.com/Yaxhveer/distributed-fs/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	
	masterAddr := ":8000"
	go master.InitialiseMasterNode(masterAddr)
	go datanode.InitialiseDataNode(":8001")
	
	time.Sleep(1000*time.Millisecond)
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        fmt.Printf("Failed to connect to DataNode: %s\n", err)
    }
    defer conn.Close()

    client := pb.NewMasterServiceClient(conn)
    status, err := client.RegisterDataNode(context.Background(), &pb.DataNodeInfo{Address: ":8001"})
    if err != nil {
        fmt.Printf("Failed to store chunk: %s\n", err)
    }
	fmt.Printf("Staus: %+v\n", status)
}