package cli

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	pb "github.com/Yaxhveer/distributed-fs/pkg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func registerNode(masterAddr, datanodeAddr string) error {
	client, err := createGRPCClient(masterAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	defer client.Close()

	masterClient := pb.NewMasterServiceClient(client)
	status, err := masterClient.RegisterDataNode(context.Background(), &pb.DataNodeInfo{Address: datanodeAddr})
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}
	log.Println(status.Message)
	return nil
}

func storeFile(masterAddr, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("could not read file: %w", err)
	}

	fileName := filepath.Base(path)
	client, err := createGRPCClient(masterAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	defer client.Close()

	masterClient := pb.NewMasterServiceClient(client)
	status, err := masterClient.UploadFile(context.Background(), &pb.FileUploadRequest{Filename: fileName, Data: data})
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}
	log.Println(status.Message)
	return nil
}

func retrieveFile(masterAddr, fileName, outputDir string) error {
	client, err := createGRPCClient(masterAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %w", err)
	}
	defer client.Close()

	masterClient := pb.NewMasterServiceClient(client)
	resp, err := masterClient.GetFile(context.Background(), &pb.FileGetRequest{Filename: fileName})
	if err != nil {
		return fmt.Errorf("retrieval failed: %w", err)
	}

	outputPath := fmt.Sprintf("%s/%s", outputDir, fileName)
	if err := os.WriteFile(outputPath, resp.Data, os.ModePerm); err != nil {
		return fmt.Errorf("failed to save file: %w", err)
	}

	log.Printf("File saved at %s", outputPath)
	return nil
}

func createGRPCClient(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
