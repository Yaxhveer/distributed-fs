package cli

import (
	"log"

	"github.com/Yaxhveer/distributed-fs/pkg/datanode"
	"github.com/Yaxhveer/distributed-fs/pkg/master"
	"github.com/spf13/cobra"
)

func Root() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "ds",
		Short: "Distributed File Storage CLI",
	}

	masterCmd := &cobra.Command{Use: "master", Short: "Manage Master Node"}
	masterCmd.AddCommand(newMasterCmd(), registerNodeCmd(), storeFileCmd(), retrieveFileCmd())

	dataNodeCmd := &cobra.Command{Use: "datanode", Short: "Manage Data Node"}
	dataNodeCmd.AddCommand(newDataNodeCmd())

	rootCmd.AddCommand(masterCmd, dataNodeCmd)
	return rootCmd
}

func newMasterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "new",
		Short: "Start a new Master Node",
		Run: func(cmd *cobra.Command, args []string) {
			addr, _ := cmd.Flags().GetString("address")
			log.Printf("Starting Master Node on %s...", addr)
			master.InitialiseMasterNode(addr)
		},
	}
	cmd.Flags().StringP("address", "a", "127.0.0.1:5000", "Master node address")
	return cmd
}

func registerNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "register",
		Short: "Register a DataNode",
		Run: func(cmd *cobra.Command, args []string) {
			masterAddr, _ := cmd.Flags().GetString("master")
			dataNodeAddr, _ := cmd.Flags().GetString("datanode")

			log.Printf("Registering DataNode %s with Master %s...", dataNodeAddr, masterAddr)
			if err := registerNode(masterAddr, dataNodeAddr); err != nil {
				log.Fatalf("Failed to register DataNode: %v", err)
			}
		},
	}
	cmd.Flags().StringP("master", "m", "127.0.0.1:5000", "Master address")
	cmd.Flags().StringP("datanode", "d", "127.0.0.1:6000", "DataNode address")
	return cmd
}

func storeFileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "store",
		Short: "Store a file",
		Run: func(cmd *cobra.Command, args []string) {
			masterAddr, _ := cmd.Flags().GetString("master")
			filePath, _ := cmd.Flags().GetString("file")

			log.Printf("Storing file %s...", filePath)
			if err := storeFile(masterAddr, filePath); err != nil {
				log.Fatalf("Failed to store file: %v", err)
			}
		},
	}
	cmd.Flags().StringP("master", "m", "127.0.0.1:5000", "Master address")
	cmd.Flags().StringP("file", "f", "example.txt", "File to store")
	return cmd
}

func retrieveFileCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retrieve",
		Short: "Retrieve a file",
		Run: func(cmd *cobra.Command, args []string) {
			masterAddr, _ := cmd.Flags().GetString("master")
			filePath, _ := cmd.Flags().GetString("file")
			outputDir, _ := cmd.Flags().GetString("output")

			log.Printf("Retrieving file %s...", filePath)
			if err := retrieveFile(masterAddr, filePath, outputDir); err != nil {
				log.Fatalf("Failed to retrieve file: %v", err)
			}
		},
	}
	cmd.Flags().StringP("master", "m", "127.0.0.1:5000", "Master address")
	cmd.Flags().StringP("file", "f", "example.txt", "File to retrieve")
	cmd.Flags().StringP("output", "o", "./out", "Output directory")
	return cmd
}

func newDataNodeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "new",
		Short: "Start a new Data Node",
		Run: func(cmd *cobra.Command, args []string) {
			addr, _ := cmd.Flags().GetString("address")
			log.Printf("Starting Data Node on %s...", addr)
			datanode.InitialiseDataNode(addr)
		},
	}
	cmd.Flags().StringP("address", "a", "127.0.0.1:6000", "DataNode address")
	return cmd
}
