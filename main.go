package main

import (
	"log"
	"os"

	"github.com/Yaxhveer/distributed-fs/pkg/cli"
)

func main() {
	rootCmd := cli.Root()
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
		os.Exit(1)
	}
}