# Distributed File Storage CLI

## Overview
The **Distributed File Storage CLI** (`df`) is a command-line tool for managing a distributed file system. It enables users to start master and data nodes, register data nodes, store files, and retrieve files in a distributed environment using gRPC. The system also supports **replication** to enhance reliability and utilizes **Chord-DHT** for efficient distributed file lookup.

## Features
- **Master Node Management:** Start a new master node to coordinate file storage and retrieval.
- **Data Node Management:** Start a new data node to store file chunks.
- **Node Registration:** Register data nodes with the master.
- **File Storage:** Store files in the distributed system.
- **File Retrieval:** Retrieve stored files from the system.
- **gRPC Communication:** Uses gRPC for efficient RPC-based communication between master and data nodes.
- **Replication:** Ensures redundancy by replicating file chunks across multiple data nodes.
- **Chord-DHT Integration:** Uses a Distributed Hash Table (DHT) based on the Chord protocol for efficient file location.

## Installation
Ensure you have Go installed. Then, clone the repository and build the CLI:

```sh
# Clone the repository
git clone https://github.com/Yaxhveer/distributed-fs.git
cd distributed-fs

# Install dependencies
go mod tidy

# Build the CLI
make build
```

## Usage
The CLI supports commands for both **Master Nodes** and **Data Nodes**.

### Master Node Commands

#### Start a new Master Node
```sh
./df master new -a 127.0.0.1:5000
```
- `-a, --address`: Address where the master node will run (default: `127.0.0.1:5000`).

#### Register a Data Node
```sh
./df master register -m 127.0.0.1:5000 -d 127.0.0.1:6000
```
- `-m, --master`: Address of the master node.
- `-d, --datanodeAddress`: Address of the data node to register.

#### Store a File
```sh
./df master store -m 127.0.0.1:5000 -f /path/to/file
```
- `-m, --master`: Address of the master node.
- `-f, --file`: Path to the file.

#### Retrieve a File
```sh
./df master retrieve -m 127.0.0.1:5000 -o ./output -f filename.txt
```
- `-m, --master`: Address of the master node.
- `-o, --path`: Destination/Output directory for retrieved file.
- `-f, --file`: Name of the file to retrieve.

### Data Node Commands

#### Start a new Data Node
```sh
./df datanode new -a 127.0.0.1:6000
```
- `-a, --address`: Address where the data node will run (default: `127.0.0.1:6000`).

## Architecture
### Components
1. **Master Node**
   - Manages metadata of stored files.
   - Coordinates file distribution among data nodes.
   - Handles file retrieval requests.
   - Replicates file chunks across multiple data nodes.
2. **Data Node**
   - Stores file chunks.
   - Responds to requests from the master node.
   - Participates in the Chord-DHT for distributed file lookup.

## Implementation Details
- **gRPC-based communication** between master and data nodes.
- **File chunking** for efficient storage and retrieval.
- **Replication** ensures redundancy and fault tolerance.
- **Chord-DHT** provides a scalable, efficient distributed lookup service.
- **Concurrency-safe storage** using mutex locks in data nodes.

## Contributing
Feel free to submit issues or pull requests to improve the project!

## Author
[Yaxhveer](https://github.com/Yaxhveer)