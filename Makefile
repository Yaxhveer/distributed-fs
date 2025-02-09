protoc:
	protoc --go_out=. --go-grpc_out=. ./pkg/proto/dfs.proto

run:
	go run .

build:
	go build -o df .