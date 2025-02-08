protoc:
	protoc --go_out=. --go-grpc_out=. ./pkg/proto/dfs.proto