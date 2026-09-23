package main

import (
	"log"
	"net"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/data/config"
	"github.com/tharunn0/castor/internal/data/server"
	"github.com/tharunn0/castor/internal/data/storage"
	"google.golang.org/grpc"
)

var (
	logPref      string = "[ data-svc-1 ]"
	rootDir      string = "data"
	maxChunkSize int    = 4 << 20
)

func main() {

	cfg := config.Load()

	log.Println(logPref, "starting service...")

	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()

	store, err := storage.New(rootDir, maxChunkSize)
	if err != nil {
		log.Fatal(err)
	}

	dataServer := server.New(store, cfg)

	castorv1.RegisterDataServiceServer(grpcServer, dataServer)

	log.Println(logPref, "service has started...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}

}
