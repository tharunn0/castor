package main

import (
	"log"
	"net"

	castorv1 "github.com/tharunn0/castor/api/gen/go/castor/v1"
	"github.com/tharunn0/castor/internal/data/server"
	"google.golang.org/grpc"
)

var logPref string = "[ data-svc-1 ]"

func main() {

	log.Println(logPref, "starting service...")

	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	dataServer := server.New()

	castorv1.RegisterDataServiceServer(grpcServer, dataServer)

	log.Println(logPref, "service has started...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}

}
