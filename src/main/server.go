package main

import (
	"context"
	"net"
	"log"
	"strings"
	"crypto/tls"
	"google.golang.org/grpc"
	pb "main/helloworld"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

const (
	port = ":50051"
)

type server struct{}

func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.Name)
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

func (s *server) SayHelloAgain(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.Name)
	return &pb.HelloReply{Message: "Hello again " + in.Name}, nil
}

func (s *server) ListUsers(ctx context.Context, in *pb.HelloRequest) (*pb.ListUserResult, error) {
	log.Printf("Received: %v", in.Name)

	return &pb.ListUserResult{
		Code : 0,
		Message : "success",
		Data : []*pb.UserResult{
			&pb.UserResult{
				Id : 1,
				Username : "Nana",
				Password : "123456",
			 },
			 &pb.UserResult{
				Id : 2,
				Username : "King PineApple",
				Password : "123456",
			 },
		},
	},nil
}

func main()  {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	cert, err := tls.LoadX509KeyPair("my_authorized/server.pem", "my_authorized/server.key")
	if err != nil {
		log.Fatalf("failed to load key pair: %s", err)
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(EnsureValidToken),
		grpc.Creds(credentials.NewServerTLSFromCert(&cert)),
	}

	s := grpc.NewServer(opts...)
	pb.RegisterGreeterServer(s, &server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func EnsureValidToken(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "missing metadata")
	}

	if !Valid(md["authorization"]){
		return nil, status.Errorf(codes.Unauthenticated, "invalid token")
	}

	return handler(ctx,req)
}

func Valid(authorization []string) bool {
	if len(authorization) == 0 {
		return false
	}

	log.Printf("authorization:%s",authorization[0])

	token := strings.TrimPrefix(authorization[0],"Bearer ")

	return token == "some-secret-token"
}