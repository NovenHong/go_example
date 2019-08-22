package main

import(
	"google.golang.org/grpc"
	pb "main/helloworld"
	"log"
	"context"
	"time"
	"os"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/credentials"
)

const (
	address     = "localhost:50051"
	defaultName = "world"
)

func main() {

	creds, err := credentials.NewClientTLSFromFile("my_authorized/server.pem", "www.test.com")

	opts := []grpc.DialOption{
		grpc.WithPerRPCCredentials(oauth.NewOauthAccess(GetToken())),
		//grpc.WithInsecure(),
		grpc.WithTransportCredentials(creds),
	}

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	name := defaultName
	if len(os.Args) > 1 {
		name = os.Args[1]
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.SayHello(ctx, &pb.HelloRequest{Name: name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.Message)

	r2, err := c.SayHelloAgain(ctx, &pb.HelloRequest{Name: name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r2.Message)

	r3,err := c.ListUsers(ctx,&pb.HelloRequest{Name: name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

	for _,user := range r3.Data {
		log.Println(user)
	}

}

func GetToken() *oauth2.Token {
	return &oauth2.Token{
		AccessToken: "some-secret-token",
	}
}