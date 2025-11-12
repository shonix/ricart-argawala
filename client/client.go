package client

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	pb "ricart-argawala/grpc"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedNodeServer
	id          string
	addr        string
	peers       []string
	connections map[string]pb.Node_MessageStreamClient
	mu          sync.Mutex
}

func (n *Node) messageStream(Stream pb.Node_MessageStreamServer) error {
	for {
		msg, err := Stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			log.Printf("Error receiving message: %v", n.id, err)
			return err
		}

		log.Printf("Received message: %v", n.id, msg.From, msg.Message, msg.LamportTimestamp)

		ack := &pb.Ack{
			From:   n.id,
			Status: "recieved",
		}

		if err := Stream.Send(ack); err != nil {
			log.Printf("Error sending message: %v", n.id, err)
			return err
		}
	}
}

func main() {
	id := flag.String("id", "", "node id")
	addr := flag.String("addr", "localhost:50051", "server address")
	flag.Parse()

	node := &Node{
		id:          *id,
		addr:        *addr,
		peers:       []string{":50052", ":50053"},
		connections: make(map[string]pb.Node_MessageStreamClient),
	}

	go node.startServer()
	go node.connectToPeers()

	for {
		msg := &pb.Message{
			From:             node.id,
			Message:          fmt.Sprintf("Hello, %s!", node.id),
			LamportTimestamp: time.Now().Unix(),
		}
		fmt.Println(msg)
		//node.broadcast(msg)
		time.Sleep(5 * time.Second)
	}
}

func (n *Node) startServer() {
	lis, err := net.Listen("tcp", n.addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterNodeServer(s, n)
	log.Printf("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (n *Node) connectToPeers() {
	for _, peer := range n.peers {
		conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Error connecting to peer: %v", err)
			continue
		}

		client := pb.NewNodeClient(conn)
		stream, err := client.MessageStream(context.Background())
		if err != nil {
			log.Printf("Error connecting to peer: %v", err)
			continue
		}

		n.mu.Lock()
		n.connections[peer] = stream
		n.mu.Unlock()

		go n.listenForAcks(peer, stream)
		log.Printf("Connected to peer %v", n.id, peer)
	}
}

func (n *Node) listenForAcks(peer string, stream pb.Node_MessageStreamClient) {
	for {
		ack, err := stream.Recv()
		if err != nil {
			log.Printf("[%s] ack recv from %s failed: %v", n.id, peer, err)
			return
		}

		log.Printf("[%s] got ack from %s: %s", n.id, ack.From, ack.Status)
	}
}

//func (n *Node) broadcast(msg *pb.Message) {
//	n.mu.Lock()
//	peers := make([]string, len(n.peers))
//	}
//}

func (n *Node) SendMessage(peer string, msg *pb.Message) {
	n.mu.Lock()
	stream, ok := n.connections[peer]
	n.mu.Unlock()

	if !ok {
		log.Printf("[%s] No stream for peer %s", n.id, peer)
		return
	}

	if err := stream.Send(msg); err != nil {
		log.Printf("Error sending message: %v", n.id, err)
	} else {
		log.Printf("Sent message: %v", n.id, msg.From, msg.Message, msg.LamportTimestamp)
	}

}
