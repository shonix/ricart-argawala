package main

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
	lamport     int64
}

func (n *Node) MessageStream(stream pb.Node_MessageStreamServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Printf("[%s] Error receiving message: %v", n.id, err)
			return err
		}

		// Update Lamport clock: max(current, received) + 1
		n.mu.Lock()
		if msg.LamportTimestamp > n.lamport {
			n.lamport = msg.LamportTimestamp
		}
		n.lamport++
		n.mu.Unlock()

		log.Printf("[%s] Received from %s: %s (Lamport: %d)", n.id, msg.From, msg.Message, msg.LamportTimestamp)

		// Send acknowledgment
		ack := &pb.Ack{
			From:   n.id,
			Status: "received",
		}
		if err := stream.Send(ack); err != nil {
			log.Printf("[%s] Error sending ack: %v", n.id, err)
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
		peers:       []string{":50052", ":50053", ":50054"},
		connections: make(map[string]pb.Node_MessageStreamClient),
	}

	go node.startServer()

	time.Sleep(2 * time.Second)

	go node.connectToPeers()

	for {
		msg := &pb.Message{
			From:             node.id,
			Message:          fmt.Sprintf("Hello from %s!", node.id),
			LamportTimestamp: time.Now().Unix(),
		}
		node.broadcast(msg)

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

//	func (n *Node) connectToPeers() {
//		for _, peer := range n.peers {
//			conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
//			if err != nil {
//				log.Printf("Error connecting to peer: %v", err)
//				continue
//			}
//
//			client := pb.NewNodeClient(conn)
//			stream, err := client.MessageStream(context.Background())
//			if err != nil {
//				log.Printf("Error connecting to peer: %v", err)
//				continue
//			}
//
//			n.mu.Lock()
//			n.connections[peer] = stream
//			n.mu.Unlock()
//
//			go n.listenForAcks(peer, stream)
//			log.Printf("Connected to peer %v", n.id, peer)
//		}
//	}

func (n *Node) connectToPeers() {
	for {
		for _, peer := range n.peers {
			n.mu.Lock()
			_, connected := n.connections[peer]
			n.mu.Unlock()
			if connected {
				continue
			}

			// Attempt to dial the peer
			conn, err := grpc.Dial(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("[%s] failed to dial %s: %v", n.id, peer, err)
				continue
			}

			client := pb.NewNodeClient(conn)

			// Persistent context for the stream (do NOT cancel)
			stream, err := client.MessageStream(context.Background())
			if err != nil {
				log.Printf("[%s] failed to open stream to %s: %v", n.id, peer, err)
				_ = conn.Close()
				continue
			}

			n.mu.Lock()
			n.connections[peer] = stream
			n.mu.Unlock()

			go n.listenForAcks(peer, stream)
			log.Printf("[%s] connected to peer %s", n.id, peer)
		}

		// Sleep a bit to avoid busy-looping
		time.Sleep(3 * time.Second)
	}
}

//func (n *Node) listenForAcks(peer string, stream pb.Node_MessageStreamClient, cc *grpc.ClientConn) {
//	for {
//		ack, err := stream.Recv()
//		if err != nil {
//			log.Printf("[%s] ack recv from %s failed: %v", n.id, peer, err)
//
//			// remove entry so connectToPeers will retry
//			n.mu.Lock()
//			delete(n.connections, peer)
//			n.mu.Unlock()
//
//			// close the underlying client connection
//			if cc != nil {
//				_ = cc.Close()
//			}
//			return
//		}
//		log.Printf("[%s] got ack from %s: %s", n.id, ack.From, ack.Status)
//	}
//}

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

func (n *Node) SendMessage(peer string, msg *pb.Message) {
	n.mu.Lock()
	// Increment Lamport on send
	n.lamport++
	msg.LamportTimestamp = n.lamport

	stream, ok := n.connections[peer]
	n.mu.Unlock()

	if !ok {
		log.Printf("[%s] No stream for peer %s", n.id, peer)
		return
	}

	if err := stream.Send(msg); err != nil {
		log.Printf("[%s] Error sending message to %s: %v", n.id, peer, err)
	} else {
		log.Printf("[%s] Sent message to %s: %s (Lamport: %d)", n.id, peer, msg.Message, msg.LamportTimestamp)
	}
}

func (n *Node) broadcast(msg *pb.Message) {
	n.mu.Lock()
	peers := make([]string, 0, len(n.connections))
	for peer := range n.connections {
		peers = append(peers, peer)
	}
	n.mu.Unlock()

	for _, peer := range peers {
		go n.SendMessage(peer, msg)
	}
}
