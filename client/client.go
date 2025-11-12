package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	pb "ricart-argawala/grpc"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type State int

const (
	RELEASED State = iota
	WANTED
	HELD
)

type Node struct {
	pb.UnimplementedNodeServer
	id               string
	addr             string
	peers            []string
	connections      map[string]pb.Node_MessageStreamClient
	mu               sync.Mutex
	lamport          int64
	state            State
	replyCount       int
	requestTimeStamp int64
	requestQueue     []pb.Message
	expectedReplies  int
}

func (n *Node) requestCriticalSection() {
	n.mu.Lock()
	n.state = WANTED
	n.lamport++
	n.requestTimeStamp = n.lamport
	n.replyCount = 0
	n.expectedReplies = len(n.connections)
	req := &pb.Message{
		From:             n.addr,
		Message:          "REQUEST",
		LamportTimestamp: n.lamport,
	}
	n.mu.Unlock()

	log.Printf("[%s] Sending req for CS (T=%d)", n.id, n.requestTimeStamp)
	n.broadcast(req)
}

func (n *Node) releaseCriticalSection() {
	n.mu.Lock()
	n.state = RELEASED
	n.replyCount = 0
	n.requestTimeStamp = 0
	queued := n.requestQueue
	n.requestQueue = nil
	n.mu.Unlock()

	log.Printf("[%s] Release CS, replying to %d queued requests", n.id, len(queued))

	for _, m := range queued {
		n.mu.Lock()
		n.lamport++
		ts := n.lamport
		n.mu.Unlock()

		reply := &pb.Message{
			From:             n.addr,
			Message:          "REPLY",
			LamportTimestamp: ts,
		}
		n.SendMessage(m.From, reply)
	}
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
		n.lamport = max(n.lamport, msg.LamportTimestamp) + 1
		localTimeStamp := n.lamport
		n.mu.Unlock()

		switch msg.Message {
		case "REQUEST":
			n.handleRequest(msg)
		case "REPLY":
			n.handleReply(msg)
		}

		log.Printf("[%s <- %s] %s (local=%d, msg=%d)", n.id, msg.From, msg.Message, localTimeStamp, msg.LamportTimestamp)

		if err := stream.Send(&pb.Ack{From: n.addr, Status: "received"}); err != nil {
			log.Printf("[%s] ack send err: %v", n.id, err)
			return err
		}
	}
}

func main() {
	id := flag.String("id", "", "node id")
	addr := flag.String("addr", "localhost:50051", "server address")
	flag.Parse()

	actualPeers := make([]string, 0)
	allPossiblePeers := []string{"localhost:50052", "localhost:50053", "localhost:50054"}

	for _, p := range allPossiblePeers {
		if p != *addr {
			actualPeers = append(actualPeers, p)
		}
	}
	node := &Node{
		id:          *id,
		addr:        *addr,
		peers:       actualPeers,
		connections: make(map[string]pb.Node_MessageStreamClient),
	}

	go node.startServer()

	time.Sleep(2 * time.Second)

	go node.connectToPeers()

	for {
		node.mu.Lock()
		connectedCount := len(node.connections)
		requiredCount := len(node.peers)
		node.mu.Unlock()

		if connectedCount == requiredCount {
			log.Printf("[%s] All %d peers connected. Starting CS loop.", node.id, requiredCount)
			break
		}
		log.Printf("[%s] Waiting for connections (%d/%d)...", node.id, connectedCount, requiredCount)
		time.Sleep(1 * time.Second)
	}

	for {
		node.requestCriticalSection()
		time.Sleep(10 * time.Second)
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

func (n *Node) listenForAcks(peer string, stream pb.Node_MessageStreamClient) {
	for {
		ack, err := stream.Recv()
		if err != nil {
			n.mu.Lock()
			delete(n.connections, peer)
			n.mu.Unlock()
			log.Printf("[%s] ack recv from %s failed: %v", n.id, peer, err)
			return
		}

		log.Printf("[%s] got ack from %s: %s", n.id, ack.From, ack.Status)
	}
}

func (n *Node) SendMessage(peer string, msg *pb.Message) {
	n.mu.Lock()
	stream, ok := n.connections[peer]
	n.mu.Unlock()

	if !ok {
		log.Printf("[%s] No stream for peer %s", n.id, peer)
		return
	}

	if err := stream.Send(msg); err != nil {
		log.Printf("[%s] send to %s failed %v", n.id, peer, err)
	} else {
		log.Printf("[%s -> %s] %s (T=%d)", n.id, peer, msg.Message, msg.LamportTimestamp)
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

func (n *Node) handleRequest(msg *pb.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == HELD || n.state == WANTED &&
		(n.requestTimeStamp < msg.LamportTimestamp ||
			(n.requestTimeStamp == msg.LamportTimestamp &&
				n.addr < msg.From)) {

		n.requestQueue = append(n.requestQueue, *msg)
		log.Printf("[%s] deferring request from %s", n.id, msg.From)
		return
	}

	n.lamport++
	reply := &pb.Message{
		From:             n.addr,
		Message:          "REPLY",
		LamportTimestamp: n.lamport,
	}

	go n.SendMessage(msg.From, reply)
	log.Printf("[%s] replied to request from %s", n.id, msg.From)
}

func (n *Node) handleReply(msg *pb.Message) {
	n.mu.Lock()
	n.replyCount++
	ready := n.state == WANTED && n.replyCount == n.expectedReplies
	n.mu.Unlock()

	if ready {
		n.mu.Lock()
		n.state = HELD
		n.lamport++
		n.mu.Unlock()
		log.Printf("[%s] Entering CS", n.addr)
		go n.criticalSection()
	}
}

func (n *Node) criticalSection() {
	log.Printf("[%s] **IN CRITICAL SECTION** (Lamport=%d)", n.id, n.lamport)
	time.Sleep(5 * time.Second)
	n.mu.Lock()
	n.lamport++
	n.mu.Unlock()
	log.Printf("[%s] **EXITING CRITICAL SECTION**", n.id)
	n.releaseCriticalSection()
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
