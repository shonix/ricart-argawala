package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	pb "ricart-argawala/grpc"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// node states for the algorithm
const (
	RELEASED = iota // not interested in CS
	WANTED          // wants to enter CS
	HELD            // currently in CS
)

// Request for the critical section
type Request struct {
	ts     int64
	nodeId string
}

type Node struct {
	pb.UnimplementedNodeServer
	id          string
	addr        string
	peers       []string
	connections map[string]pb.Node_MessageStreamClient
	mu          sync.Mutex
	lamport     int64

	// algorithm state
	state        int       // RELEASED, WANTED, or HELD
	reqQueue     []Request // pending requests sorted by timestamp
	deferredList []string  // nodes we need to reply to later
	replies      int       // how many replies we got
	numPeers     int       // total peers
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

		// check what type of message it is
		switch msg.MessageType {
		case pb.MessageType_REQUEST:
			n.onRequest(msg)
		case pb.MessageType_REPLY:
			n.onReply(msg)
		case pb.MessageType_RELEASE:
			n.onRelease(msg)
		}

		// send ack
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

// when we get a request from another node
func (n *Node) onRequest(msg *pb.Message) {
	log.Printf("[%s] Received REQUEST from %s (ts=%d)", n.id, msg.From, msg.LamportTimestamp)

	n.mu.Lock()
	// add to queue
	n.addToQueue(msg.LamportTimestamp, msg.From)

	// figure out if we should reply now
	replyNow := false
	switch n.state {
	case RELEASED:
		replyNow = true
	case WANTED:
		// check timestamps to see who has priority
		if msg.LamportTimestamp > n.lamport {
			replyNow = false
		} else if msg.LamportTimestamp < n.lamport {
			replyNow = true
		} else {
			// timestamps are equal, compare node ids
			replyNow = msg.From > n.id
		}
	}

	if replyNow {
		log.Printf("[%s] Sending REPLY to %s", n.id, msg.From)
		n.mu.Unlock() // Release lock before calling sendReply to avoid deadlock
		n.sendReply(msg.From)
	} else {
		log.Printf("[%s] Deferring reply to %s", n.id, msg.From)
		n.deferredList = append(n.deferredList, msg.From)
		n.mu.Unlock()
	}
}

// got a reply from another node
func (n *Node) onReply(msg *pb.Message) {
	log.Printf("[%s] got reply from %s", n.id, msg.From)

	n.mu.Lock()
	n.replies++
	log.Printf("[%s] replies so far: %d out of %d", n.id, n.replies, n.numPeers)
	n.mu.Unlock()
}

// someone released the CS
func (n *Node) onRelease(msg *pb.Message) {
	log.Printf("[%s] %s released CS", n.id, msg.From)

	n.mu.Lock()
	defer n.mu.Unlock()

	// remove from queue
	n.removeFromQueue(msg.From)
}

// send reply back
func (n *Node) sendReply(toNode string) {
	reply := &pb.Message{
		MessageType:      pb.MessageType_REPLY,
		From:             n.id,
		LamportTimestamp: n.lamport,
		Message:          "reply to " + toNode,
	}

	// just broadcast it - all peers get it but only requester uses it
	// TODO: could optimize by mapping node IDs to connections
	n.broadcast(reply)
}

func main() {
	id := flag.String("id", "", "node id")
	addr := flag.String("addr", "localhost:50051", "server address")
	flag.Parse()

	peers := []string{":50052", ":50053", ":50054"}
	node := &Node{
		id:          *id,
		addr:        *addr,
		peers:       peers,
		connections: make(map[string]pb.Node_MessageStreamClient),

		// init algorithm state
		state:        RELEASED,
		reqQueue:     make([]Request, 0),
		deferredList: make([]string, 0),
		replies:      0,
		numPeers:     len(peers),
	}

	go node.startServer()

	time.Sleep(2 * time.Second)

	go node.connectToPeers()

	// wait until we have all connections
	log.Printf("[%s] waiting for all %d peers to connect...", node.id, node.numPeers)
	for {
		node.mu.Lock()
		connected := len(node.connections)
		node.mu.Unlock()

		if connected >= node.numPeers {
			log.Printf("[%s] now all the peers are connected -- starting algorithm", node.id)
			break
		}
		time.Sleep(1 * time.Second)
	}

	// keep requesting CS periodically
	for {
		node.requestCS()
		node.enterCS()
		node.releaseCS()

		// wait a bit before next request
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
			conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

		// sleep for a bit to avoid busy loop
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

// try to get access to critical section
func (n *Node) requestCS() {
	n.mu.Lock()
	n.state = WANTED
	n.lamport++
	reqTs := n.lamport
	n.replies = 0
	// put our request in queue too
	n.addToQueue(reqTs, n.id)
	n.mu.Unlock()

	log.Printf("[%s] requesting CS with timestamp %d", n.id, reqTs)

	// send request to everyone
	req := &pb.Message{
		MessageType:      pb.MessageType_REQUEST,
		From:             n.id,
		LamportTimestamp: reqTs,
		Message:          "request",
	}
	n.broadcast(req)

	// wait for replies
	for {
		n.mu.Lock()
		got := n.replies >= n.numPeers
		n.mu.Unlock()

		if got {
			log.Printf("[%s] got all replies!", n.id)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// enter CS
func (n *Node) enterCS() {
	n.mu.Lock()
	n.state = HELD
	n.mu.Unlock()

	log.Printf("[%s] *** entering CS ***", n.id)

	// do work in CS
	log.Printf("[%s] doing work", n.id)
	time.Sleep(2 * time.Second)

	log.Printf("[%s] *** done ***", n.id)
}

// release CS
func (n *Node) releaseCS() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = RELEASED

	// remove ourselves from queue
	n.removeFromQueue(n.id)

	// send replies we deferred
	if len(n.deferredList) > 0 {
		log.Printf("[%s] sending %d deferred replies", n.id, len(n.deferredList))
	}
	for _, nodeId := range n.deferredList {
		n.sendReply(nodeId)
	}
	n.deferredList = make([]string, 0)

	// tell everyone we're done
	release := &pb.Message{
		MessageType:      pb.MessageType_RELEASE,
		From:             n.id,
		LamportTimestamp: n.lamport,
		Message:          "release",
	}
	n.broadcast(release)

	log.Printf("[%s] released CS", n.id)
}

// add to queue and keep it sorted
func (n *Node) addToQueue(timestamp int64, nodeId string) {
	n.reqQueue = append(n.reqQueue, Request{
		ts:     timestamp,
		nodeId: nodeId,
	})
	// sort by timestamp first, then by node id
	sort.Slice(n.reqQueue, func(i, j int) bool {
		if n.reqQueue[i].ts == n.reqQueue[j].ts {
			return n.reqQueue[i].nodeId < n.reqQueue[j].nodeId
		}
		return n.reqQueue[i].ts < n.reqQueue[j].ts
	})
}

// remove from queue
func (n *Node) removeFromQueue(nodeId string) {
	for i, r := range n.reqQueue {
		if r.nodeId == nodeId {
			n.reqQueue = append(n.reqQueue[:i], n.reqQueue[i+1:]...)
			return
		}
	}
}
