package main

import (
	pb "Ricart-Argawala/grpc"
	"context"
	"flag"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type state int

const (
	released state = iota
	wanted
	held
)

func (s state) String() string {
	switch s {
	case wanted:
		return "WANTED"
	case held:
		return "HELD"
	default:
		return "RELEASED"
	}
}

var nodes = []string{
	"5001",
	"5002",
	"5003",
}

type Node struct {
	pb.UnimplementedNodeServer
	clients []pb.NodeClient

	state         state
	port          string
	id            string
	address       string
	lamport       int64
	timeOfRequest int64
	mu            sync.Mutex
	queue         []chan struct{}
}

func (n *Node) Serve() error {
	lis, err := net.Listen("tcp", ":"+n.port)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterNodeServer(s, n)
	return s.Serve(lis)
}

func (n *Node) RecieveMessage(ctx context.Context, req *pb.MessageRequest) (*emptypb.Empty, error) {
	n.mu.Lock()
	if req.Lamport > n.lamport {
		n.lamport = req.Lamport
	}
	n.lamport++
	makeThemWait := n.state == held || (n.state == wanted && n.isBefore(req.Lamport, req.SenderId)) //boolean

	if !makeThemWait {
		n.mu.Unlock()
		return &emptypb.Empty{}, nil
	}
	//we are the first to access the CS
	wait := make(chan struct{})
	n.queue = append(n.queue, wait)
	n.mu.Unlock()

	select {
	case <-wait:
		n.mu.Lock()
		n.lamport++
		n.mu.Unlock()
		return &emptypb.Empty{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}

}

func (n *Node) isBefore(otherT int64, otherId string) bool {

	if n.timeOfRequest != otherT {
		return n.timeOfRequest < otherT
	}
	return n.id < otherId
}

// Enter

func (n *Node) Enter() {
	log.Printf("Node %d is now wanting to enter CS", n.id)
	n.mu.Lock()
	n.state = wanted
	n.lamport++
	n.timeOfRequest = n.lamport
	req := &pb.MessageRequest{SenderId: n.id, Lamport: n.timeOfRequest}
	others := n.clients
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, p := range others {
		wg.Add(1)
		go func(c pb.NodeClient) {
			defer wg.Done()
			for {
				_, err := c.RecieveMessage(context.Background(), req, grpc.WaitForReady(true))
				if err == nil {
					return
				}
				log.Printf("Node %s: request failed, retrying: %v", n.id, err)
				time.Sleep(200 * time.Millisecond)
			}
		}(p)
	}
	wg.Wait()

	n.mu.Lock()
	n.state = held
	n.lamport++
	n.mu.Unlock()
	log.Printf("Node %s entered critical section", n.id)
}

//CreateNode

// Exit
func (n *Node) Exit() {
	n.mu.Lock()
	n.state = released
	for _, nodeChan := range n.queue {
		close(nodeChan)
	}
	n.queue = nil
	n.mu.Unlock()
	log.Printf("Node %s exited critical section", n.id)
}

func (n *Node) Connect() error {
	for _, addr := range nodes {
		if addr == n.address {
			continue
		}
		conn, err := grpc.NewClient("localhost:"+addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		n.clients = append(n.clients, pb.NewNodeClient(conn))
	}
	log.Printf("Node %s connected to %d peers", n.id, len(n.clients))
	return nil
}

func main() {
	id := flag.String("id", "invalidId", "The id, please select either 0,1,2")
	flag.Parse()
	if *id == "invalidId" {
		log.Fatal("please give id using -id flag")
		return
	}
	idInt, err := strconv.Atoi(*id)

	if err != nil {
		log.Fatalf("please provide an id of either 0,1,2")
	}

	addr := nodes[idInt]

	n := Node{id: *id, port: nodes[idInt], state: released, address: addr}

	go func() {
		if err := n.Serve(); err != nil {
			log.Fatalf("Serve:", err)
		}
	}()
	time.Sleep(20 * time.Second) // wait for all to be created

	if err := n.Connect(); err != nil {
		log.Fatalf("connection:", err)
	}

	//Core logic
	for {
		time.Sleep(time.Duration(1000+rand.Intn(4000)) * time.Millisecond)
		n.Enter()
		log.Printf("Node %s entered critical section", n.id)
		n.EnterCriticalSection()
		log.Printf("Node %s exited critical section", n.id)
		n.Exit()
	}

}

func (node *Node) EnterCriticalSection() {
	lis, err := net.Dial("tcp", ":9000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()
}
