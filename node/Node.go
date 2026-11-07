package node

import (
	pb "Ricart-Argawala/grpc"
	"context"
	"flag"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
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
	"localhost:5001",
	"localhost:5002",
	"localhost:5003",
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
			if _, err := c.RecieveMessage(context.Background(), req); err != nil {
				log.Print("Peer call failed to", n.id)
			}
		}(p)
	}
	wg.Wait()

	n.mu.Lock()
	n.state = held
	n.lamport++ //? Not sure if this is needed// yours truely <3
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

func main() {
	id := flag.String("id", "invalidId", "The id")
	flag.Parse()
	if id == "invalidId" {
		log.Fatal("please give id using -id flag")
		return
	}

}
