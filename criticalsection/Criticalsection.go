package criticalsection

import (
	"bufio"
	"log"
	"net"
	"strings"
)

func main() {
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer lis.Close()

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Fatalf("Failed to accept connection: %v", err)
		}
		go handeConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading message: %v", err)
			return
		}
		message = strings.TrimSpace(message)
		switch message {
		case "exit":
			return
		default:
			log.Printf("Message: %s", message)
		}
	}
}
