package Criticalsection

import (
	"fmt"
)

var EnterCS = make(chan string)
var ExitCS = make(chan struct{})

func StartCriticalSection() {
	go func() {
		for node := range EnterCS {
			fmt.Printf("Node %s entered critical section\n", node)

			<-ExitCS

			fmt.Printf("Node %s left critical section\n", node)
		}
	}()
}
