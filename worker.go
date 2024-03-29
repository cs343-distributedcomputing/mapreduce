// receive a list of inputs (word files)
// map -> key value pair ("word", "1")
// reduce -> combines output from map

// rpc design
package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
)

type Worker struct {
	Status string // idle, in-progress
}

type MapKeyValue struct {
	Key   string
	Value string
}

type MapArgs struct {
	Chunk []string
}

type ReducKeyValue struct {
	Key    string
	Values []string
}

// returns mapReply of list of key val pairs
// ex: [MapKeyValue{"hi": "1"}, MapKeyValue{"hi", "1"}, MapKeyValue{"hey", "1"}]
func (t *Worker) Map(args MapArgs, reply *[]MapKeyValue) error { 
	// TODO: map words; change return to error later
	fmt.Printf("\n\nmap func called in worker machine")
	// fmt.Printf("chunk in worker.map: ", string(args.Chunk))
	chunks := args.Chunk
	//words := strings.Fields(args) // split by white space
	var kvPairs []MapKeyValue
	for _, chunk := range chunks {
		words := strings.Fields(chunk)
		for _, word := range words {
			kvPairs = append(kvPairs, MapKeyValue{word, "1"})
			fmt.Printf("\n" + word)
		}
	}
	*reply = kvPairs
	return nil
}

func (t *Worker) Reduce(args *ReducKeyValue, reply *int) error {
	// TODO: reduce words; change return to error later
	fmt.Printf("\n\nreduce func called in worker machine")
	return nil
}

func main() {
	worker := new(Worker)
	rpc.Register(worker)
	rpc.HandleHTTP()

	port := ":3002" // change as needed
	fmt.Printf("\nWorker is listening at port: " + port)
	l, err := net.Listen("tcp", port)

	if err != nil {
		log.Fatal("\nlisten error:", err)
	}
	http.Serve(l, nil)
}
