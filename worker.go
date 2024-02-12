// receive a list of inputs (word files)
// map -> key value pair ("word", "1")
// reduce -> combines output from map

// rpc design
package main

import (
	// "errors"
	"net"
	"net/rpc"
	"net/http"
	"log"
	"fmt"
	"strings"
)

type Worker struct {
	Status string // idle, in-progress
}

type MapKeyValue struct {
	Key string
	Value string
}

type MapChunkInput struct {
	Chunk string
}

type ReducKeyValue struct {
	Key string
	Values []string
}

func (t *Worker) Map(args *MapChunkInput, reply *int) []MapKeyValue {
	// TODO: map words; change return to error later
	fmt.Printf("\n\nmap func called in worker machine")
	words := strings.Fields(args) // split by white space
	var kvPairs []MapKeyValue
	for _, word := range words {
        kvPairs = append(kvPairs, MapKeyValue{word, "1"})
    }
    return kvPairs
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

	fmt.Printf("\nWorker is listening...")
	l, err := net.Listen("tcp", ":3000")

	if err != nil {
		log.Fatal("\nlisten error:", err)
	}
	http.Serve(l, nil)
}
