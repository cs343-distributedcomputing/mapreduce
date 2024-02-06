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
)

type Worker struct {
	status string // idle, in-progress, completed
}

type MapKeyValue struct {
	Key string
	Value string
}

type ReducKeyValue struct {
	Key string
	Values []string
}

func (t *Worker) Map(args *MapKeyValue, reply *int) error {
	// TODO: map words; change return to error later
	fmt.Printf("map func")
	return nil
}

func (t *Worker) Reduce(args *ReducKeyValue, reply *int) error {
	// TODO: reduce words; change return to error later
	fmt.Printf("reduce func")
	return nil
}

func main() {
	worker := new(Worker)
	rpc.Register(worker)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":3000")
	fmt.Printf("yas listen worker")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	http.Serve(l, nil)
}
