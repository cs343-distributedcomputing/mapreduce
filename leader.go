// assigns one worker to do map or reduce
// check if a worker is alive
// tally of words
// status of worker: idle, alive, dead

// calls rpc
package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type MapArgs struct {
	Key string
	Value string
}

type ReduceArgs struct {
	Key string
	Value []string
}

func main() {
	serverAddress := "10.155.1.178"
	leader, err := rpc.DialHTTP("tcp", serverAddress + ":3000")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// sync call
	mapArgs := &MapArgs{"test","1"} // args for map or reduce funcs
	var mapReply int
	err = leader.Call("Worker.Map", mapArgs, &mapReply)
	if err != nil {
		log.Fatal("worker map error:", err)
	}
	fmt.Printf("map result: %d*%d=%d", mapArgs.Key, mapArgs.Value, mapReply)

	var reduceArgList = []string{"1", "1"}
	reduceArgs := &ReduceArgs{"test", reduceArgList} // args for map or reduce funcs
	var reduceReply int
	err = leader.Call("Worker.Reduce", reduceArgs, &reduceReply)
	if err != nil {
		log.Fatal("worker reduce error:", err)
	}
	fmt.Printf("reduce result: %d*%d=%d", reduceArgs.Key, reduceArgs.Value, reduceReply)
}
