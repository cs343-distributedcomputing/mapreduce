// assigns one worker to do map or reduce
// check if a worker is alive
// tally of words
// status of worker: idle, in-progress, completed

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
		log.Fatal("\ndialing error:", err)
	}

	// map and reduce rpc calls for multiple worker machines
	numWorkers := 3
	for i:=0; i < numWorkers; i++ {
		// sync call: MAP
		mapArgs := &MapArgs{"test","1"} // args for map or reduce funcs
		var mapReply int
		err = leader.Call("Worker.Map", mapArgs, &mapReply)
		if err != nil {
			log.Fatal("\nworker map error:", err)
		}
		fmt.Printf("\n\nLeader calls map rpc: key - %s, value - %s, err reply - %d", 
			mapArgs.Key, mapArgs.Value, mapReply)

		// sync call: REDUCE
		var reduceArgList = []string{"1", "1"}
		reduceArgs := &ReduceArgs{"test", reduceArgList} // args for map or reduce funcs
		var reduceReply int
		err = leader.Call("Worker.Reduce", reduceArgs, &reduceReply)
		if err != nil {
			log.Fatal("\n\nworker reduce error:", err)
		}
		fmt.Printf("\nLeader calls reduce rpc: key - %s, value - %s, err reply - %d", 
			reduceArgs.Key, reduceArgs.Value, reduceReply)
	}
}
