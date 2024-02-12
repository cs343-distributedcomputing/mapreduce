// assigns one worker to do map or reduce
// check if a worker is alive
// tally of words
// status of worker: idle, in-progress, completed

// calls rpc
package main

import (
	"log"
	"net/rpc"
	"errors"
	"fmt"
)


type MapArgs struct {
	Key string
	Value string
}

type ReduceArgs struct {
	Key string
	Value []string
}

type Worker struct { // redundant, also in worker.go?
	Status string // idle, in-progress
}

var ( // global vars
	serverAddress string = "10.154.0.117"
	workers []*Worker = []*Worker{ // 4 workers for example
		{Status: "idle"},
		// {Status: "idle"},
		// {Status: "idle"},
		// {Status: "idle"},
	}
)

func (t *Worker) SetWorkerStatus(status string) {
	t.Status = status
}

func assignTaskToWorker(taskType string, taskArgs interface{}, address string) (error, int) {
	if len(workers) == 0 {
		return errors.New("\nno workers defined"), -1
	}

	// find idle worker to assign task to
	var worker *Worker
	for _, w := range workers {
		if w.Status == "idle" {
			worker = w
			break
		}
	}

	if worker == nil {
		return errors.New("no idle worker available"), -1
	}

	// change status of worker to 'in-progress' since it's getting a task
	worker.SetWorkerStatus("in-progress")

	
	client, err := rpc.DialHTTP("tcp", serverAddress + ":" + address)
	if err != nil {
		log.Fatal("dialing:", err)
		return err, -1
	}

	// client1, err1 := rpc.DialHTTP("tcp", serverAddress + ":3001")
	// if err1 != nil {
	// 	log.Fatal("dialing:", err1)
	// 	return err1, -1
	// }

	// client2, err2 := rpc.DialHTTP("tcp", serverAddress + ":3002")
	// if err2 != nil {
	// 	log.Fatal("dialing:", err2)
	// 	return err2, -1
	// }

	// rpc calls based on task type (map or reduce)
	var reply int
	if taskType == "Map" {
		err = client.Call("Worker.Map", taskArgs, &reply)
	} else if taskType == "Reduce" {
		err = client.Call("Worker.Reduce", taskArgs, &reply)
	} else {
		err = errors.New("invalid task type :<")
	}

	// after worker is done reset its status to available/idle
	worker.SetWorkerStatus("idle")

	return err, reply
}

func main() {

	numInputFiles := 1 // change as needed
	filesProcessed := 0 // increment as files are processed
	done := false // status of mapreduce entire operation 
	// TODO: flag done as true once entire operation finishes

	// periodically check worker status to reassign tasks
	for !done {
        if filesProcessed >= numInputFiles {
			break // no more tasks to assign
		}
       
		var addressList [3]string
		addressList[0] = "3000"
		addressList[1] = "3001"
		addressList[2] = "3002"
		// dial server to make rpc's
		for _, address := range addressList {
			chunk := 
			// assign map tasks
			mapArgs := &MapArgs{chunk}
			err, mapReply := assignTaskToWorker("Map", mapArgs, address)
			if err != nil {
				log.Printf("error assigning map task: %v", err)
			}
			fmt.Printf("\n\nLeader calls map rpc: key - %s, value - %s, reply - %d", 
						mapArgs.Key, mapArgs.Value, mapReply)

			// assign reduce tasks
			reduceArgs := &ReduceArgs{Key: "test", Value: []string{"1", "1"}}
			err, reduceReply := assignTaskToWorker("Reduce", reduceArgs, address)
			if err != nil {
				log.Printf("error assigning reduce task: %v", err)
			}
			fmt.Printf("\n\nLeader calls reduce rpc: key - %s, value - %s, reply - %d", 
			reduceArgs.Key, reduceArgs.Value, reduceReply)
		}
		filesProcessed++
    }
}
