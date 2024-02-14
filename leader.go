// assigns one worker to do map or reduce
// check if a worker is alive
// tally of words
// status of worker: idle, in-progress, completed

// calls rpc
package main

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"

	//"regexp"
	"io"
	"strings"
)

type MapArgs struct {
	Chunk []string
}

type ReduceArgs struct {
	Key   string
	Value []string
}

type Worker struct { // redundant, also in worker.go?
	Status string // idle, in-progress
}

var ( // global vars
	serverAddress string    = "10.154.0.117"
	workers       []*Worker = []*Worker{ // 4 workers for example
		{Status: "idle"},
		// {Status: "idle"},
		// {Status: "idle"},
		// {Status: "idle"},
	}
	files []string = []string{
		"input/test.txt",
		//"input/",
		// "input/",
	}
)

func (t *Worker) SetWorkerStatus(status string) {
	t.Status = status
}

func assignTaskToWorker(taskType string, taskArgs interface{}, address string) (error, int) {
	fmt.Printf("assigning tasks")
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

	client, err := rpc.DialHTTP("tcp", serverAddress+":"+address)
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
func read_file_chunk(chunkSize int64, startByte int64, filePath string) string {
	//read from full file the designated chunk of bytes into the buffer
	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	stringFileContent := string(fileContent)
	reader := strings.NewReader(stringFileContent)
	r := io.NewSectionReader(reader, startByte, chunkSize)

	buf := make([]byte, chunkSize)
	n, err := r.Read(buf)
	if err != nil {
		panic(err)
	}
	fmt.Printf("n: %v\n", n)

	//split content into a wordlist
	fileChunkWords := string(buf)
	return fileChunkWords
	// re1 := regexp.MustCompile(`\p{P}|[^\S+]`)
	// wordList := re1.Split(fileChunkWords, -1)

	//add to dicitonary
	// for j := 0; j < len(wordList); j++{
	// 	if wordList[j] != ""{
	// 		lowercaseWord := strings.ToLower(wordList[j])
	// 		numWordDouble[lowercaseWord] += 1
	// 	}
	// }
}
func split_chunk(files []string) []string {
	var chunkArray []string
	for i := 0; i < len(files); i++ {
		//divide file based on length
		file, err := os.Open(files[i])
		if err != nil {
			log.Fatal(err)
		}
		fi, err := file.Stat()
		if err != nil {
			log.Fatal(err)
		}
		sizeOfFile := fi.Size()
		sizeOfFileChunk := int64(100)

		//Loop through a file and call thread for each chunk
		for startByte := int64(0); startByte < sizeOfFile; startByte = startByte + sizeOfFileChunk {
			//if remaining bytes of the file is smaller than file chunk edge case
			if sizeOfFile <= int64(startByte)+sizeOfFileChunk {
				//if the remaining byte is smaller than the expected chunk size
				//fmt.Print("remaining bytes less than size")
				sizeOfFileChunk = sizeOfFile - startByte
			}
			// checks if start byte at the end of the file
			if startByte >= sizeOfFile {
				//fmt.Print("end of search")
			} else {
				newChunk := read_file_chunk(sizeOfFileChunk, startByte, files[i])
				chunkArray = append(chunkArray, newChunk)
			}
		}
	}
	return chunkArray
}

func main() {

	numInputFiles := 1  // change as needed
	filesProcessed := 0 // increment as files are processed
	done := false       // status of mapreduce entire operation
	// TODO: flag done as true once entire operation finishes

	// periodically check worker status to reassign tasks
	for !done {
		if filesProcessed >= numInputFiles {
			break // no more tasks to assign
		}

		var addressList [1]string
		addressList[0] = "3000"
		// addressList[1] = "3001"
		// addressList[2] = "3002"

		chunkArray := split_chunk(files)
		fmt.Print(chunkArray)

		numChunksForOneWorker := len(chunkArray) / len(addressList)

		// for each worker get {chunkArray/numWrokers} number of chunks
		// dial server to make rpc's
		for _, address := range addressList { // for each worker
			//chunk := "test"
			// loop thru number of chunks that one worker needs to work on
			// first chunk is the index of the first chunk that the address will grab
			firstChunk := 0
			// assign map tasks
			mapArgs := &MapArgs{chunkArray[firstChunk:numChunksForOneWorker]}
			firstChunk = firstChunk + numChunksForOneWorker
			err, mapReply := assignTaskToWorker("Map", mapArgs, address)
			if err != nil {
				log.Printf("error assigning map task: %v", err)
			}
			fmt.Printf("\n\nLeader calls map rpc: key - %s, value - %s, reply - %d",
				mapArgs.Chunk, mapReply)

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
