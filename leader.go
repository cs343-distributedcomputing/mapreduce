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

type MapReply struct {
	Key   string
	Value string
}

type ReduceArgs struct {
	Key   string
	Value []string
}

var ( // global vars
	serverAddress string  = "10.154.0.117"
	files []string = []string{
		"input/test.txt",
		//"input/",
		// "input/",
	}
)

func assignTaskToWorker(taskType string, taskArgs interface{}, address string) (error, []MapReply) {
	fmt.Printf("\nassigning tasks")
	client, err := rpc.DialHTTP("tcp", serverAddress+":"+address)
	if err != nil {
		log.Fatal("dialing:", err)
		return err, nil
	}
	fmt.Printf("\nhttp dialed")

	// client1, err1 := rpc.DialHTTP("tcp", serverAddress + ":3001")
	// if err1 != nil {
	// 	log.Fatal("dialing:", err1)
	// 	return err1, nil
	// }

	// client2, err2 := rpc.DialHTTP("tcp", serverAddress + ":3002")
	// if err2 != nil {
	// 	log.Fatal("dialing:", err2)
	// 	return err2, nil
	// }

	// rpc calls based on task type (map or reduce)
	var reply []MapReply
	if taskType == "Map" {
		fmt.Printf("\nleader calls worker to do map")
		err = client.Call("Worker.Map", taskArgs, &reply)
	} else if taskType == "Reduce" {
		err = client.Call("Worker.Reduce", taskArgs, &reply)
	} else {
		err = errors.New("invalid task type :<")
	}
	fmt.Printf("\nreply: ", reply)
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
		fmt.Print("\n chunkArray: ", chunkArray)

		numChunksForOneWorker := len(chunkArray) / len(addressList)

		// TODO: keep track of all the words and their counts
		wordCounts := make(map[string]int)

		// for each worker get {chunkArray/numWrokers} number of chunks
		// dial server to make rpc's
		for _, address := range addressList { // for each worker
			// loop thru number of chunks that one worker needs to work on
			// first chunk is the index of the first chunk that the address will grab
			firstChunk := 0
			// assign map tasks
			mapArgs := &MapArgs{Chunk: chunkArray[firstChunk:numChunksForOneWorker]}
			firstChunk += numChunksForOneWorker
			fmt.Print("\nmapArgs: ", mapArgs)
			err, mapReply := assignTaskToWorker("Map", mapArgs, address)
			if err != nil {
				fmt.Printf("error assigning map task: %v", err)
			}
			fmt.Printf("\n\nLeader calls map rpc: key - %s, value - %s, reply - %s",
				mapArgs.Chunk, mapReply)

			// assign reduce tasks - mocked; did not need to implement
			// reduceArgs := &ReduceArgs{Key: "test", Value: []string{"1", "1"}}
			// err, reduceReply := assignTaskToWorker("Reduce", reduceArgs, address)
			// if err != nil {
			// 	fmt.Printf("error assigning reduce task: %v", err)
			// }
			// fmt.Printf("\n\nLeader calls reduce rpc: key - %s, value - %s, reply - %d",
			// 	reduceArgs.Key, reduceArgs.Value, reduceReply)

			// aggregate word counts from map reply
			for _, reply := range mapReply {
				_, ok := wordCounts[reply.Key]
				if ok  {
					wordCounts[reply.Key] += 1
				} else {
					wordCounts[reply.Key] = 1
				}
			}
		}
		filesProcessed++

		// write final word counts to output file
		outputFile, err := os.Create("output.txt")
		if err != nil {
			log.Fatal("failed to create output file:", err)
		}
		defer outputFile.Close()

		for word, count := range wordCounts {
			_, err := fmt.Fprintf(outputFile, "%s: %d\n", word, count)
			if err != nil {
				log.Fatal("Error writing to output file:", err)
			}
		}
		fmt.Println("\nword counts written to output file successfully.")
	}
}
