package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		reply := CoordinatorReply{}
		GetATask(&reply)
		// fmt.Println("reply.TaskType: %v reply.WorkId %v", reply.TaskType, reply.WorkId)
		switch reply.TaskType {
		case MapTask:
			handleMapTask(mapf, &reply)
		case ReduceTask:
			// fmt.Println("Get reduce work id %v", reply.WorkId)
			handleReduceTask(reducef, &reply)
		case Exit:
			os.Exit(0)
		case Wait:
			time.Sleep(1 * time.Second)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func handleReduceTask(reducef func(string, []string) string, reply *CoordinatorReply) {
	// Load intermediate files
	intermediate := []KeyValue{}
	// fmt.Println(reply.InputFiles)
	for m := 0; m < len(reply.InputFiles); m++ {
		file, err := os.Open(reply.InputFiles[m])

		if err != nil {
			log.Fatalf("fail to open file %v", reply.InputFiles[m])
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}

			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	// Sort intermediate kvs by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// Create output file
	outFileName := fmt.Sprintf("mr-out-%d", reply.WorkId)
	outFile, _ := os.CreateTemp("", outFileName)

	// Apply reduce function
	i := 0
	for i < len(intermediate) {
		j := i + 1
		vals := []string{}
		vals = append(vals, intermediate[i].Value)
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			vals = append(vals, intermediate[j].Value)
			j++
		}
		output := reducef(intermediate[i].Key, vals)
		fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	outFile.Close()
	os.Rename(outFile.Name(), outFileName)

	// Update reduce task status
	NotifyComplete(reply)
}

func handleMapTask(mapf func(string, string) []KeyValue, reply *CoordinatorReply) {
	if len(reply.InputFiles) != 1 {
		log.Fatalf("len(reply.InputFiles) != 1")
	}
	fileName := reply.InputFiles[0]
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	writeIntermediate(reply.WorkId, reply.NReduce, kva)
	NotifyComplete(reply)
}

func writeIntermediate(mapID int, nReduce int, kvs []KeyValue) error {
	// Open one file and encoder per reduce bucket
	files := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)

	// create tp files then atomatically rename at the end
	for i := 0; i < nReduce; i++ {
		name := fmt.Sprintf("mr-%d-%d", mapID, i)
		f, err := os.Create(name)
		if err != nil {
			return fmt.Errorf("create %s: %w", name, err)
		}
		files[i] = f
		encs[i] = json.NewEncoder(f)
	}

	// ensure files get closed
	defer func() {
		for _, f := range files {
			if f != nil {
				_ = f.Close()
			}
		}
	}()

	// Dispatch each kv to its bucket and encode as a JSON line
	for _, kv := range kvs {
		r := ihash(kv.Key) % nReduce
		if err := encs[r].Encode(&kv); err != nil {
			return fmt.Errorf("encode kv to bucket %d: %w", r, err)
		}
	}
	return nil
}

func GetATask(reply *CoordinatorReply) {
	args := WorkerArgs{}
	args.TaskStatus = AskANewTask

	status := call("Coordinator.AssignTask", &args, reply)

	if !status {
		fmt.Println("call Coordinator.AssignTask failed")
	}
}

func NotifyComplete(reply *CoordinatorReply) {
	args := WorkerArgs{
		WorkID:     reply.WorkId,
		TaskStatus: Success,
		TaskType:   reply.TaskType,
	}

	status := call("Coordinator.NotifyComplete", &args, reply)
	if !status {
		fmt.Println("call Coordinator.NotifyComplete failed")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
