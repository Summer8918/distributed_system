package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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
	reply := CoordinatorReply{}
	GetATask(&reply)
	switch reply.TaskType {
	case MapTask:
		handleMapTask(mapf, &reply)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func handleMapTask(mapf func(string, string) []KeyValue, reply *CoordinatorReply) {
	fileName := reply.InputFile
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("cannot open file %v", fileName)
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	writeIntermediate(reply.WorkId, reply.NReduce, kva)
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
			return fmt.Errorf("Create %s: %w", name, err)
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
			return fmt.Errorf("Encode kv to bucket %d: %w", r, err)
		}
	}
	return nil
}

func GetATask(reply *CoordinatorReply) {
	args := WorkerArgs{}
	args.TaskStatus = AskANewTask

	status := call("Coordinator.AssignTask", &args, reply)

	if status {
		fmt.Println("Get reply:", reply.NReduce)
	} else {
		fmt.Println("call Coordinator.AssignTask failed")
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
