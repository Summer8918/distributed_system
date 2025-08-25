package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type mapTask struct {
	taskID   int
	fileName string
	status   TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	nReduce      int
	mapTasks     []mapTask
	inputfiles   []string
	mapTasksDone bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *WorkerArgs, reply *CoordinatorReply) error {
	if !c.mapTasksDone {
		for i := range len(c.mapTasks) {
			if c.mapTasks[i].status != Success {
				reply.InputFile = c.mapTasks[i].fileName
				reply.WorkId = i
				break
			}
		}
	}
	reply.TaskType = MapTask
	reply.NReduce = c.nReduce
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fileN := len(files)
	c := Coordinator{
		inputfiles: files,
		mapTasks:   make([]mapTask, fileN),
		nReduce:    nReduce,
	}

	// Your code here.

	for i := range fileN {
		c.mapTasks[i] = mapTask{
			taskID:   i,
			fileName: files[i],
			status:   Todo,
		}
	}
	c.server()

	return &c
}
