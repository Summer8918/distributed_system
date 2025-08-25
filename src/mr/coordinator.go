package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type mapTask struct {
	taskID    int
	fileName  string
	status    TaskStatus
	timeStamp time.Time
}

type reduceTask struct {
	taskID    int
	files     []string
	status    TaskStatus
	timeStamp time.Time
}

type Coordinator struct {
	// Your definitions here.
	nReduce           int
	mapTasks          []mapTask
	reduceTasks       []reduceTask
	inputfiles        []string
	mapTasksDone      bool
	mutex             sync.Mutex
	mapTaskDoneNum    int
	reduceTaskDoneNum int
	reduceTasksDone   bool
	allDone           bool
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.NReduce = c.nReduce
	if !c.mapTasksDone {
		for i := range len(c.mapTasks) {
			if c.mapTasks[i].status == Todo || (c.mapTasks[i].status == Assigned && time.Since(c.mapTasks[i].timeStamp) > 10*time.Second) {
				reply.WorkId = i
				reply.InputFiles = append(reply.InputFiles, c.mapTasks[i].fileName)
				reply.TaskType = MapTask
				c.mapTasks[i].status = Assigned
				c.mapTasks[i].timeStamp = time.Now()
				return nil
			}
		}
	} else if !c.reduceTasksDone {
		for i := range len(c.reduceTasks) {
			if c.reduceTasks[i].status == Todo || (c.reduceTasks[i].status == Assigned && time.Since(c.reduceTasks[i].timeStamp) > 10*time.Second) {
				reply.InputFiles = c.reduceTasks[i].files
				reply.WorkId = i
				reply.TaskType = ReduceTask
				c.reduceTasks[i].status = Assigned
				c.reduceTasks[i].timeStamp = time.Now()
				return nil
			}
		}
	}
	if !c.reduceTasksDone || !c.mapTasksDone {
		reply.TaskType = Wait
		return nil
	} else {
		reply.TaskType = Exit
	}
	return nil
}

func (c *Coordinator) NotifyComplete(args *WorkerArgs, reply *CoordinatorReply) error {
	switch args.TaskType {
	case MapTask:
		c.mutex.Lock()
		defer c.mutex.Unlock()
		workID := args.WorkID
		if workID >= len(c.mapTasks) {
			log.Fatalf("workID >= len(c.mapTasks)")
		}
		c.mapTasks[workID].status = Success
		c.mapTaskDoneNum += 1
		if c.mapTaskDoneNum == len(c.mapTasks) {
			c.mapTasksDone = true
		}
	case ReduceTask:
		c.mutex.Lock()
		defer c.mutex.Unlock()
		c.reduceTaskDoneNum += 1
		if c.reduceTaskDoneNum == c.nReduce {
			c.allDone = true
		}
	}
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
		inputfiles:  files,
		mapTasks:    make([]mapTask, fileN),
		nReduce:     nReduce,
		reduceTasks: make([]reduceTask, nReduce),
	}

	// Your code here.

	for i := range fileN {
		c.mapTasks[i] = mapTask{
			taskID:   i,
			fileName: files[i],
			status:   Todo,
		}
	}

	for i := range nReduce {
		c.reduceTasks[i] = reduceTask{
			taskID: i,
			files:  make([]string, fileN),
			status: Todo,
		}
		for j := range fileN {
			name := fmt.Sprintf("mr-%d-%d", j, i)
			c.reduceTasks[i].files[j] = name
		}
	}

	c.server()

	return &c
}
