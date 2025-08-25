package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskTypeT int

const (
	MapTask    TaskTypeT = iota // 0
	ReduceTask                  // 1
	Exit                        // 2
	Wait                        // 3
)

type TaskStatus int

const (
	Success TaskStatus = iota // 0
	Fail
	AskANewTask
	Todo
	Assigned
)

type WorkerArgs struct {
	TaskStatus TaskStatus
	WorkID     int
	TaskType   TaskTypeT
}

type CoordinatorReply struct {
	TaskType   TaskTypeT
	InputFiles []string
	NReduce    int
	WorkId     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
