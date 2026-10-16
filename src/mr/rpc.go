package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type WorkerArgs struct{
	MapTaskNum int
	ReduceTaskNum int
}

type WorkerReply  struct{
	TaskType int //0：mapTask， 1: reduceTask， 2: please waiting， 3:all tasks finished
	
	NMap  int
	MapTaskID int
	MapTaskFileName string
	
	NReduce int
	ReduceTaskID int
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
