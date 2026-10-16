package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	// Your definitions here.
	files []string

	nMap  int
	nFinishedMap int
	mapTaskLog []string

	nReduce int
	nFinishedReduce int
	reduceTaskLog []string
	
	mutex sync.Mutex
}


func (c *Coordinator) mapTimeout(i int) {
	time.Sleep(10 * time.Second)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.mapTaskLog[i] == "allocate" {
		c.mapTaskLog[i] = ""   // 超时，重新变空闲
	}
}

func (c *Coordinator) reduceTimeout(i int) {
	time.Sleep(10 * time.Second)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.reduceTaskLog[i] == "allocate" {
		c.reduceTaskLog[i] = ""
	}
}
// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func (c *Coordinator)AllocateTask(args *WorkerArgs, reply *WorkerReply) error{
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.nFinishedMap < c.nMap {
		for i := 0; i < c.nMap; i++ {
			if c.mapTaskLog[i] == "" {
				c.mapTaskLog[i] = "allocate"
				
				reply.TaskType = 0
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				reply.MapTaskID = i
				reply.MapTaskFileName = c.files[i]

				go c.mapTimeout(i)

				return nil
			}
		}
		reply.TaskType = 2
		return nil
	} else if c.nFinishedMap == c.nMap && c.nFinishedReduce < c.nReduce {
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskLog[i] == "" {
				c.reduceTaskLog[i] = "allocate"
				
				reply.TaskType = 1
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				reply.ReduceTaskID = i

				go c.reduceTimeout(i)

				return nil
			}
		}
		reply.TaskType = 2
		return nil
	}

	reply.TaskType = 3
	return nil
}



func (c *Coordinator)ReceiveFinishedMap(args *WorkerArgs, reply *WorkerReply) error{
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.mapTaskLog[args.MapTaskNum] == "finish"{
		return nil
	}
	c.nFinishedMap++
	c.mapTaskLog[args.MapTaskNum] = "finish"
	return nil
}



func (c *Coordinator)ReceiveFinishedReduce(args *WorkerArgs, reply *WorkerReply) error{
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.reduceTaskLog[args.ReduceTaskNum] == "finish"{
		return nil
	}
	c.nFinishedReduce++
	c.reduceTaskLog[args.ReduceTaskNum] = "finish"
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret = (c.nFinishedReduce == c.nReduce)

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	
	c.nMap = len(files)
	c.nFinishedMap = 0
	c.mapTaskLog = make([]string, c.nMap)

	c.nReduce = nReduce
	c.nFinishedReduce = 0
	c.reduceTaskLog = make([]string, c.nReduce)

	c.server()
	return &c
}
