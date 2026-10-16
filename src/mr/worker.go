package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"sort"
	"time"
	"os"
	"io"
	"strconv"
	"encoding/json"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMapTask(reply WorkerReply, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}

	fileName := reply.MapTaskFileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName) //print error and execute os.Exit(1)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	kva := mapf(reply.MapTaskFileName, string(content))
	
	intermediate = append(intermediate, kva...)

	// 将 键值对 按规则 平均分到NRdecu个桶里，方便后续分发
	buckets := make([][]KeyValue, reply.NReduce)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}
	for _, kv := range intermediate {
		buckets[ihash(kv.Key)%reply.NReduce] = append(buckets[ihash(kv.Key)%reply.NReduce], kv)
	}

	for idx := range buckets {
		outname := "mr-" + strconv.Itoa(reply.MapTaskID) + "-" + strconv.Itoa(idx)
		ofile, err := os.CreateTemp(".", outname+"*")
		if err != nil{
			log.Fatal(err)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[idx] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write into %v", outname)
			}
		}
		ofile.Close()

		os.Rename(ofile.Name(), outname)
	}

	finishedArgs := WorkerArgs{reply.MapTaskID, -1}
	finishedReply := WorkerReply{}
	call("Coordinator.ReceiveFinishedMap", &finishedArgs, &finishedReply)

}

func doReduceTask(reply WorkerReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for idx := 0; idx < reply.NMap; idx++ {
		inname := "mr-" + strconv.Itoa(idx) + "-" + strconv.Itoa(reply.ReduceTaskID)
		file, err := os.Open(inname)
		if err != nil{
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break   // 读到 EOF 或出错
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.ReduceTaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create output file %v", oname)
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()


	//删除中间文件
	for i := 0; i < reply.NMap; i++ {
		inname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskID)
		err := os.Remove(inname)
		if err != nil {
			log.Fatalf("cannot open delete" + inname)
		}
	}

	finishedArgs := WorkerArgs{-1, reply.ReduceTaskID}
	finishedReply := WorkerReply{}
	call("Coordinator.ReceiveFinishedReduce", &finishedArgs, &finishedReply)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.AllocateTask", &args, &reply);
		if !ok {
			fmt.Println("error call(Coordinator.AllocateTask)")
			return
		}
		switch reply.TaskType {
		case 0:
			doMapTask(reply, mapf)
		case 1:
			doReduceTask(reply, reducef)
		case 2:
			time.Sleep(time.Second)
		case 3:
			// all tasks finished
			return
		default:
			fmt.Println("unvalid Tasktype")

		}

	}
}



//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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






//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }
