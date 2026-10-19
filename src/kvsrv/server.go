package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	kvstore map[string]string
	cookieStore map[int64]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvstore[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.cookieStore[args.ID]; ok {
		reply.Value = kv.cookieStore[args.ID]
		return
	}
	kv.kvstore[args.Key] = args.Value
	kv.cookieStore[args.ID] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.cookieStore[args.ID]; ok {
		reply.Value = kv.cookieStore[args.ID]
		return
	}
	oldValue := kv.kvstore[args.Key]
	kv.kvstore[args.Key] = oldValue + args.Value
	kv.cookieStore[args.ID] = oldValue
	reply.Value = oldValue
}

func (kv * KVServer) Finish(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.cookieStore[args.ID]; ok {
		delete(kv.cookieStore, args.ID)
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvstore = make(map[string]string)
	kv.cookieStore = make(map[int64]string)

	return kv
}
