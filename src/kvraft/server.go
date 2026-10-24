package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

const ExecuteTimeout = 500 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64
	RequestID int64
	OpType    string
	Key       string
	Value     string
}

type SnapshotData struct {
	KvStore       map[string]string
	RequestCookie map[int64]int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore       map[string]string
	requestCookie map[int64]int64  // 每个client最近一次请求的requestID
	waitChMap     map[int]chan *Op //通知chan，key为日志的下标，值为通道

	persister *raft.Persister
	lastIncludedIndex int
}

func (kv *KVServer) encodeSnapshot() []byte {
	w := bytes.NewBuffer(nil)
	e := labgob.NewEncoder(w)
	e.Encode(SnapshotData{
		KvStore:       kv.kvStore,
		RequestCookie: kv.requestCookie,
	})
	return w.Bytes()
}

func (kv *KVServer) decodeSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewReader(snapshot)
	d := labgob.NewDecoder(r)
	var data SnapshotData
	if err := d.Decode(&data); err != nil {
		return
	}
	kv.kvStore = data.KvStore
	kv.requestCookie = data.RequestCookie
	if kv.kvStore == nil {
		kv.kvStore = make(map[string]string)
	}
	if kv.requestCookie == nil {
		kv.requestCookie = make(map[int64]int64)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		OpType:    "Get",
		Key:       args.Key,
	}
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	waitChan, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan *Op, 1)
		waitChan = kv.waitChMap[index]
	}
	kv.mu.Unlock()

	select {
	case res := <-waitChan:
		reply.Value = res.Value
		reply.Err = OK
		currentTerm, stillLeader := kv.rf.GetState()
		if currentTerm != term || !stillLeader {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("[%d] timeout", kv.me)
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.waitChMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//非Get命令需要检查requestID是重传还是新请求，保持幂等性
	kv.mu.Lock()
	if lastRequestID, ok := kv.requestCookie[args.ClientID]; ok {
		if args.RequestID <= lastRequestID {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}

	op := Op{
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	waitChan, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan *Op, 1)
		waitChan = kv.waitChMap[index]
	}
	kv.mu.Unlock()

	select {
	case <-waitChan:
		reply.Err = OK
		currentTerm, stillLeader := kv.rf.GetState()
		if currentTerm != term || !stillLeader {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.waitChMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.PutAppend(args, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.PutAppend(args, reply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 监听applychan
func (kv *KVServer) applyChListener() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()

		if applyMsg.CommandValid {
			if applyMsg.CommandIndex <= kv.lastIncludedIndex {
				kv.mu.Unlock()
				continue
			}
			op := applyMsg.Command.(Op)
			kv.execute(&op)
			currentTerm, stillLeader := kv.rf.GetState()
			if currentTerm == applyMsg.CommandTerm && stillLeader {
				kv.notifyWaitCh(applyMsg.CommandIndex, &op)
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate {
				snapshot := kv.encodeSnapshot()
				kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
			}
			kv.lastIncludedIndex = applyMsg.CommandIndex
		} else if applyMsg.SnapshotValid {
			if applyMsg.SnapshotIndex <= kv.lastIncludedIndex {
				kv.mu.Unlock()
				continue
			}
			kv.decodeSnapshot(applyMsg.Snapshot)
			kv.lastIncludedIndex = applyMsg.SnapshotIndex
		}

		kv.mu.Unlock()
	}
}

// 执行op
func (kv *KVServer) execute(op *Op) {
	if op.OpType == "Get" {
		op.Value = kv.kvStore[op.Key]
		return
	}

	// 只对 Put/Append 去重：第一次见到该 client 时必须执行
	if last, ok := kv.requestCookie[op.ClientID]; ok && op.RequestID <= last {
		return // 重传的旧请求，直接忽略
	}

	if op.OpType == "Put" {
		kv.kvStore[op.Key] = op.Value
	} else if op.OpType == "Append" {
		kv.kvStore[op.Key] = kv.kvStore[op.Key] + op.Value
	}
	kv.requestCookie[op.ClientID] = op.RequestID
}

func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// 给waitCh发送通知，让其生成响应
// 在发送之前需要检查waitCh是否关闭
func (kv *KVServer) notifyWaitCh(index int, op *Op) {
	if waitCh, ok := kv.waitChMap[index]; ok {
		waitCh <- op
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.requestCookie = make(map[int64]int64)
	kv.waitChMap = make(map[int]chan *Op)

	kv.decodeSnapshot(kv.persister.ReadSnapshot())

	go kv.applyChListener()

	return kv
}
