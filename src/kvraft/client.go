package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID int64
	RequestID int64
	leaderID int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientID = nrand()
	ck.RequestID = 0
	ck.leaderID = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
		ClientID: ck.ClientID,
		RequestID: ck.RequestID,
	}
	for {
		reply := GetReply{}

		if ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply); !ok{
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue;
		}

		switch reply.Err {
		case OK:
			ck.RequestID++
			return reply.Value
		case ErrNoKey:
			ck.RequestID++
			return ""
		case ErrWrongLeader:
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ClientID: ck.ClientID,
		RequestID: ck.RequestID,
	}
	for {
		reply := PutAppendReply{}

		if ok := ck.servers[ck.leaderID].Call("KVServer."+op, &args, &reply); !ok{
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue;
		}

		switch reply.Err {
		case OK, ErrNoKey:
			ck.RequestID++
			return
		case ErrWrongLeader:
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
