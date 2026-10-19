package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state (paper's Figure 2).
	role        Role
	currentTerm int
	voteFor     int
	log         []LogEntry

	// Volatile state.
	commitIndex int
	lastApplied int

	// Election / heartbeat timing.
	electionStartTime time.Time
	electionTimeout   time.Duration
	lastHeartbeat     time.Time
}

// LogEntry is one entry in the replicated log. Index 0 is a sentinel
// entry ({Term: 0, Index: 0}) so the log is never empty.
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
	Dead
)

const (
	HeartbeatInterval = 100 * time.Millisecond
	ElectionMinMs     = 300
	ElectionMaxMs     = 500
)

func (rf *Raft) resetElectionTimer() {
	rf.electionStartTime = time.Now()
	rf.electionTimeout = time.Duration(ElectionMinMs+rand.Intn(ElectionMaxMs-ElectionMinMs+1)) * time.Millisecond
}

// becomeFollower transitions to follower because we learned of a
// strictly higher term. Caller must hold rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.voteFor = -1
	rf.resetElectionTimer()
	rf.persist()
}

// becomeCandidate starts a new election. Caller must hold rf.mu.
func (rf *Raft) becomeCandidate() {
	rf.role = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.resetElectionTimer()
	rf.persist()
}

// becomeLeader takes leadership after winning an election.
// Caller must hold rf.mu.
func (rf *Raft) becomeLeader() {
	rf.role = Leader
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) lastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// startElection kicks off a RequestVote round. Caller must hold rf.mu.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeCandidate()

	votes := 1 // vote for self
	term := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogTerm:  rf.lastLogTerm(),
		LastLogIndex: rf.lastLogIndex(),
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != Candidate || rf.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeader()
				}
			} else if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
		}(server)
	}
}

// runHeartbeats sends an empty AppendEntries (heartbeat) to every peer.
// It is called without holding rf.mu.
func (rf *Raft) runHeartbeats() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := &AppendEntriesReply{}
			if !rf.sendAppendEntries(server, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
		}(server)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogTerm  int
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Dead {
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}else if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
		}
		// Grant only if the candidate's log is at least as up-to-date as ours.
		upToDate := args.LastLogTerm > rf.lastLogTerm() ||
			(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())
	
		if upToDate && rf.voteFor == -1 { //(rf.voteFor == -1 || rf.voteFor == args.CandidateID)
			rf.voteFor = args.CandidateID
			rf.electionStartTime = time.Now()
			reply.VoteGranted = true
		}
	}

}

// AppendEntries RPC arguments. For 3A only the heartbeat fields are
// used; log replication (3B) will add PrevLogIndex/PrevLogTerm/Entries.
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// (3B) PrevLogIndex int
	// (3B) PrevLogTerm  int
	// (3B) Entries      []LogEntry
	// (3B) LeaderCommit int
}

// AppendEntries RPC reply.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler. In 3A it only handles the heartbeat case:
// if the leader's term is valid, reset our election timer.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Dead {
		return
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else {
		// Same term: a valid leader is still alive, reset the timer
		// without disturbing votedFor.
		// 若清空状态回退到follower，会丢失voteFor
		// 若同一时刻有个candidate还未收到leader的心跳，向该回退的follower请求了投票，
		// 由于该follower的voteFor重制为了-1，会投票给该candidate，于是一轮中follower投了两票
		rf.role = Follower
		rf.resetElectionTimer()
	}
	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries sends an AppendEntries (heartbeat) RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker is the single long-running goroutine driving elections and
// heartbeats for this peer. It checks the current role and acts
// accordingly, so we do not spawn a new goroutine on every role change.
func (rf *Raft) ticker() {
	for !rf.killed() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		<-ticker.C

		rf.mu.Lock()
		switch rf.role {
		case Follower, Candidate:
			if time.Since(rf.electionStartTime) >= rf.electionTimeout {
				go rf.startElection()
			}
		case Leader:
			if time.Since(rf.lastHeartbeat) >= HeartbeatInterval {
				rf.lastHeartbeat = time.Now()
				go rf.runHeartbeats()
			}
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role = Follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.resetElectionTimer()
	rf.lastHeartbeat = time.Now()

	// Index 0 is a sentinel so the log is never empty.
	rf.log = make([]LogEntry, 0, 16)
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start the single ticker goroutine to drive elections and heartbeats
	go rf.ticker()

	return rf
}
