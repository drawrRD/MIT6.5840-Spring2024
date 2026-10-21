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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

	role Role
	// Persistent state (paper's Figure 2).
	// 只持久化那些丢失后会导致安全性违规的字段
	//如果一个服务器在重启后无法恢复某个状态，而这个状态的丢失会导致它违反 Raft 的安全性属性，那么这个状态就必须持久化
	currentTerm int
	voteFor     int
	log         []LogEntry

	// `nextIndex[i]` = 下一个要发给 peer i 的日志索引
	nextIndex []int
	// `matchIndex[i]` =  peer i 已经成功复制的最高日志索引
	matchIndex []int

	// 用它把已提交日志交给上层服务
	applyCh chan ApplyMsg

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

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1 // 乐观假设 follower 和自己一样新
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) lastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// 切片中第一个日志的全局索引
func (rf *Raft) lastIncludedIndex() int {
	return rf.log[0].Index
}
func (rf *Raft) lastIncludedTerm() int {
	return rf.log[0].Term
}

// logAt 将 全局索引index 映射到 切片索引。
func (rf *Raft) logAt(index int) LogEntry {
	return rf.log[index-rf.log[0].Index]
}

// startElection kicks off a RequestVote round. Caller must hold rf.mu.
func (rf *Raft) startElection() {
	rf.becomeCandidate()

	votes := 1 // vote for self

	term := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
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
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		rf.replicateLog(server)
	}
}

func (rf *Raft) replicateLog(server int) {
	nextIndex := rf.nextIndex[server]
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.logAt(prevLogIndex).Term
	var entries []LogEntry
	if nextIndex <= rf.lastLogIndex() {
		entries = make([]LogEntry, rf.lastLogIndex()-nextIndex+1)
		copy(entries, rf.log[nextIndex-rf.lastIncludedIndex():])
	}

	term := rf.currentTerm
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	go func(server int) {
		reply := &AppendEntriesReply{}
		if !rf.sendAppendEntries(server, args, reply) {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != Leader || rf.currentTerm != term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			if match > rf.matchIndex[server] {
				rf.matchIndex[server] = match
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			// update commit index
			for i := rf.lastLogIndex(); i > rf.commitIndex; i-- {
				if rf.logAt(i).Term != rf.currentTerm {
					break
				}
				cnt := 1
				for j := range rf.peers {
					if j == rf.me {
						continue
					}
					if rf.matchIndex[j] >= i {
						cnt++
					}
				}
				if cnt > len(rf.peers)/2 {
					rf.commitIndex = i
					break
				}
			}

		} else {
			// follower 日志不一致：
			// 快速回退 nextIndex 并立即重试
			next := rf.nextIndex[server]

			if reply.ConflictTerm == -1 {
				// follower 日志太短 → nextIndex = XLen
				next = reply.ConflictIndex
			} else {
				// 在 leader 自己的日志里找 ConflictTerm 的最后一条
				last := -1
				for i := rf.lastLogIndex(); i > rf.lastIncludedIndex(); i-- {
					t := rf.logAt(i).Term
					if t == reply.ConflictTerm {
						last = i
						break
					}
					if t < reply.ConflictTerm {
						break // 任期单调不降，再往前不会有该任期了
					}
				}
				if last == -1 {
					// leader 没有该任期 → nextIndex = XIndex
					// index 	1 2 3 4 5
					// leader 	3 3 5 5 5
					// follower 3 3 4 4 4

					// leader send index 5
					// Xindex = 3
					// last = -1
					// nextIndex = 3
					next = reply.ConflictIndex
				} else {
					// leader 也有该任期 → nextIndex = 该任期最后一条 + 1
					// index 	1 2 3 4 5 6 7 8 9
					// leader 	3 3 3 4 4 4 4 5 5
					// follower 3 3 3 4 4 4 4 4 4

					// leader send index 9
					// Xindex = 4
					// last = 7
					// nextIndex = 7 + 1 = 8
					next = last + 1
				}
			}
			// 边界保护：必须落在 [1, lastLogIndex+1]
			if next < rf.lastIncludedIndex()+1 {
				next = rf.lastIncludedIndex() + 1 // 0处非日志（哨兵），不能回退
			}
			if next > rf.lastLogIndex()+1 {
				next = rf.lastLogIndex() + 1 // Entry为空，纯心跳
			}

			// 避免信息异常时死循环
			if next >= rf.nextIndex[server] {
				next = rf.nextIndex[server] - 1
				if next < rf.lastIncludedIndex()+1 {
					next = rf.lastIncludedIndex() + 1
				}
			}

			rf.nextIndex[server] = next
			// 不在这里立即重发：long reordering 下每个延迟的失败回复都会再触发
			// 一次携带整段日志的大包重传，叠加 100ms 心跳会造成大量重复 RPC；
			// 下一轮心跳会带着修正后的 nextIndex 重试，足够快。
		}
	}(server)
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	rf.persister.Save(w.Bytes(), nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		return
	}
	if err := d.Decode(&voteFor); err != nil {
		return
	}
	if err := d.Decode(&log); err != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.log = log
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
	} else if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			// 注意：这里不能走 becomeFollower——它会重置选举计时器。
			// 日志落后但任期更高的节点会不停发 RequestVote（它永远选不上），
			// 若每次都重置别人的计时器，集群将长期无人能超时发起选举。
			// 论文 Figure 2 中计时器只在收到 AppendEntries 时重置。
			rf.role = Follower
			rf.currentTerm = args.Term
			rf.voteFor = -1
			rf.persist()
		}
		// Grant only if the candidate's log is at least as up-to-date as ours.
		upToDate := args.LastLogTerm > rf.lastLogTerm() ||
			(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())

		if upToDate && rf.voteFor == -1 { //(rf.voteFor == -1 || rf.voteFor == args.CandidateID)
			rf.voteFor = args.CandidateID
			rf.persist()
			rf.resetElectionTimer()
			reply.VoteGranted = true
		}
	}

}

// AppendEntries RPC arguments. For 3A only the heartbeat fields are
// used; log replication (3B) will add PrevLogIndex/PrevLogTerm/Entries.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int        //leader 认为该 follower 在"这批新日志之前"应已有此索引的条目
	PrevLogTerm  int        //上述条目的任期，follower 用它做一致性校验
	Entries      []LogEntry //为空=心跳
	LeaderCommit int        //leader 的`commitIndex` ，follower 据此推进自己的`commitIndex`
}

// AppendEntries RPC reply.
type AppendEntriesReply struct {
	Term    int
	Success bool

	// 快速回退：leader 收到失败回复后（Success=false），不是一条一条 nextIndex--，而是尽量跳过整段冲突 term：
	ConflictTerm  int //follower 在冲突位置的 term；如果 follower 太短，可能是 -1
	ConflictIndex int //leader 下次可以尝试的更早 index
}

// AppendEntries RPC handler. In 3A it only handles the heartbeat case:
// if the leader's term is valid, reset our election timer.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if rf.role == Dead {
		return
	}

	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else {
		// 若清空状态回退到follower，会丢失voteFor
		// 若同一时刻有个candidate还未收到leader的心跳，向该回退的follower请求了投票，
		// 由于该follower的voteFor重制为了-1，会投票给该candidate，于是一轮中follower投了两票
		rf.role = Follower
		rf.resetElectionTimer()
	}

	// 一致性校验
	// 维护一致性【Log Matching Property】任意两个日志若在同索引同任期，则此前全部条目必须一致
	if args.PrevLogIndex > rf.lastLogIndex() {
		//XLen 场景：follower 日志太短，直接告诉 leader 自己的日志长度
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1 // XLen：全局索引，不能用 len(log)
		return
	}
	if args.PrevLogTerm != rf.logAt(args.PrevLogIndex).Term {
		conflictTerm := rf.logAt(args.PrevLogIndex).Term
		// index     1 2 3 4 5
		// leader:   3 3 3 5
		// follower: 3 3 4 4 4//简单存放term
		// leader发index=4，follower有index=4，但term 5 ！= 4，冲突
		// 找到该冲突任期的第一条日志（XIndex）
		// XIndex = 3
		idx := args.PrevLogIndex
		for idx > rf.lastIncludedIndex() && rf.logAt(idx-1).Term == conflictTerm {
			idx--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = idx
		return
	}

	// 追加日志。
	// 注意：RPC 可能乱序到达（Figure8 会开启 long reordering），
	// 因此已存在且任期一致的条目必须跳过，绝不能从它开始截断，
	// 否则会误删更晚到达的、更新的日志后缀。
	changed := false
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastLogIndex() {
			if rf.logAt(idx).Term == entry.Term {
				continue // 同索引同任期 => 同一条日志，保留后面的条目
			}
			rf.log = rf.log[:idx-rf.lastIncludedIndex()] // 冲突才截断
		}
		rf.log = append(rf.log, entry)
		changed = true
	}
	if changed {
		rf.persist()
	}

	// 推进commitIndex
	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.PrevLogIndex + len(args.Entries) //已确认复制的最后日志的索引
		if newCommit < args.LeaderCommit {
			rf.commitIndex = newCommit
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	reply.Term = rf.currentTerm
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
	isLeader := false

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return index, term, isLeader
	}

	index = rf.lastLogIndex() + 1
	term = rf.currentTerm
	isLeader = true
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	})
	rf.persist()
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
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for !rf.killed() {
		<-ticker.C

		rf.mu.Lock()
		switch rf.role {
		case Follower, Candidate:
			if time.Since(rf.electionStartTime) >= rf.electionTimeout {
				// 必须持锁同步触发：若用 go 延迟执行，期间收到心跳会重置计时，
				// 但已排队的选举仍会发起，造成本不该有的 term 膨胀与换主。
				rf.startElection()
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

func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond) // 空闲时才歇，避免忙等
			continue
		}
		// 一次取走当前全部可提交条目，锁外批量投递。
		// 若每条都等一次 ticker，吞吐会被限制在 100 条/秒，
		// follower 断开重连后日志积压较多时根本追不上（Figure8 正是如此）。
		start := rf.lastApplied + 1
		end := rf.commitIndex
		batch := make([]ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			entry := rf.logAt(i)
			batch = append(batch, ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			})
		}
		rf.lastApplied = end
		rf.mu.Unlock()

		for _, msg := range batch {
			rf.applyCh <- msg // 持锁期间绝不能发 applyCh！通道可能阻塞
		}
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
	rf.applyCh = applyCh

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
	go rf.applyLoop()

	return rf
}
