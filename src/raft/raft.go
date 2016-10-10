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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const ElectionTimeout = time.Millisecond * 100
const PingPeerPeriod = time.Millisecond * 1

type Role int

const (
	LeaterRole    Role = 1
	CandidateRole Role = 2
	FlowerRole    Role = 3
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm  int
	VotedFor     int
	Log          []*LogEntry
	CommitIndex  int
	LastAppllyed int
	NextIndex    []int
	MatchIndex   []int

	Role           Role
	LastUpdateTime time.Time

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.CurrentTerm
	isleader = (rf.Role == LeaterRole)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

func (rf *Raft) lastLog() *LogEntry {
	return rf.Log[len(rf.Log)-1]
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here.
	reply.Term = rf.CurrentTerm
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.setFlower()
	}

	if rf.Role != FlowerRole {
		reply.VoteGranted = false
		return
	}

	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	if args.LastLogIndex < len(rf.Log) {
		reply.VoteGranted = false
		return
	}

	if len(rf.Log) > 0 && rf.lastLog().Term < args.LastLogTerm {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.VotedFor = args.CandidateId
	return
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	if rf.Role == FlowerRole {
		rf.LastUpdateTime = time.Now()
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.setFlower()
	}

	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}

	reply.Success = true
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.Role != LeaterRole {
		return -1, -1, false
	}

	index := -1
	term := -1
	isLeader := true

	for i := 0; i < len(rf.Log); i++ {
		if rf.Log[i].Command == command {
			index = i + 1
			if rf.CommitIndex >= index {
				term = rf.CurrentTerm
			}
			return index, term, isLeader
		}
	}

	entry := &LogEntry{
		Term:    rf.CurrentTerm,
		Command: command,
	}

	rf.Log = append(rf.Log, entry)
	index = len(rf.Log)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) setFlower() {
	// 不可重复设置导致投票给不同peer
	if rf.Role == FlowerRole {
		return
	}
	rf.VotedFor = -1
	rf.Role = FlowerRole
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.setFlower()
	rf.applyCh = make(chan ApplyMsg, 10)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// DPrintf("my role: %v", rf.Role)

	go rf.loog()

	return rf
}

func (rf *Raft) pingPeer() {
	for _ = range time.Tick(PingPeerPeriod) {
		if rf.Role != LeaterRole && rf.Role != CandidateRole {
			continue
		}

		var args AppendEntriesArgs
		args.Term = rf.CurrentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.CommitIndex
		args.Entries = nil
		// my todo
		// args.PrevLogIndex = len(rf.Log)
		// args.PrevLogTerm = rf.lastLog()
		for i := 0; i < len(rf.peers); i++ {
			func(server int) {
				var reply = new(AppendEntriesArgs)
				rf.peers[server].Call("Raft.AppendEntries", args, reply)
			}(i)
		}
	}
}

func (cf *Raft) appendFlower() {

}

func (rf *Raft) loog() {
	go rf.pingPeer()

	for {
		select {
		case <-time.Tick(time.Millisecond * 10):
			now := time.Now()
			if rf.Role == FlowerRole && rf.LastUpdateTime.Add(ElectionTimeout).Before(now) {
				go rf.beCandidate()
				continue
			}
		}

	}
}

func (rf *Raft) beCandidate() {
	rf.Role = CandidateRole
	DPrintf("%+v: beCandidate", *rf)

	nextElect := time.NewTimer(time.Millisecond)

	for {
		select {
		case <-nextElect.C:
			if rf.Role != CandidateRole {
				return
			}

			rf.CurrentTerm++
			tryTerm := rf.CurrentTerm
			if rf.tryGetVote() {
				DPrintf("%+v: vote success", *rf)
				if tryTerm == rf.CurrentTerm {
					rf.Role = LeaterRole
				}
				return
			} else {
				nextElect = time.NewTimer(ElectionTimeout + time.Duration(rand.Intn(int(ElectionTimeout))))
			}

		}
	}

}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.Log) == 0 {
		return 0
	}

	return rf.lastLog().Term
}

func (rf *Raft) tryGetVote() bool {
	var args RequestVoteArgs
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.Log)
	args.LastLogTerm = rf.getLastLogTerm()
	args.Term = rf.CurrentTerm

	votes := make(chan bool, len(rf.peers))
	voteCount := 0
	for i := 0; i < len(rf.peers); i++ {
		go func(peerId int) {
			if peerId == rf.me {
				votes <- true
				return
			}

			var reply = new(RequestVoteReply)
			ok := rf.peers[peerId].Call("Raft.RequestVote", args, reply)
			if ok && reply.VoteGranted {
				votes <- true
			} else {
				votes <- false
			}
		}(i)
	}

	recvResultCount := 0
	for vote := range votes {
		recvResultCount++
		if vote {
			voteCount++
		}
		if voteCount*2 > len(rf.peers) {
			return true
		}

		if recvResultCount == len(rf.peers) {
			return false
		}
	}

	return false
}
