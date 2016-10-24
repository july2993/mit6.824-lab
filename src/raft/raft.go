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
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const ElectionTimeout = time.Millisecond * 100
const PingPeerPeriod = time.Millisecond * 10
const MaxEntrysPerTime = 100

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
	CurrentTerm          int
	VotedFor             int
	Log                  []*LogEntry
	CommitIndex          int
	LeaderLastCommitTime time.Time
	LastAppllyed         int
	NextIndex            []int
	MatchIndex           []int

	lastResponseTime []time.Time

	Role           Role
	LastUpdateTime time.Time

	applyCh chan ApplyMsg

	//
	NeedAppend bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.CurrentTerm, rf.Role == LeaterRole
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)

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

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// Your code here.
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.setFlower()
	}

	reply.Term = rf.CurrentTerm

	if rf.Role != FlowerRole {
		reply.VoteGranted = false
		return
	}

	// 5.1
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}

	// check vote
	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	if len(rf.Log) > 0 {
		if rf.lastLog().Term > args.LastLogTerm ||
			rf.lastLog().Term == args.LastLogTerm && len(rf.Log) > args.LastLogIndex {
			reply.VoteGranted = false
			return
		}
	}
	// end check vote

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

// 5.2
func (rf *Raft) checkPreLog(prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex <= 0 {
		return true
	}

	if len(rf.Log) < prevLogIndex {
		return false
	}

	if rf.Log[prevLogIndex-1].Term != prevLogTerm {
		return false
	}

	return true
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if len(args.Entries) > 0 {
		DPrintf("*%+v* args: %+v reply: %+v\n", rf.me, args, *reply)
		DPrintf("log len: %v\n", len(rf.Log))
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.setFlower()
	}

	reply.Term = rf.CurrentTerm

	// 5.1
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}

	// 5.2
	if rf.checkPreLog(args.PrevLogIndex, args.PrevLogTerm) == false {
		reply.Success = false
		return
	}

	// 5.3
	for i := 0; i < len(args.Entries); i++ {
		rf.appendLogAt(args.Entries[i], args.PrevLogIndex+(i+1))
	}

	// ok
	canCommitIndex := minInt(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	if canCommitIndex > rf.CommitIndex {
		rf.CommitIndex = canCommitIndex
		DPrintf("%v flowwer commit: %d\n", rf.me, rf.CommitIndex)
	}

	if rf.Role == FlowerRole {
		rf.LastUpdateTime = time.Now()
	}

	reply.Success = true
	return
}

func minInt(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func (rf *Raft) appendLogAt(entry *LogEntry, index int) {
	if len(rf.Log)+1 < index {
		panic("err index")
	}

	if len(rf.Log)+1 == index {
		rf.Log = append(rf.Log, entry)
		return
	}

	if rf.Log[index-1].Term != entry.Term {
		rf.Log[index-1] = entry
		rf.Log = rf.Log[:index]
	}
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
	defer rf.persist()

	index := -1
	term := rf.CurrentTerm
	isLeader := (rf.Role == LeaterRole)

	defer func() {
		// fmt.Printf("Start retrun: %d %d %v\n", index, term, isLeader)
	}()

	// for i := 0; i < len(rf.Log); i++ {
	// 	if rf.Log[i].Command == command {
	// 		index = i + 1
	// 		return index, term, isLeader
	// 	}
	// }

	if rf.Role != LeaterRole {
		return index, term, isLeader
	}

	entry := &LogEntry{
		Term:    rf.CurrentTerm,
		Command: command,
	}

	rf.Log = append(rf.Log, entry)
	rf.MatchIndex[rf.me] = len(rf.Log)
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
	DPrintf("%v from %v to %v\n", rf.me, rf.Role, FlowerRole)
	rf.VotedFor = -1
	rf.Role = FlowerRole
	rf.LastUpdateTime = time.Now()
	rf.NeedAppend = false
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
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.lastResponseTime = make([]time.Time, len(peers))
	rf.me = me

	// Your initialization code here.
	rand.Seed(int64(me))

	rf.setFlower()
	rf.applyCh = applyCh
	rf.LastUpdateTime = time.Now().Add(-time.Duration(rand.Intn(int(ElectionTimeout))))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for _, log := range rf.Log {
		_ = log
		// fmt.Printf("*********** %v %v \n", log.Command, reflect.TypeOf(log.Command))
		// log.Command = int(reflect.ValueOf(log.Command).Float())
		// fmt.Printf("*********** %v %v \n", log.Command, reflect.TypeOf(log.Command))
	}

	// DPrintf("my role: %v", rf.Role)

	go rf.loop()

	return rf
}

// 5.3 5.4
func (rf *Raft) checkCanCommit(n int) bool {
	if len(rf.Log) < n || rf.Log[n-1].Term != rf.CurrentTerm {
		return false
	}

	matchCount := 0
	for i := 0; i < len(rf.peers); i++ {
		if rf.MatchIndex[i] >= n {
			matchCount++
		}
	}

	if matchCount*2 > len(rf.peers) {
		return true
	}

	return false
}

func (rf *Raft) pingPeer() {
	for i := 0; i < len(rf.peers); i++ {
		go func(server int) {
			var wait time.Duration = PingPeerPeriod
			for {
				time.Sleep(wait)
				wait = PingPeerPeriod

				rf.mu.Lock()
				// apply log

				if !rf.NeedAppend {
					rf.mu.Unlock()
					continue
				}

				// master commit log
				if rf.Role == LeaterRole {
					// DPrintf("*%+v* \n", *rf)
					for cn := rf.CommitIndex + 1; cn <= len(rf.Log); cn++ {
						if rf.checkCanCommit(cn) {
							DPrintf("%v leader commit: %d %+v\n", rf.me, cn, *rf.Log[cn-1])
							// fmt.Printf("leader commit: %d\n", rf.CommitIndex+1)
							rf.CommitIndex = cn
							rf.LeaderLastCommitTime = time.Now()
						}
					}
				}

				// leader or candidate AppendEntrys Flowwers
				var args AppendEntriesArgs
				args.Term = rf.CurrentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.CommitIndex

				if server == rf.me {
					rf.mu.Unlock()
					continue
				}

				if rf.Role == LeaterRole {
					args.Entries = nil
					args.PrevLogIndex = 0
					args.PrevLogTerm = 0

					if rf.NextIndex[server] == 0 || rf.NextIndex[server] > len(rf.Log)+1 {
						rf.NextIndex[server] = len(rf.Log) + 1
					}

					entryNum := len(rf.Log) - rf.NextIndex[server] + 1
					if entryNum > MaxEntrysPerTime {
						entryNum = MaxEntrysPerTime
					}

					start := rf.NextIndex[server] - 1
					if start+entryNum > len(rf.Log) || start < 0 {
						fmt.Println("fuck: ", len(rf.Log), start, entryNum)
					}
					args.Entries = append(args.Entries, rf.Log[start:start+entryNum]...)

					args.PrevLogIndex = rf.NextIndex[server] - 1
					if args.PrevLogIndex < 0 {
						args.PrevLogIndex = 0
					}
					if args.PrevLogIndex > 0 {
						args.PrevLogTerm = rf.Log[args.PrevLogIndex-1].Term
					}
				}

				var reply = new(AppendEntriesReply)
				rf.mu.Unlock()
				ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
				rf.mu.Lock()
				if ok {

					rf.feedResponseTime(server)
					// delay response
					if args.Term != rf.CurrentTerm {
						rf.mu.Unlock()
						continue
					}

					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.persist()
						rf.setFlower()
						rf.mu.Unlock()
						continue
					}
					if reply.Success {

						rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.NextIndex[server] = rf.MatchIndex[server] + 1
					} else {
						if rf.NextIndex[server] > 0 {
							rf.NextIndex[server]--
						}

						if rf.Role == LeaterRole {
							wait = 0
						}
					}
				} else {
					wait = 0
				}

				rf.mu.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) getPingCommand() (cmd interface{}) {
	i := rand.Int()
	if i > 0 {
		i = -i
	}

	cmd = i
	return
}

func (rf *Raft) loop() {

	go rf.pingPeer()

	go func() {
		for {
			select {
			case <-time.Tick(time.Millisecond * 500):
				rf.mu.Lock()
				DPrintf("tick %v --- %+v\n", rf.me, *rf)
				rf.mu.Unlock()

			}
		}
	}()

	var applyCh chan ApplyMsg
	var applyMsg ApplyMsg
	for {
		rf.mu.Lock()
		if applyCh == nil && rf.CommitIndex > rf.LastAppllyed {
			msg := ApplyMsg{
				Index:   rf.LastAppllyed + 1,
				Command: rf.Log[rf.LastAppllyed].Command,
			}
			applyCh = rf.applyCh
			applyMsg = msg
		}
		rf.mu.Unlock()

		select {
		case <-time.Tick(time.Millisecond * 1):
			rf.mu.Lock()
			// Start a no effect log avoid the previor term log can't be committed
			if rf.Role == LeaterRole {
				if rf.CommitIndex < len(rf.Log) && rf.getLastLogTerm() != rf.CurrentTerm && rf.LeaderLastCommitTime.Add(ElectionTimeout*3).Before(time.Now()) {
					rf.mu.Unlock()
					rf.Start(rf.getPingCommand())
					rf.mu.Lock()
				}

				// can't still connected with majoratry
				count := 0
				for idx, lastTime := range rf.lastResponseTime {
					if idx == rf.me || time.Now().Sub(lastTime) < ElectionTimeout {
						count++
					}
				}
				if count*2 <= len(rf.peers) {
					go rf.beCandidate()
					rf.mu.Unlock()
					continue
				}
			}

			now := time.Now()
			if rf.Role == FlowerRole && rf.LastUpdateTime.Add(ElectionTimeout).Before(now) {
				go rf.beCandidate()
			}
			rf.mu.Unlock()
		case applyCh <- applyMsg:
			applyCh = nil
			rf.mu.Lock()
			rf.LastAppllyed++
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) beCandidate() {
	rf.mu.Lock()
	if rf.Role == CandidateRole {
		panic("repeat candidate")
	}
	rf.Role = CandidateRole
	rf.mu.Unlock()
	DPrintf("%+v: beCandidate", rf.me)

	nextElect := time.NewTimer(time.Millisecond)

	for {
		select {
		case <-nextElect.C:
			rf.mu.Lock()
			if rf.Role != CandidateRole {
				DPrintf("%+v: to be not candidate\n", rf.me)
				rf.mu.Unlock()
				return
			}

			rf.CurrentTerm++
			rf.persist()
			rf.NeedAppend = true
			tryTerm := rf.CurrentTerm
			DPrintf("try elect: %+v\n", *rf)
			rf.mu.Unlock()
			if rf.tryGetVote() {
				DPrintf("%+v: %v vote success", *rf, rf.me)
				rf.mu.Lock()
				if tryTerm == rf.CurrentTerm {
					rf.Role = LeaterRole
					rf.MatchIndex = make([]int, len(rf.peers))
					rf.NextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.NextIndex); i++ {
						rf.NextIndex[i] = len(rf.Log) + 1
					}
					rf.LeaderLastCommitTime = time.Now()
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				if tryTerm != rf.CurrentTerm {
					rf.mu.Unlock()
					continue
				}
				rf.NeedAppend = false
				rf.mu.Unlock()
				wait := time.Duration(rand.Intn(2 * int(ElectionTimeout)))
				DPrintf("vote fail wait: +%v\n", wait)
				nextElect = time.NewTimer(wait)
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

func (rf *Raft) feedResponseTime(server int) {
	now := time.Now()
	if rf.lastResponseTime[server].Before(now) {
		rf.lastResponseTime[server] = now
	}
}

func (rf *Raft) tryGetVote() bool {
	rf.mu.Lock()
	var args RequestVoteArgs
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.Log)
	args.LastLogTerm = rf.getLastLogTerm()
	args.Term = rf.CurrentTerm
	rf.mu.Unlock()

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
			if ok {
				rf.feedResponseTime(peerId)
				if reply.VoteGranted {
					votes <- true
				}
			} else {
				votes <- false
			}
		}(i)
	}

	recvResultCount := 0
	timeout := time.NewTimer(ElectionTimeout)
	for {
		select {
		case vote := <-votes:
			recvResultCount++
			if vote {
				voteCount++
			}
			if voteCount*2 > len(rf.peers) {
				return true
			}

			if (len(rf.peers)-recvResultCount+voteCount)*2 <= len(rf.peers) {
				return false
			}
		case <-timeout.C:
			return false
		}
	}

	return false
}
