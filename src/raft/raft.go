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

const ElectionTimeout = time.Millisecond * 500
const PingPeerPeriod = time.Millisecond * 50
const MaxEntrysPerTime = 300

type Role int

const (
	LeaderRole    Role = 1
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
	votes     int
	beLeader  chan bool
	beFlowwer chan bool
	heartBeat chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.CurrentTerm, rf.Role == LeaderRole
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

	// myself
	reply.Term = rf.CurrentTerm
	if args.CandidateId == rf.me {
		reply.VoteGranted = true
		return
	}

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
	Term      int
	Success   bool
	NextIndex int
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

	// DPrintf("%v handle AppendEntries\n", rf.me)

	rf.heartBeat <- true

	reply.Term = rf.CurrentTerm
	reply.NextIndex = args.PrevLogIndex
	if reply.NextIndex == 0 {
		reply.NextIndex = 1
	}

	// 5.1
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}

	// 5.2
	if rf.checkPreLog(args.PrevLogIndex, args.PrevLogTerm) == false {
		reply.Success = false
		if args.PrevLogIndex > len(rf.Log) {
			reply.NextIndex = len(rf.Log) + 1
		} else {
			reply.NextIndex = args.PrevLogIndex
			term := rf.Log[args.PrevLogIndex-1].Term
			for reply.NextIndex > 1 && rf.Log[reply.NextIndex-1].Term == term {
				reply.NextIndex--
			}
		}
		return
	}

	// 5.3
	for i := 0; i < len(args.Entries); i++ {
		rf.appendLogAt(args.Entries[i], args.PrevLogIndex+(i+1))
	}

	// ok match PrevLogIndex
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

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		rf.feedResponseTime(server)

		if args.Term != rf.CurrentTerm {
			return ok
		}

		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.persist()
			rf.setFlower()
			return ok
		}

		if reply.VoteGranted {
			rf.votes++
		}
		if rf.votes*2 > len(rf.peers) {
			if rf.Role == CandidateRole {
				rf.beLeader <- true
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		rf.feedResponseTime(server)

		if args.Term != rf.CurrentTerm {
			return ok
		}

		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.persist()
			rf.setFlower()
			return ok
		}

		if reply.Success {
			rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.NextIndex[server] = rf.MatchIndex[server] + 1

			if rf.Role == LeaderRole {
				for cn := rf.CommitIndex + 1; cn <= len(rf.Log); cn++ {
					if rf.checkCanCommit(cn) {
						DPrintf("%v leader commit: %d %+v\n", rf.me, cn, *rf.Log[cn-1])
						// fmt.Printf("leader commit: %d\n", rf.CommitIndex+1)
						rf.CommitIndex = cn
						rf.LeaderLastCommitTime = time.Now()
					}
				}
			}

		} else {
			rf.NextIndex[server] = reply.NextIndex
			// if rf.NextIndex[server] > 0 {
			// 	rf.NextIndex[server]--
			// }

		}
	}

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
	isLeader := (rf.Role == LeaderRole)

	defer func() {
		// fmt.Printf("Start retrun: %d %d %v\n", index, term, isLeader)
	}()

	// for i := 0; i < len(rf.Log); i++ {
	// 	if rf.Log[i].Command == command {
	// 		index = i + 1
	// 		return index, term, isLeader
	// 	}
	// }

	if rf.Role != LeaderRole {
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
	rf.beFlowwer <- true
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
	rf.beLeader = make(chan bool, 1000)
	rf.beFlowwer = make(chan bool, 1000)
	rf.heartBeat = make(chan bool, 1000)

	// Your initialization code here.
	rand.Seed(int64(me))

	rf.setFlower()
	rf.applyCh = applyCh
	rf.LastUpdateTime = time.Now().Add(-time.Duration(rand.Intn(int(ElectionTimeout))))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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

func (rf *Raft) boardCaseAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var argsTPL AppendEntriesArgs
	argsTPL.Term = rf.CurrentTerm
	argsTPL.LeaderId = rf.me
	argsTPL.LeaderCommit = rf.CommitIndex

	for server := 0; server < len(rf.peers); server++ {
		var args AppendEntriesArgs = argsTPL
		if rf.Role == LeaderRole {
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
		go rf.sendAppendEntries(server, args, reply)
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

	// debug
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
	// end debug

	// apply log
	go func() {
		for {
			rf.mu.Lock()
			if rf.CommitIndex == rf.LastAppllyed {
				rf.mu.Unlock()
				time.Sleep(time.Millisecond)
				continue
			}
			msg := ApplyMsg{
				Index:   rf.LastAppllyed + 1,
				Command: rf.Log[rf.LastAppllyed].Command,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.LastAppllyed++
			rf.mu.Unlock()
		}
	}()

	for {
		switch rf.Role {
		case LeaderRole:
			select {
			case <-rf.beFlowwer:
			default:
				rf.boardCaseAppendEntries()
				time.Sleep(PingPeerPeriod)

				rf.mu.Lock()
				if rf.CommitIndex < len(rf.Log) && rf.getLastLogTerm() != rf.CurrentTerm && rf.LeaderLastCommitTime.Add(ElectionTimeout*3).Before(time.Now()) {
					rf.mu.Unlock()
					rf.Start(rf.getPingCommand())
					rf.mu.Lock()
				}

				// can't still connected with majoratry
				count := 0
				for idx, lastTime := range rf.lastResponseTime {
					if idx == rf.me || time.Now().Sub(lastTime) <= ElectionTimeout {
						count++
					}
				}
				if count*2 <= len(rf.peers) {
					DPrintf("%v leader be CandidateRole\n", rf.me)
					rf.Role = CandidateRole
					rf.mu.Unlock()
					continue
				}
				rf.mu.Unlock()
			}
		case CandidateRole:
			rf.mu.Lock()
			rf.votes = 0
			rf.VotedFor = rf.me
			rf.CurrentTerm++
			rf.persist()
			rf.mu.Unlock()
			go rf.boardCatRequestVote()
			timeout := ElectionTimeout
			select {
			case <-rf.beLeader:
				rf.mu.Lock()
				DPrintf("%v be leader\n", rf.me)
				rf.Role = LeaderRole
				rf.MatchIndex = make([]int, len(rf.peers))
				rf.NextIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.NextIndex); i++ {
					rf.NextIndex[i] = len(rf.Log) + 1
				}
				rf.LeaderLastCommitTime = time.Now()
				rf.mu.Unlock()
			case <-time.NewTimer(timeout).C:
				rf.mu.Lock()
				rf.Role = FlowerRole
				DPrintf("%v vote timeout\n", rf.me)
				rf.mu.Unlock()
			case <-rf.beFlowwer:
			}
		case FlowerRole:
			timeout := ElectionTimeout + time.Duration(rand.Intn(int(ElectionTimeout*2)+22))
			select {
			case <-rf.heartBeat:
				// DPrintf("%v receive heartBeat\n", rf.me)
			case <-rf.beFlowwer:
			case <-time.NewTimer(timeout).C:
				rf.mu.Lock()
				DPrintf("flowwer timeout: %v\n", rf.me)
				rf.Role = CandidateRole
				rf.mu.Unlock()
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

func (rf *Raft) boardCatRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var args RequestVoteArgs
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.Log)
	args.LastLogTerm = rf.getLastLogTerm()
	args.Term = rf.CurrentTerm

	for i := 0; i < len(rf.peers); i++ {
		var resp = new(RequestVoteReply)
		go rf.sendRequestVote(i, args, resp)
	}
}
