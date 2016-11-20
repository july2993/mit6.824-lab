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
	Index   int
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

	Role Role

	applyCh chan ApplyMsg
	killed  chan struct{}

	//
	votes     int
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

func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
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

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
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

func (rf *Raft) truncatelog(lastIndex int, lastTerm int, logs []*LogEntry) []*LogEntry {
	var newLogs []*LogEntry
	newLogs = append(newLogs, &LogEntry{Index: lastIndex, Term: lastTerm})
	for i := 0; i < len(logs); i++ {
		if logs[i].Index == lastIndex && logs[i].Term == lastTerm {
			newLogs = append(newLogs, logs[i+1:]...)
			break
		}
	}

	return newLogs
}

func (rf *Raft) sendSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return false
	}

	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.setFlower()
		return false
	}

	rf.NextIndex[server] = args.LastIncludeIndex + 1
	rf.MatchIndex[server] = args.LastIncludeIndex

	return ok
}

func (rf *Raft) readSnapshot(data []byte) {

	if len(data) == 0 {
		return
	}

	buffer := bytes.NewBuffer(data)
	d := gob.NewDecoder(buffer)
	var index int
	var term int

	d.Decode(&index)
	d.Decode(&term)

	rf.CurrentTerm = term

	rf.Log = rf.truncatelog(index, term, rf.Log)
	rf.CommitIndex = index
	rf.LastAppllyed = index

	msg := ApplyMsg{
		Index:       index,
		UseSnapshot: true,
		Snapshot:    data,
	}

	go func() {
		rf.applyCh <- msg
	}()
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		return
	}

	rf.heartBeat <- true
	rf.Role = FlowerRole

	rf.persister.SaveSnapshot(args.Data)
	rf.Log = rf.truncatelog(args.LastIncludeIndex, args.LastIncludeTerm, rf.Log)

	msg := ApplyMsg{
		Index:       args.LastIncludeIndex,
		UseSnapshot: true,
		Snapshot:    args.Data,
	}

	rf.LastAppllyed = args.LastIncludeIndex
	rf.CommitIndex = args.LastIncludeIndex

	rf.persist()
	rf.applyCh <- msg
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	DPrintf("*%v* start snapshot %v", rf.me, index)
	defer func() {
		DPrintf("*%v* end start snapshot %v", rf.me, index)
	}()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.Log[0].Index
	lastIndex := rf.getLastIndex()

	if index > lastIndex || index <= baseIndex {
		return
	}

	var newLogs []*LogEntry

	newLogs = append(newLogs, &LogEntry{Index: index, Term: rf.Log[index-baseIndex].Term})
	newLogs = append(newLogs, rf.Log[index-baseIndex+1:]...)

	rf.Log = newLogs

	buffer := new(bytes.Buffer)
	e := gob.NewEncoder(buffer)
	e.Encode(newLogs[0].Index)
	e.Encode(newLogs[0].Term)
	data := buffer.Bytes()
	data = append(data, snapshot...)

	rf.persist()
	rf.persister.SaveSnapshot(data)
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() {
		DPrintf("*%v*  Handle RequestVote: %+v %+v", rf.me, args, *reply)
	}()
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
			rf.lastLog().Term == args.LastLogTerm && rf.getLastIndex() > args.LastLogIndex {
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

func (rf *Raft) indexIdx(index int) int {
	if len(rf.Log) == 0 {
		return 0
	}

	base := rf.Log[0].Index

	return index - base
}

// 5.2
func (rf *Raft) checkPreLog(prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex <= 0 {
		return true
	}

	if len(rf.Log) == 0 || rf.getLastIndex() < prevLogIndex {
		return false
	}

	// because of snapshot, master may has't not all the log
	baseIndex := rf.Log[0].Index
	if prevLogIndex < baseIndex {
		return false
	}

	idx := rf.indexIdx(prevLogIndex)

	if rf.Log[idx].Term != prevLogTerm {
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
		if args.PrevLogIndex > rf.getLastIndex() {
			reply.NextIndex = rf.getLastIndex() + 1
		} else {
			reply.NextIndex = args.PrevLogIndex
			var baseIndex int
			if len(rf.Log) > 0 {
				baseIndex = rf.Log[0].Index
			}

			if len(rf.Log) > 0 && args.PrevLogIndex >= rf.Log[0].Index {
				term := rf.Log[rf.indexIdx(args.PrevLogIndex)].Term
				for reply.NextIndex > 1 && reply.NextIndex > baseIndex && rf.Log[rf.indexIdx(reply.NextIndex)].Term == term {
					reply.NextIndex--
				}
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
		DPrintf("%v flowwer commit: %d %+v %v\n", rf.me, rf.CommitIndex, rf.Log[rf.indexIdx(rf.CommitIndex)], args)
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

func (rf *Raft) getLastIndex() int {
	if len(rf.Log) == 0 {
		return 0
	}

	return rf.Log[len(rf.Log)-1].Index
}

func (rf *Raft) appendLogAt(entry *LogEntry, index int) {
	if rf.getLastIndex()+1 < index {
		panic("err index")
	}

	if rf.getLastIndex()+1 == index {
		rf.Log = append(rf.Log, entry)
		return
	}

	for i := len(rf.Log) - 1; i >= 0; i-- {
		if rf.Log[i].Index == index {
			if rf.Log[i].Term != entry.Term {
				rf.Log[i] = entry
				rf.Log = rf.Log[:i+1]
			}
			break
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply, beLeader chan struct{}) bool {
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
			DPrintf("*%v* get votes from %v %+v %+v", rf.me, server, args, *reply)
		}
		if rf.votes*2 > len(rf.peers) {
			if rf.Role == CandidateRole {
				// close to signal be leader
				select {
				case <-beLeader:
				default:
					close(beLeader)
				}
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
				for cn := rf.CommitIndex + 1; cn <= rf.getLastIndex(); cn++ {
					if rf.checkCanCommit(cn) {
						DPrintf("%v leader commit: %d %+v\n", rf.me, cn, *rf.Log[rf.indexIdx(cn)])
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
		DPrintf("*%v*: start retrun: %d %d %v\n", rf.me, index, term, isLeader)
	}()

	if rf.Role != LeaderRole {
		return index, term, isLeader
	}

	DPrintf("*%v*: start: %+v", rf.me, command)

	entry := &LogEntry{
		Index:   rf.getLastIndex() + 1,
		Term:    rf.CurrentTerm,
		Command: command,
	}

	rf.Log = append(rf.Log, entry)
	rf.MatchIndex[rf.me] = rf.getLastIndex()
	index = entry.Index

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
	close(rf.killed)
}

func (rf *Raft) setFlower() {
	DPrintf("%v from %v to %v\n", rf.me, rf.Role, FlowerRole)
	rf.VotedFor = -1
	rf.Role = FlowerRole
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
	rf.killed = make(chan struct{})
	// rf.Log = append(rf.Log, &LogEntry{Index: 0, Term: 0})
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.lastResponseTime = make([]time.Time, len(peers))
	rf.me = me
	rf.beFlowwer = make(chan bool, 1000)
	rf.heartBeat = make(chan bool, 1000)

	// Your initialization code here.
	rand.Seed(int64(me))

	rf.setFlower()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// DPrintf("my role: %v", rf.Role)

	go rf.loop()

	return rf
}

// 5.3 5.4
func (rf *Raft) checkCanCommit(index int) bool {
	if rf.getLastIndex() < index || rf.Log[rf.indexIdx(index)].Term != rf.CurrentTerm {
		return false
	}

	matchCount := 0
	for i := 0; i < len(rf.peers); i++ {
		if rf.MatchIndex[i] >= index {
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

	baseIndex := 0
	if len(rf.Log) > 0 {
		baseIndex = rf.Log[0].Index
	}
	// lastIndex := rf.getLastIndex()

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		var args AppendEntriesArgs = argsTPL
		if rf.Role != LeaderRole {
			var reply = new(AppendEntriesReply)
			go rf.sendAppendEntries(server, args, reply)
			continue
		}

		if rf.NextIndex[server] == 0 || rf.NextIndex[server] > rf.getLastIndex()+1 {
			rf.NextIndex[server] = rf.getLastIndex() + 1
		}

		// has snapshot && need snapshot
		if len(rf.Log) > 0 && rf.Log[0].Command == nil && baseIndex >= rf.NextIndex[server] {
			var reply = new(InstallSnapshotReply)
			var args InstallSnapshotArgs
			args.LastIncludeIndex = rf.Log[0].Index
			args.LastIncludeTerm = rf.Log[0].Term
			args.LeaderId = rf.me
			args.Term = rf.CurrentTerm
			args.Data = rf.persister.ReadSnapshot()
			go rf.sendSnapshot(server, args, reply)

		} else {
			entryNum := len(rf.Log) - rf.indexIdx(rf.NextIndex[server])
			if entryNum > MaxEntrysPerTime {
				entryNum = MaxEntrysPerTime
			}

			start := rf.indexIdx(rf.NextIndex[server])
			args.Entries = append(args.Entries, rf.Log[start:start+entryNum]...)
			args.PrevLogIndex = rf.NextIndex[server] - 1
			if args.PrevLogIndex < 0 {
				args.PrevLogIndex = 0
			}
			if args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf.Log[rf.indexIdx(args.PrevLogIndex)].Term
			}
			var reply = new(AppendEntriesReply)
			go rf.sendAppendEntries(server, args, reply)
		}

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

func (rf *Raft) String() string {
	tmp := *rf
	tmp.Log = nil
	s := fmt.Sprintf("%+v log len: %v", tmp, len(rf.Log))
	if len(rf.Log) > 0 {
		s += fmt.Sprintf("---%+v", *rf.lastLog())
	}
	return s
}

func (rf *Raft) loop() {

	// debug
	go func() {
		for {
			select {
			case <-time.Tick(time.Millisecond * 500):
				rf.mu.Lock()
				DPrintf("tick %+v --- %+v\n", rf.me, rf.String())
				rf.mu.Unlock()
			case <-rf.killed:
				return
			}
		}
	}()
	// end debug

	// apply log
	go func() {
		for {
			select {
			case <-rf.killed:
				return
			default:
			}

			rf.mu.Lock()
			if rf.CommitIndex == rf.LastAppllyed {
				rf.mu.Unlock()
				time.Sleep(time.Millisecond)
				continue
			}
			// if rf.indexIdx(rf.LastAppllyed) >= len(rf.Log)-1 || rf.indexIdx(rf.LastAppllyed) < 0 {
			// 	fmt.Println(rf.CommitIndex, rf.LastAppllyed, rf.indexIdx(rf.LastAppllyed), len(rf.Log))
			// }
			rf.LastAppllyed++
			msg := ApplyMsg{
				Index:   rf.LastAppllyed,
				Command: rf.Log[rf.indexIdx(rf.LastAppllyed)].Command,
				// Command: rf.Log[rf.LastAppllyed-1].Command,
			}
			DPrintf("*%v*: apply %v %+v", rf.me, rf.LastAppllyed, rf.Log[rf.indexIdx(rf.LastAppllyed)])
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			// rf.LastAppllyed++
			rf.mu.Unlock()
		}
	}()

	for {
		switch rf.Role {
		case LeaderRole:
			select {
			case <-rf.killed:
				return
			case <-rf.beFlowwer:
			default:
				rf.boardCaseAppendEntries()
				time.Sleep(PingPeerPeriod)

				rf.mu.Lock()
				if rf.CommitIndex < rf.getLastIndex() && rf.getLastLogTerm() != rf.CurrentTerm && rf.LeaderLastCommitTime.Add(ElectionTimeout*3).Before(time.Now()) {
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
			beLeader := make(chan struct{})
			rf.CurrentTerm++
			rf.persist()
			rf.mu.Unlock()
			go rf.boardCatRequestVote(beLeader)
			timeout := ElectionTimeout
			select {
			case <-rf.killed:
				return
			case <-beLeader:
				rf.mu.Lock()
				DPrintf("%v be leader\n", rf.me)
				rf.Role = LeaderRole
				rf.MatchIndex = make([]int, len(rf.peers))
				rf.NextIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.NextIndex); i++ {
					rf.NextIndex[i] = rf.getLastIndex() + 1
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
			timeout := ElectionTimeout + time.Duration(rand.Intn(int(ElectionTimeout*1)+22))
			select {
			case <-rf.killed:
				return
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

func (rf *Raft) boardCatRequestVote(beLeader chan struct{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var args RequestVoteArgs
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	args.Term = rf.CurrentTerm

	for i := 0; i < len(rf.peers); i++ {
		var resp = new(RequestVoteReply)
		go rf.sendRequestVote(i, args, resp, beLeader)
	}
}
