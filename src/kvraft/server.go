package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	PutAppendOP = iota
	GetOP
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind  int
	Key   string
	Value string
	Op    string

	//
	CID int64
	RID int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	db     map[string]string
	dup    map[int64]int64
	dbMu   sync.RWMutex
	result map[int]chan Op

	// Your definitions here.
}

func (kv *RaftKV) isDup(cid int64, rid int64) bool {
	v, ok := kv.dup[cid]

	if !ok {
		return false
	}

	return v >= rid
}

func (kv *RaftKV) applyOp(op *Op) {
	switch op.Kind {
	case PutAppendOP:
		kv.dbMu.Lock()
		if op.Op == "Put" {
			kv.db[op.Key] = op.Value
		} else if op.Op == "Append" {
			kv.db[op.Key] += op.Value
		} else {
			panic(op.Op)
		}
		kv.dbMu.Unlock()
	case GetOP:
	}

	kv.dup[op.CID] = op.RID
}

func (kv *RaftKV) handleApply() {
	for msg := range kv.applyCh {
		if msg.UseSnapshot {
			buffer := bytes.NewBuffer(msg.Snapshot)
			d := gob.NewDecoder(buffer)
			var index int
			var term int

			kv.mu.Lock()
			d.Decode(&index)
			d.Decode(&term)
			kv.db = make(map[string]string)
			kv.dup = make(map[int64]int64)
			d.Decode(&kv.db)
			d.Decode(&kv.dup)
			kv.mu.Unlock()
		} else {
			op := msg.Command.(Op)
			if !kv.isDup(op.CID, op.RID) {
				kv.applyOp(&op)
			}

			kv.mu.Lock()
			opChan, ok := kv.result[msg.Index]

			if ok {
				// select {
				// case <-opChan:
				// default:
				// }

				opChan <- op
			} else {
				opChan = make(chan Op, 1)
				opChan <- op
				kv.result[msg.Index] = opChan
			}

			if kv.maxraftstate != -1 && kv.rf.GetPersistSize() > kv.maxraftstate {
				buffer := new(bytes.Buffer)
				e := gob.NewEncoder(buffer)
				e.Encode(kv.db)
				e.Encode(kv.dup)
				go kv.rf.StartSnapshot(buffer.Bytes(), msg.Index)
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *RaftKV) appendOp(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		return false
	}

	kv.mu.Lock()
	resultChan, ok := kv.result[index]
	if !ok {
		resultChan = make(chan Op, 1)
		kv.result[index] = resultChan
	}
	kv.mu.Unlock()

	select {
	case respOp := <-resultChan:
		// you may think you are leader, but not, the return index may apply a log from the true leader
		return respOp == op
	case <-time.NewTimer(time.Second).C:
		return false
	}

	return false
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Kind: GetOP,
		Key:  args.Key,
		CID:  args.CID,
		RID:  args.RID,
	}

	ok := kv.appendOp(op)

	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	kv.dbMu.RLock()
	reply.Value = kv.db[args.Key]
	kv.dbMu.RUnlock()

	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Kind:  PutAppendOP,
		Key:   args.Key,
		Value: args.Value,
		Op:    args.Op,
		CID:   args.CID,
		RID:   args.RID,
	}

	ok := kv.appendOp(op)

	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.dup = make(map[int64]int64)
	kv.result = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.handleApply()

	return kv
}
