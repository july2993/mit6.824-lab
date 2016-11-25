package shardmaster

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		s := fmt.Sprintf(format, a...)
		log.Output(2, s)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	db     map[string]string
	dup    map[int64]int64
	dbMu   sync.RWMutex
	result map[int]chan Op

	configs []Config // indexed by config num
}

const (
	JoinOp = iota
	LeaveOp
	MoveOp
	QueryOp
)

func (kv *ShardMaster) isDup(cid int64, rid int64) bool {
	v, ok := kv.dup[cid]

	if !ok {
		return false
	}

	return v >= rid
}

func (sm *ShardMaster) copyLastConfig() (config Config) {
	last := sm.configs[len(sm.configs)-1]
	config = last
	config.Groups = make(map[int][]string)
	for k, v := range last.Groups {
		config.Groups[k] = v
	}
	config.Num = last.Num + 1

	return
}

func (sm *ShardMaster) getLeastShareGroup(c *Config) (int, int) {
	count := make(map[int]int)
	for g, _ := range c.Groups {
		count[g] = 0
	}
	for i := 0; i < len(c.Shards); i++ {
		if c.Shards[i] == 0 {
			continue
		}

		if _, ok := c.Groups[c.Shards[i]]; ok == false {
			continue
		}

		count[c.Shards[i]] += 1
	}

	minGroup := 0
	min := 1000000000
	for group, c := range count {
		if c < min {
			minGroup = group
			min = c
		}
	}

	return minGroup, min
}

func (sm *ShardMaster) getMaxShareGroup(c *Config) (int, int) {
	count := make(map[int]int)
	for g, _ := range c.Groups {
		count[g] = 0
	}
	for i := 0; i < len(c.Shards); i++ {
		if c.Shards[i] == 0 {
			continue
		}

		if _, ok := c.Groups[c.Shards[i]]; ok == false {
			continue
		}

		count[c.Shards[i]] += 1
	}

	maxGroup := 0
	max := -1
	for group, c := range count {
		if c > max {
			maxGroup = group
			max = c
		}
	}

	return maxGroup, max
}

func (sm *ShardMaster) makeGroupEven(c *Config) {
	if len(c.Groups) == 0 {
		return
	}

	for i := 0; i < len(c.Shards); i++ {
		_, ok := c.Groups[c.Shards[i]]
		if c.Shards[i] == 0 || !ok {
			g, _ := sm.getLeastShareGroup(c)
			c.Shards[i] = g
		}
	}

	for {
		minG, minV := sm.getLeastShareGroup(c)
		maxG, maxV := sm.getMaxShareGroup(c)
		// DPrintf("-%v- %v %v %v %v", sm.me, minG, minV, maxG, maxV)
		if minV+1 >= maxV {
			return
		}

		for i := 0; i < len(c.Shards); i++ {
			if c.Shards[i] == maxG {
				c.Shards[i] = minG
				break
			}
		}
	}
}

func (sm *ShardMaster) configString() string {
	var s string
	for _, c := range sm.configs {
		s += fmt.Sprintf("%+v\n", c)
	}
	return s
}

func (kv *ShardMaster) applyOp(op *Op) {
	kv.mu.Lock()
	DPrintf("-%v- apply: %+v", kv.me, *op)
	defer func() {
		DPrintf("-%v- %v", kv.me, kv.configString())
	}()

	switch op.Kind {
	case JoinOp:
		c := kv.copyLastConfig()
		for k, v := range op.Servers {
			c.Groups[k] = v
		}

		kv.makeGroupEven(&c)

		kv.configs = append(kv.configs, c)
	case LeaveOp:
		c := kv.copyLastConfig()
		for _, gid := range op.GIDs {
			delete(c.Groups, gid)
		}

		kv.makeGroupEven(&c)

		kv.configs = append(kv.configs, c)
	case MoveOp:
		c := kv.copyLastConfig()
		c.Shards[op.Shard] = op.GID

		kv.configs = append(kv.configs, c)
	case QueryOp:
	default:
		panic(op.Kind)

	}

	kv.mu.Unlock()

	kv.dup[op.CID] = op.RID
}

func (kv *ShardMaster) handleApply() {
	for msg := range kv.applyCh {
		if msg.UseSnapshot {
			panic("no snapshot")
		} else {
			op := msg.Command.(Op)
			// DPrintf("-%v- get apply: %+v", kv.me, op)
			if !kv.isDup(op.CID, op.RID) {
				kv.applyOp(&op)
			}

			kv.mu.Lock()
			opChan, ok := kv.result[msg.Index]

			if ok {
				select {
				case <-opChan:
				default:
				}

				opChan <- op
			} else {
				opChan = make(chan Op, 1)
				opChan <- op
				kv.result[msg.Index] = opChan
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *ShardMaster) appendOp(op Op) bool {
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
		return respOp.CID == op.CID && respOp.RID == op.RID
	case <-time.NewTimer(time.Second).C:
		return false
	}

	return false
}

type Op struct {
	// Your data here.
	Kind    int
	CID     int64
	RID     int64
	Servers map[int][]string
	GIDs    []int
	GID     int
	Shard   int
	Num     int
}

var ErrWrongLeader Err = "wrong leader"

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	DPrintf("-%v- req: %+v", sm.me, *args)
	defer func() {
		DPrintf("-%v- resp: %+v", sm.me, *reply)
	}()

	op := Op{
		Kind:    JoinOp,
		CID:     args.CID,
		RID:     args.RID,
		Servers: args.Servers,
	}

	ok := sm.appendOp(op)

	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	return
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("-%v- req: %+v", sm.me, *args)
	defer func() {
		DPrintf("-%v- resp: %+v", sm.me, *reply)
	}()

	op := Op{
		Kind: LeaveOp,
		CID:  args.CID,
		RID:  args.RID,
		GIDs: args.GIDs,
	}
	ok := sm.appendOp(op)

	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("-%v- req: %+v", sm.me, *args)
	defer func() {
		DPrintf("-%v- resp: %+v", sm.me, *reply)
	}()

	op := Op{
		Kind:  MoveOp,
		CID:   args.CID,
		RID:   args.RID,
		GID:   args.GID,
		Shard: args.Shard,
	}
	ok := sm.appendOp(op)

	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	return
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("-%v- req: %+v", sm.me, *args)
	defer func() {
		DPrintf("-%v- resp: %+v", sm.me, *reply)
	}()

	op := Op{
		Kind: QueryOp,
		CID:  args.CID,
		RID:  args.RID,
		Num:  args.Num,
	}
	ok := sm.appendOp(op)

	if !ok {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	sm.mu.Lock()
	idx := args.Num
	if idx < 0 || idx >= len(sm.configs) {
		idx = len(sm.configs) - 1
	}
	reply.Config = sm.configs[idx]
	sm.mu.Unlock()

	return
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.applyCh = make(chan raft.ApplyMsg, 1000)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.dup = make(map[int64]int64)
	sm.result = make(map[int]chan Op)

	go sm.handleApply()

	return sm
}
