package shardkv

// import "shardmaster"
import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"strconv"
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

const (
	PutAppendOP = iota
	GetOP
	ConfigOp
	PreConfigOp
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind          int
	Key           string
	Value         string
	Op            string
	Config        shardmaster.Config
	GetShardReply GetShardReply

	//
	CID int64
	RID int64

	ApplyCode int
}

const WrongGruopCode = 1

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// need snapshoot
	db     map[string]string
	dup    map[int64]int64
	config []shardmaster.Config

	result map[int]chan Op

	mck *shardmaster.Clerk
}

func (kv *ShardKV) isDup(cid int64, rid int64) bool {
	if cid == 0 {
		return false
	}

	v, ok := kv.dup[cid]

	if !ok {
		return false
	}

	return v >= rid
}

func (kv *ShardKV) checkGroup(k string) bool {
	shard := key2shard(k)

	if len(kv.config) == 0 {
		return false
	}

	c := kv.config[len(kv.config)-1]
	return c.Shards[shard] == kv.gid
}

func (kv *ShardKV) applyOp(op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if op.Kind == PutAppendOP || op.Kind == GetOP {
		if kv.checkGroup(op.Key) == false {
			op.ApplyCode = WrongGruopCode
			return
		}
	}

	switch op.Kind {
	case PutAppendOP:
		if op.Op == "Put" {
			kv.db[op.Key] = op.Value
		} else if op.Op == "Append" {
			kv.db[op.Key] += op.Value
		} else {
			panic(op.Op)
		}
	case GetOP:
	case ConfigOp:
		if op.Config.Num != len(kv.config) {
			break
		}

		for k, v := range op.GetShardReply.DB {
			kv.db[k] = v
		}
		for k, v := range op.GetShardReply.Dup {
			if v > kv.dup[k] {
				kv.dup[k] = v
			}
		}
		kv.config = append(kv.config, op.Config)
	case PreConfigOp:
	}

	kv.dup[op.CID] = op.RID
}

func (kv *ShardKV) handleApply() {
	for msg := range kv.applyCh {
		DPrintf("-%v- handle apply: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), msg)
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
			d.Decode(&kv.config)

			kv.mu.Unlock()
		} else {
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}
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

			if kv.maxraftstate != -1 && kv.rf.GetPersistSize() > kv.maxraftstate {
				buffer := new(bytes.Buffer)
				e := gob.NewEncoder(buffer)
				e.Encode(kv.db)
				e.Encode(kv.dup)
				e.Encode(kv.config)

				go kv.rf.StartSnapshot(buffer.Bytes(), msg.Index)
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) appendOp(op Op) Err {
	kv.mu.Lock()
	if op.CID == 0 {
		panic("0 cid")
	}

	if op.Kind != ConfigOp && op.Kind != PreConfigOp {
		if kv.checkGroup(op.Key) == false {
			kv.mu.Unlock()
			return ErrWrongGroup
		}
	}

	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		kv.mu.Unlock()
		return ErrWrongLeader
	}

	resultChan, ok := kv.result[index]
	if !ok {
		resultChan = make(chan Op, 1)
		kv.result[index] = resultChan
	}
	kv.mu.Unlock()

	select {
	case respOp := <-resultChan:
		// you may think you are leader, but not, the return index may apply a log from the true leader
		if respOp.RID == op.RID && respOp.CID == op.CID && respOp.ApplyCode == 0 {
			return OK
		} else if respOp.ApplyCode == WrongGruopCode {
			return ErrWrongGroup
		} else {
			return ErrWrongLeader
		}
	case <-time.NewTimer(time.Second).C:
		return ErrWrongLeader
	}

	return ErrWrongLeader
}

// delete老的数据可以让对方通知后走rafe log 删除没用了的shard数据
func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	DPrintf("-%v- GetShard: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), *args)
	defer func() {
		DPrintf("-%v- GetShard resp: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), *reply)
	}()

	kv.mu.Lock()
	if len(kv.config)-1 < args.Config.Num {
		kv.mu.Unlock()
		reply.Err = ErrNotReady
		return
	}
	kv.mu.Unlock()

	op := Op{
		CID:    nrand(),
		Kind:   PreConfigOp,
		Config: args.Config,
	}

	err := kv.appendOp(op)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.DB = make(map[string]string)
	reply.Dup = make(map[int64]int64)
	for k, v := range kv.db {
		if key2shard(k) == args.Shard {
			reply.DB[k] = v
		}
	}
	for k, v := range kv.dup {
		reply.Dup[k] = v
	}

	reply.Err = OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("-%v- Get: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), *args)
	defer func() {
		DPrintf("-%v- Get resp: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), *reply)
	}()

	op := Op{
		Kind: GetOP,
		Key:  args.Key,
		CID:  args.CID,
		RID:  args.RID,
	}

	err := kv.appendOp(op)

	if err != OK {
		reply.WrongLeader = (err == ErrWrongLeader)
		reply.Err = err
		return
	}

	kv.mu.Lock()
	var hasKey bool
	reply.Value, hasKey = kv.db[args.Key]
	kv.mu.Unlock()
	if hasKey == false {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}

	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("-%v- PutAppend: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), *args)
	defer func() {
		DPrintf("-%v- PutAppend resp: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), *reply)
	}()

	op := Op{
		Kind:  PutAppendOP,
		Key:   args.Key,
		Value: args.Value,
		Op:    args.Op,
		CID:   args.CID,
		RID:   args.RID,
	}

	err := kv.appendOp(op)

	if err != OK {
		reply.WrongLeader = (err == ErrWrongLeader)
		reply.Err = err
		return
	}

	reply.Err = OK

	return
}

func (kv *ShardKV) String() string {
	kv.mu.Lock()
	tmp := *kv
	defer kv.mu.Unlock()
	var s string
	s += fmt.Sprintf("config len: %v\n", len(kv.config))
	s += fmt.Sprintf("+%+v\n", tmp)

	return s
}

func (kv *ShardKV) tryReconfig(now shardmaster.Config) bool {
	DPrintf("-%v- tryReconfig: %+v len: %v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), now, len(kv.config))
	defer func() {
		DPrintf("-%v- tryReconfig  end: ", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me))
	}()

	kv.mu.Lock()

	if now.Num != len(kv.config) {
		kv.mu.Unlock()
		return false
	}

	var resp GetShardReply
	if len(kv.config) > 0 {
		last := kv.config[len(kv.config)-1]
		kv.mu.Unlock()
		for s := 0; s < shardmaster.NShards; s++ {
			if now.Shards[s] == kv.gid && (last.Shards[s] != kv.gid && last.Shards[s] != 0) {
				names := last.Groups[last.Shards[s]]
				getShardOK := false
				for _, name := range names {
					srv := kv.make_end(name)
					var args GetShardArgs
					args.Config = now
					args.Shard = s

					var reply GetShardReply
					// 有可能卡住？
					ok := srv.Call("ShardKV.GetShard", &args, &reply)
					if ok && reply.Err == OK {
						resp.Merge(&reply)
						getShardOK = true
						DPrintf("-%v- get shard ok: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), s)
						break
					} else {
						DPrintf("-%v- get shard not ok: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), s)
					}
				}
				if !getShardOK {
					return false
				}
			}
		}
	} else {
		kv.mu.Unlock()
	}

	op := Op{
		CID:           nrand(),
		Kind:          ConfigOp,
		Config:        now,
		GetShardReply: resp,
	}

	err := kv.appendOp(op)

	if err != OK {
		DPrintf("-%v- tryReconfig err: %+v %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), err, now)
		return false
	} else {
		DPrintf("-%v- tryReconfig ok: %+v %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), err, now)
	}

	return true
}

func (kv *ShardKV) fetchConfig() {
	nextNum := len(kv.config)
	for _ = range time.Tick(time.Millisecond * 101) {
		kv.mu.Lock()
		if len(kv.config) > nextNum {
			nextNum = len(kv.config)
		}
		kv.mu.Unlock()
		c := kv.mck.Query(nextNum)
		if c.Num != nextNum {
			time.Sleep(time.Second)
			continue
		}

		DPrintf("-%v- get new config: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), c)
		for {
			kv.mu.Lock()
			if len(kv.config) > c.Num {
				nextNum++
				kv.mu.Unlock()
				break
			}
			kv.mu.Unlock()

			ok := kv.tryReconfig(c)
			if !ok {
				time.Sleep(time.Second)
			} else {
				nextNum++
				break
			}
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.dup = make(map[int64]int64)
	kv.result = make(map[int]chan Op)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.handleApply()
	go kv.fetchConfig()
	go kv.printInfo()

	return kv
}

func (kv *ShardKV) printInfo() {
	for _ = range time.Tick(time.Second * 2) {
		DPrintf("-%v- info: %+v", strconv.Itoa(kv.gid)+"-"+strconv.Itoa(kv.me), kv.String())
	}
}
