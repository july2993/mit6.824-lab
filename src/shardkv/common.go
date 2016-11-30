package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "Wrongleader"
	ErrNotReady    = "NotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CID int64
	RID int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CID int64
	RID int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type GetShardArgs struct {
	Shard  int
	Config shardmaster.Config
}

type GetShardReply struct {
	Err Err
	DB  map[string]string
	Dup map[int64]int64
}

func (r *GetShardReply) Merge(other *GetShardReply) {
	for k, v := range other.DB {
		if r.DB == nil {
			r.DB = make(map[string]string)
		}
		r.DB[k] = v
	}

	for k, v := range other.Dup {
		if v > r.Dup[k] {
			if r.Dup == nil {
				r.Dup = make(map[int64]int64)
			}
			r.Dup[k] = v
		}
	}
}
