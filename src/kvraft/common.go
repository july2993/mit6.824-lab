package raftkv

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "wrong leader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	CID   int64
	RID   int64
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	RID int64
	Key string
	CID int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
