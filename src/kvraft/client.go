package raftkv

import (
	"crypto/rand"
	"labrpc"
	"time"
)

import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	cid    int64
	rid    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.cid = nrand()
	ck.rid = 1
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.rid++

	var args = &GetArgs{
		CID: ck.cid,
		RID: ck.rid,
		Key: key,
	}

	for {
		var resp = new(GetReply)
		ok := ck.servers[ck.leader].Call("RaftKV.Get", args, resp)
		if ok == false || resp.WrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(time.Millisecond)
			continue
		}

		return resp.Value
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.rid++
	// You will have to modify this function.
	var args = &PutAppendArgs{
		CID:   ck.cid,
		RID:   ck.rid,
		Key:   key,
		Value: value,
		Op:    op,
	}
	for {
		var resp = new(PutAppendReply)
		ok := ck.servers[ck.leader].Call("RaftKV.PutAppend", args, resp)
		DPrintf("call: %v %+v %+v", ok, *args, *resp)
		if ok == false || resp.WrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(time.Millisecond)
			continue
		}

		return
	}

	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
