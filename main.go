package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// RaftNode holds the state for our Raft implementation.
type RaftNode struct {
	n  *maelstrom.Node
	mu sync.Mutex

	stateMachine map[int]int

	currentTerm int
	votedFor    *int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// NewRaftNode creates a new RaftNode.
func NewRaftNode() *RaftNode {
	maelstromNode := maelstrom.NewNode()

	rn := &RaftNode{
		n:            maelstromNode,
		stateMachine: make(map[int]int),
	}
	return rn
}

// TODO
type LogEntry struct {
}

type AppendEntriesArgs struct {
	maelstrom.MessageBody

	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	maelstrom.MessageBody

	Term int `json:"term"`
}

func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply := &AppendEntriesReply{
		MessageBody: maelstrom.MessageBody{
			Type: "append_entries_ok",
		},
	}
	return reply, nil
}

type RequestVoteArgs struct {
	maelstrom.MessageBody

	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	maelstrom.MessageBody

	Term int `json:"term"`
}

func (rn *RaftNode) RequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{
		MessageBody: maelstrom.MessageBody{
			Type: "request_vote_ok",
		},
	}
	return reply, nil
}

type KVReadArgs struct {
	maelstrom.MessageBody

	Key int
}

type KVReadReply struct {
	maelstrom.MessageBody

	Value int `json:"value"`
}

func (rn *RaftNode) KVRead(args *KVReadArgs) (*KVReadReply, error) {
	val, ok := rn.stateMachine[args.Key]
	if !ok {
		return nil, &maelstrom.RPCError{
			Code: maelstrom.KeyDoesNotExist,
			Text: maelstrom.ErrorCodeText(maelstrom.KeyDoesNotExist),
		}
	}

	reply := &KVReadReply{
		Value: val,
		MessageBody: maelstrom.MessageBody{
			Type: "read_ok",
		},
	}
	return reply, nil
}

type KVWriteArgs struct {
	maelstrom.MessageBody

	Key   int
	Value int
}

type KVWriteReply struct {
	maelstrom.MessageBody
}

func (rn *RaftNode) KVWrite(args *KVWriteArgs) (*KVWriteReply, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.stateMachine[args.Key] = args.Value

	reply := &KVWriteReply{
		MessageBody: maelstrom.MessageBody{
			Type: "write_ok",
		},
	}
	return reply, nil
}

type KVCasArgs struct {
	maelstrom.MessageBody

	Key  int
	From int
	To   int
}

type KVCasReply struct {
	maelstrom.MessageBody
}

func (rn *RaftNode) KVCas(args *KVCasArgs) (*KVCasReply, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	val, ok := rn.stateMachine[args.Key]
	if !ok {
		return nil, &maelstrom.RPCError{
			Code: maelstrom.KeyDoesNotExist,
			Text: maelstrom.ErrorCodeText(maelstrom.KeyDoesNotExist),
		}
	}
	if val != args.From {
		return nil, &maelstrom.RPCError{
			Code: maelstrom.PreconditionFailed,
			Text: maelstrom.ErrorCodeText(maelstrom.PreconditionFailed),
		}
	}
	rn.stateMachine[args.Key] = args.To

	reply := &KVCasReply{
		MessageBody: maelstrom.MessageBody{
			Type: "cas_ok",
		},
	}
	return reply, nil
}

func main() {
	rn := NewRaftNode()

	rn.n.Handle("request_vote", rn.handleRequestVote)
	rn.n.Handle("append_entries", rn.handleAppendEntries)

	rn.n.Handle("read", rn.handleKVRead)
	rn.n.Handle("write", rn.handleKVWrite)
	rn.n.Handle("cas", rn.handleKVCas)

	if err := rn.n.Run(); err != nil {
		log.Fatalf("Node %s: error running Maelstrom node: %s", rn.n.ID(), err)
	}
}

func (rn *RaftNode) handleRequestVote(msg maelstrom.Message) error {
	args := &RequestVoteArgs{}
	if err := json.Unmarshal(msg.Body, args); err != nil {
		return err
	}

	reply, err := rn.RequestVote(args)
	if err != nil {
		return err
	}
	return rn.n.Reply(msg, reply)
}

func (rn *RaftNode) handleAppendEntries(msg maelstrom.Message) error {
	args := &AppendEntriesArgs{}
	if err := json.Unmarshal(msg.Body, args); err != nil {
		return err
	}

	reply, err := rn.AppendEntries(args)
	if err != nil {
		return err
	}
	return rn.n.Reply(msg, reply)
}

func (rn *RaftNode) handleKVRead(msg maelstrom.Message) error {
	args := &KVReadArgs{}
	if err := json.Unmarshal(msg.Body, args); err != nil {
		return err
	}

	reply, err := rn.KVRead(args)
	if err != nil {
		return err
	}
	return rn.n.Reply(msg, reply)
}

func (rn *RaftNode) handleKVWrite(msg maelstrom.Message) error {
	args := &KVWriteArgs{}
	if err := json.Unmarshal(msg.Body, args); err != nil {
		return err
	}

	reply, err := rn.KVWrite(args)
	if err != nil {
		return err
	}
	return rn.n.Reply(msg, reply)
}

func (rn *RaftNode) handleKVCas(msg maelstrom.Message) error {
	args := &KVCasArgs{}
	if err := json.Unmarshal(msg.Body, args); err != nil {
		return err
	}

	reply, err := rn.KVCas(args)
	if err != nil {
		return err
	}
	return rn.n.Reply(msg, reply)
}
