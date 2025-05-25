package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"k8s.io/utils/ptr"
)

const (
	Follower = iota
	Candidate
	Leader
)

func State(state int) (string, bool) {
	switch state {
	case Follower:
		return "Follower", true
	case Candidate:
		return "Candidate", true
	case Leader:
		return "Leader", true
	default:
		return "", false
	}
}

// RaftNode holds the state for our Raft implementation.
type RaftNode struct {
	n      *maelstrom.Node
	mu     sync.Mutex
	logger *slog.Logger

	state        int
	stateMachine map[int]int

	currentTerm int
	votedFor    *string
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term int
	Op   interface{}
}

// NewRaftNode creates a new RaftNode.
func NewRaftNode() *RaftNode {
	maelstromNode := maelstrom.NewNode()

	rn := &RaftNode{
		n:            maelstromNode,
		logger:       slog.Default(),
		state:        Follower,
		stateMachine: make(map[int]int),
		logs: []LogEntry{
			{
				Term: 0,
				Op:   nil,
			},
		},
	}
	return rn
}

func (rn *RaftNode) becomeFollower() {
	rn.mu.Lock()
	rn.state = Follower
	rn.mu.Unlock()
	rn.logger.Info(fmt.Sprintf("node %s became follower for term %d", rn.n.ID(), rn.currentTerm))
}

func (rn *RaftNode) becomeCandidate() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.mu.Unlock()

	rn.advanceTerm(rn.currentTerm + 1)
	rn.requestVotes()
	rn.logger.Info(fmt.Sprintf("node %s became candidate for term %d", rn.n.ID(), rn.currentTerm))
}

func (rn *RaftNode) becomeLeader() {
	if rn.state != Candidate {
		rn.logger.Info(fmt.Sprintf("node %s cannot become leader if it's not a candidate", rn.n.ID()))
		return
	}
	rn.mu.Lock()
	rn.state = Leader
	rn.mu.Unlock()
	rn.logger.Info(fmt.Sprintf("node %s became leader for term %d", rn.n.ID(), rn.currentTerm))
}

type AppendEntriesArgs struct {
	maelstrom.MessageBody

	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	maelstrom.MessageBody

	Term    int  `json:"term"`
	Success bool `json:"success"`
}

func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

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
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	maelstrom.MessageBody

	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

func (rn *RaftNode) RequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	granted := false
	currentLastLogIndex := len(rn.logs) - 1
	currentLastLogTerm := rn.logs[currentLastLogIndex].Term
	rn.stepDown(args.Term)

	if args.Term < rn.currentTerm {
		rn.logger.Info(fmt.Sprintf("candidate term %d lower than ours %d, not granting vote!", args.Term, rn.currentTerm))
	} else if rn.votedFor != nil {
		rn.logger.Info(fmt.Sprintf("already voted for %s, not granting vote!", *rn.votedFor))
	} else if args.LastLogTerm < currentLastLogTerm {
		rn.logger.Info(fmt.Sprintf("have log entries from term %d, which is newer than %d, not granting vote!", currentLastLogTerm, args.LastLogTerm))
	} else if args.LastLogTerm == currentLastLogTerm && args.LastLogIndex < currentLastLogIndex {
		rn.logger.Info(fmt.Sprintf("both logs at term %d, but our logs size %d, theirs %d, not granting vote!", currentLastLogTerm, args.LastLogIndex, currentLastLogIndex))
	} else {
		rn.logger.Info(fmt.Sprintf("granting vote to %s!", args.CandidateId))
		granted = true
		rn.mu.Lock()
		rn.votedFor = &args.CandidateId
		rn.mu.Unlock()
	}

	reply := &RequestVoteReply{
		MessageBody: maelstrom.MessageBody{
			Type: "request_vote_ok",
		},
		Term:        rn.currentTerm,
		VoteGranted: granted,
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

func (rn *RaftNode) handleEcho(msg maelstrom.Message) error {
	var args map[string]any
	if err := json.Unmarshal(msg.Body, &args); err != nil {
		return err
	}

	args["type"] = "echo_ok"
	return rn.n.Reply(msg, args)
}

func (rn *RaftNode) ElectionTimeout() time.Duration {
	lo := 150
	hi := 300
	duration := rand.Intn(int(hi)-int(lo)) + int(lo)
	return time.Duration(duration) * time.Millisecond
}

func (rn *RaftNode) StepDownTimeout() time.Duration {
	return 2 * time.Second
}

func (rn *RaftNode) advanceTerm(term int) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if term < rn.currentTerm {
		return fmt.Errorf("term cannot go backwards: from %d, to %d", rn.currentTerm, term)
	}
	rn.currentTerm = term
	rn.votedFor = nil
	return nil
}

func (rn *RaftNode) runEventLoop() {
	go func() {
		rn.n.Serve()
	}()

	for {
		select {
		case msg, ok := <-rn.n.MessageChannel():
			if !ok {
				rn.logger.Info("the chan was closed", "node_id", rn.n.ID())
				return
			}
			if err := rn.n.ProcessMessage(msg); err != nil {
				rn.logger.Error(fmt.Sprintf("error processing msg %s", msg.Type()))
			}

		case <-time.After(rn.ElectionTimeout()):
			rn.logger.Info("election timeout expired", "node_id", rn.n.ID())
			if rn.state != Leader {
				rn.becomeCandidate()
			}
		case <-time.After(rn.StepDownTimeout()):
			rn.logger.Info("step down timeout expired", "node_id", rn.n.ID())
			if rn.state == Leader {
				rn.becomeFollower()
			}
		}
	}
}

func (rn *RaftNode) broadcast(body any, h maelstrom.HandlerFunc) {
	for _, id := range rn.n.NodeIDs() {
		if id == rn.n.ID() {
			continue
		}
		rn.n.RPC(id, body, h)
	}
}

func (rn *RaftNode) stepDown(remoteTerm int) bool {
	if rn.currentTerm < remoteTerm {
		rn.logger.Info(fmt.Sprintf("stepping down: remote term %d bigger than current term %d", remoteTerm, rn.currentTerm))
		rn.advanceTerm(remoteTerm)
		rn.becomeFollower()
		return true
	}
	return false
}

func (rn *RaftNode) requestVotes() {
	votes := make(map[string]struct{})
	votes[rn.n.ID()] = struct{}{}

	rn.mu.Lock()
	rn.votedFor = ptr.To(rn.n.ID())
	rn.mu.Unlock()

	body := &RequestVoteArgs{
		MessageBody: maelstrom.MessageBody{
			Type: "request_vote",
		},
		Term:         rn.currentTerm,
		CandidateId:  rn.n.ID(),
		LastLogIndex: len(rn.logs) - 1,
		LastLogTerm:  rn.logs[len(rn.logs)-1].Term,
	}
	rn.broadcast(body, func(msg maelstrom.Message) error {
		reply := &RequestVoteReply{}
		if err := json.Unmarshal(msg.Body, reply); err != nil {
			return err
		}
		if ok := rn.stepDown(reply.Term); ok {
			return nil
		}
		if reply.VoteGranted {
			rn.mu.Lock()
			votes[msg.Src] = struct{}{}
			rn.mu.Unlock()
			rn.logger.Info(fmt.Sprintf("have votes: %v", votes))
		}
		if rn.haveMajority(len(votes)) {
			rn.becomeLeader()
		}
		return nil
	})
}

func (rn *RaftNode) haveMajority(numVotes int) bool {
	return numVotes > len(rn.n.NodeIDs())/2
}

func main() {
	rn := NewRaftNode()

	rn.n.Handle("request_vote", rn.handleRequestVote)
	rn.n.Handle("append_entries", rn.handleAppendEntries)
	rn.n.Handle("read", rn.handleKVRead)
	rn.n.Handle("write", rn.handleKVWrite)
	rn.n.Handle("cas", rn.handleKVCas)
	rn.n.Handle("echo", rn.handleEcho)

	rn.runEventLoop()
}
