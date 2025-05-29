package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
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

const (
	ElectionTimeoutLower = 300 * time.Millisecond
	ElectionTimeoutUpper = 500 * time.Millisecond

	HeartbeatTimeout = 150 * time.Millisecond
	StepDownTimeout  = 2000 * time.Millisecond
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
	leaderId    *string
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  map[string]int
	matchIndex map[string]int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	stepDownTimer  *time.Timer

	// Track pending client requests
	pendingRequests map[int]*maelstrom.Message // map[logIndex]clientMessage
}

type LogEntry struct {
	Term int
	Op   interface{}
}

// Operation types
type KVWriteOp struct {
	Key   int
	Value int
}

type KVCasOp struct {
	Key  int
	From int
	To   int
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
		nextIndex:       make(map[string]int),
		matchIndex:      make(map[string]int),
		lastApplied:     0,
		commitIndex:     0,
		pendingRequests: make(map[int]*maelstrom.Message),
	}

	return rn
}

func (rn *RaftNode) becomeFollower() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.state = Follower
	rn.matchIndex = make(map[string]int)
	rn.nextIndex = make(map[string]int)
	rn.leaderId = nil
	// Clear pending requests when losing leadership
	rn.pendingRequests = make(map[int]*maelstrom.Message)
	rn.resetTimersLocked()

	rn.logger.Info(fmt.Sprintf("node %s became follower for term %d", rn.n.ID(), rn.currentTerm))
}

func (rn *RaftNode) becomeCandidate() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.leaderId = nil
	rn.currentTerm++
	rn.votedFor = ptr.To(rn.n.ID())
	rn.resetTimersLocked()
	rn.mu.Unlock()

	rn.logger.Info(fmt.Sprintf("node %s became candidate for term %d", rn.n.ID(), rn.currentTerm))
	go rn.requestVotes()
}

func (rn *RaftNode) becomeLeader() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Candidate {
		rn.logger.Info(fmt.Sprintf("node %s cannot become leader if it's not a candidate", rn.n.ID()))
		return
	}

	rn.state = Leader
	rn.leaderId = nil

	// Initialize nextIndex and matchIndex
	for _, node := range rn.n.NodeIDs() {
		if node == rn.n.ID() {
			continue
		}
		rn.nextIndex[node] = len(rn.logs)
		rn.matchIndex[node] = 0
	}

	// Consider self when calculating commit index
	rn.matchIndex[rn.n.ID()] = len(rn.logs) - 1

	rn.resetTimersLocked()
	rn.logger.Info(fmt.Sprintf("node %s became leader for term %d", rn.n.ID(), rn.currentTerm))

	// Send initial heartbeat
	go rn.heartbeat()
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

	// Step down if necessary
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.votedFor = nil
		if rn.state != Follower {
			rn.state = Follower
			rn.matchIndex = make(map[string]int)
			rn.nextIndex = make(map[string]int)
			rn.pendingRequests = make(map[int]*maelstrom.Message)
		}
	}

	reply := &AppendEntriesReply{
		MessageBody: maelstrom.MessageBody{
			Type: "append_entries_ok",
		},
		Success: false,
		Term:    rn.currentTerm,
	}

	if args.Term < rn.currentTerm {
		rn.logger.Info("not accepting entries from leaders behind", "remote_term", args.Term, "current_term", rn.currentTerm)
		return reply, nil
	}

	// Valid leader for current term
	rn.leaderId = &args.LeaderId
	rn.resetTimersLocked()

	// Check log consistency
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rn.logs) {
		rn.logger.Info("prev log index out of bounds", "prev_log_index", args.PrevLogIndex, "logs_size", len(rn.logs))
		return reply, nil
	}

	if rn.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		rn.logger.Info("log term mismatch", "index", args.PrevLogIndex, "expected_term", args.PrevLogTerm, "actual_term", rn.logs[args.PrevLogIndex].Term)
		return reply, nil
	}

	// Append new entries (truncate if necessary)
	rn.logs = rn.logs[:args.PrevLogIndex+1]
	rn.logs = append(rn.logs, args.Entries...)
	rn.logger.Info("follower appended entries", "prev_log_idx", args.PrevLogIndex, "num_new_entries", len(args.Entries), "new_log_len", len(rn.logs))

	// Update commit index
	if args.LeaderCommit > rn.commitIndex {
		newCommitIndex := min(args.LeaderCommit, len(rn.logs)-1)
		if newCommitIndex > rn.commitIndex {
			rn.commitIndex = newCommitIndex
			rn.logger.Info("follower updated commit index", "new_commit_index", rn.commitIndex)
			go rn.advanceStateMachine()
		}
	}

	reply.Success = true
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
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Step down if necessary
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.votedFor = nil
		if rn.state != Follower {
			rn.state = Follower
			rn.matchIndex = make(map[string]int)
			rn.nextIndex = make(map[string]int)
			rn.pendingRequests = make(map[int]*maelstrom.Message)
		}
	}

	reply := &RequestVoteReply{
		MessageBody: maelstrom.MessageBody{
			Type: "request_vote_ok",
		},
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	// Check if we can grant vote
	if args.Term < rn.currentTerm {
		rn.logger.Info(fmt.Sprintf("candidate term %d lower than ours %d, not granting vote!", args.Term, rn.currentTerm))
		return reply, nil
	}

	if rn.votedFor != nil && *rn.votedFor != args.CandidateId {
		rn.logger.Info(fmt.Sprintf("already voted for %s, not granting vote!", *rn.votedFor))
		return reply, nil
	}

	// Check log up-to-date
	currentLastLogIndex := len(rn.logs) - 1
	currentLastLogTerm := rn.logs[currentLastLogIndex].Term

	if args.LastLogTerm < currentLastLogTerm {
		rn.logger.Info(fmt.Sprintf("candidate log term %d older than ours %d, not granting vote!", args.LastLogTerm, currentLastLogTerm))
		return reply, nil
	}

	if args.LastLogTerm == currentLastLogTerm && args.LastLogIndex < currentLastLogIndex {
		rn.logger.Info(fmt.Sprintf("candidate log shorter (%d) than ours (%d) at same term, not granting vote!", args.LastLogIndex, currentLastLogIndex))
		return reply, nil
	}

	// Grant vote
	rn.votedFor = &args.CandidateId
	rn.resetTimersLocked()
	reply.VoteGranted = true
	rn.logger.Info(fmt.Sprintf("granting vote to %s!", args.CandidateId))

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
	rn.mu.Lock()
	defer rn.mu.Unlock()

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

type KVCasArgs struct {
	maelstrom.MessageBody

	Key  int
	From int
	To   int
}

type KVCasReply struct {
	maelstrom.MessageBody
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

	rn.mu.Lock()

	if rn.state == Leader {
		// Append to log
		logIndex := len(rn.logs)
		rn.logs = append(rn.logs, LogEntry{
			Term: rn.currentTerm,
			Op:   &KVWriteOp{Key: args.Key, Value: args.Value},
		})
		rn.pendingRequests[logIndex] = &msg
		rn.logger.Info("leader appended write to log", "log_idx", logIndex, "key", args.Key, "value", args.Value)
		rn.mu.Unlock()

		// Trigger replication
		go rn.replicateLogs()
		return nil // Reply will be sent when entry is committed

	} else if rn.leaderId != nil && *rn.leaderId != "" {
		leader := *rn.leaderId
		rn.mu.Unlock()

		// Proxy to leader
		rn.logger.Info("proxying write to leader", "leader_id", leader)
		return rn.n.RPC(leader, msg.Body, func(res maelstrom.Message) error {
			return rn.n.Reply(msg, res.Body)
		})

	} else {
		rn.mu.Unlock()
		return rn.n.Reply(msg, maelstrom.NewRPCError(maelstrom.TemporarilyUnavailable, "no leader known"))
	}
}

func (rn *RaftNode) handleKVCas(msg maelstrom.Message) error {
	args := &KVCasArgs{}
	if err := json.Unmarshal(msg.Body, args); err != nil {
		return err
	}

	rn.mu.Lock()

	if rn.state == Leader {
		// Append to log
		logIndex := len(rn.logs)
		rn.logs = append(rn.logs, LogEntry{
			Term: rn.currentTerm,
			Op:   &KVCasOp{Key: args.Key, From: args.From, To: args.To},
		})
		rn.pendingRequests[logIndex] = &msg
		rn.logger.Info("leader appended cas to log", "log_idx", logIndex, "key", args.Key)
		rn.mu.Unlock()

		// Trigger replication
		go rn.replicateLogs()
		return nil // Reply will be sent when entry is committed

	} else if rn.leaderId != nil && *rn.leaderId != "" {
		leader := *rn.leaderId
		rn.mu.Unlock()

		// Proxy to leader
		rn.logger.Info("proxying cas to leader", "leader_id", leader)
		return rn.n.RPC(leader, msg.Body, func(res maelstrom.Message) error {
			return rn.n.Reply(msg, res.Body)
		})

	} else {
		rn.mu.Unlock()
		return rn.n.Reply(msg, maelstrom.NewRPCError(maelstrom.TemporarilyUnavailable, "no leader known"))
	}
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
	duration := rand.Intn(int(ElectionTimeoutUpper)-int(ElectionTimeoutLower)) + int(ElectionTimeoutLower)
	return time.Duration(duration)
}

func (rn *RaftNode) StepDownTimeout() time.Duration {
	return StepDownTimeout
}

func (rn *RaftNode) HeartbeatTimeout() time.Duration {
	return HeartbeatTimeout
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

// resetTimersLocked resets timers - must be called with lock held
func (rn *RaftNode) resetTimersLocked() {
	if rn.electionTimer != nil {
		rn.electionTimer.Reset(rn.ElectionTimeout())
	}
	if rn.stepDownTimer != nil && rn.state == Leader {
		rn.stepDownTimer.Reset(rn.StepDownTimeout())
	}
}

func (rn *RaftNode) runEventLoop() {
	go func() {
		rn.n.Serve()
	}()

	rn.electionTimer = time.NewTimer(rn.ElectionTimeout())
	rn.heartbeatTimer = time.NewTimer(rn.HeartbeatTimeout())
	rn.stepDownTimer = time.NewTimer(rn.StepDownTimeout())

	for {
		select {
		case msg, ok := <-rn.n.MessageChannel():
			if !ok {
				rn.logger.Info("the chan was closed", "node_id", rn.n.ID())
				return
			}
			if err := rn.n.ProcessMessage(msg); err != nil {
				rn.logger.Error(fmt.Sprintf("error processing msg %s: %v", msg.Type(), err))
			}

		case <-rn.electionTimer.C:
			rn.logger.Info("election timeout expired", "node_id", rn.n.ID())
			if rn.state != Leader {
				rn.becomeCandidate()
			}
			rn.electionTimer.Reset(rn.ElectionTimeout())

		case <-rn.stepDownTimer.C:
			rn.logger.Info("step down timeout expired", "node_id", rn.n.ID())
			if rn.state == Leader {
				rn.becomeFollower()
			}
			rn.stepDownTimer.Reset(rn.StepDownTimeout())

		case <-rn.heartbeatTimer.C:
			if rn.state == Leader {
				go rn.heartbeat()
			}
			rn.heartbeatTimer.Reset(rn.HeartbeatTimeout())
		}
	}
}

// replicateLogs replicates logs to followers
func (rn *RaftNode) replicateLogs() {
	rn.mu.Lock()

	if rn.state != Leader {
		rn.mu.Unlock()
		return
	}

	// Save current state for use in callbacks
	currentTerm := rn.currentTerm

	for _, node := range rn.n.NodeIDs() {
		if node == rn.n.ID() {
			continue
		}

		ni := rn.nextIndex[node]
		if ni < 1 || ni > len(rn.logs) {
			rn.logger.Info("skipping replication due to next index", "next_index", ni, "logs_size", len(rn.logs), "to_node", node)
			continue
		}

		// Copy entries to send
		entries := make([]LogEntry, len(rn.logs[ni:]))
		copy(entries, rn.logs[ni:])

		prevLogIndex := ni - 1
		prevLogTerm := rn.logs[prevLogIndex].Term
		leaderCommit := rn.commitIndex

		body := &AppendEntriesArgs{
			MessageBody: maelstrom.MessageBody{
				Type: "append_entries",
			},
			Term:         currentTerm,
			LeaderId:     rn.n.ID(),
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}

		targetNode := node // Capture for closure
		rn.n.RPC(node, body, func(msg maelstrom.Message) error {
			reply := &AppendEntriesReply{}
			if err := json.Unmarshal(msg.Body, reply); err != nil {
				return err
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// Step down if we're behind
			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.votedFor = nil
				if rn.state != Follower {
					rn.state = Follower
					rn.matchIndex = make(map[string]int)
					rn.nextIndex = make(map[string]int)
					rn.pendingRequests = make(map[int]*maelstrom.Message)
				}
				return nil
			}

			// Ignore stale replies
			if rn.state != Leader || rn.currentTerm != currentTerm {
				return nil
			}

			if reply.Success {
				// Update indices
				newNextIndex := ni + len(entries)
				newMatchIndex := newNextIndex - 1

				if newNextIndex > rn.nextIndex[targetNode] {
					rn.nextIndex[targetNode] = newNextIndex
				}
				if newMatchIndex > rn.matchIndex[targetNode] {
					rn.matchIndex[targetNode] = newMatchIndex
				}

				rn.logger.Info("successful replication", "to", targetNode, "next_index", rn.nextIndex[targetNode], "match_index", rn.matchIndex[targetNode])

				// Try to advance commit index
				rn.advanceCommitIndexLocked()

				// Reset step down timer on successful communication
				rn.resetTimersLocked()
			} else {
				// Decrement nextIndex and retry
				if rn.nextIndex[targetNode] > 1 {
					rn.nextIndex[targetNode]--
					rn.logger.Info("replication failed, decrementing next_index", "to", targetNode, "new_next_index", rn.nextIndex[targetNode])
				}
			}

			return nil
		})
	}

	rn.mu.Unlock()
}

// advanceCommitIndexLocked advances commit index based on match indices
// Must be called with lock held
func (rn *RaftNode) advanceCommitIndexLocked() {
	if rn.state != Leader {
		return
	}

	// Find the median match index
	matchIndices := make([]int, 0, len(rn.n.NodeIDs()))
	for _, node := range rn.n.NodeIDs() {
		if node == rn.n.ID() {
			matchIndices = append(matchIndices, len(rn.logs)-1)
		} else if idx, ok := rn.matchIndex[node]; ok {
			matchIndices = append(matchIndices, idx)
		} else {
			matchIndices = append(matchIndices, 0)
		}
	}

	sort.Ints(matchIndices)
	majorityIndex := len(matchIndices) / 2
	newCommitIndex := matchIndices[majorityIndex]

	// Only commit entries from current term
	if newCommitIndex > rn.commitIndex && rn.logs[newCommitIndex].Term == rn.currentTerm {
		rn.commitIndex = newCommitIndex
		rn.logger.Info("leader advancing commit index", "new_commit_index", rn.commitIndex)
		go rn.advanceStateMachine()
	}
}

// advanceStateMachine applies committed entries to state machine
func (rn *RaftNode) advanceStateMachine() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.logs[rn.lastApplied]

		var reply interface{}
		var err error

		switch op := entry.Op.(type) {
		case *KVWriteOp:
			rn.stateMachine[op.Key] = op.Value
			rn.logger.Info("applied write", "index", rn.lastApplied, "key", op.Key, "value", op.Value)
			reply = &KVWriteReply{
				MessageBody: maelstrom.MessageBody{Type: "write_ok"},
			}

		case *KVCasOp:
			currentVal, exists := rn.stateMachine[op.Key]
			if !exists {
				err = &maelstrom.RPCError{
					Code: maelstrom.KeyDoesNotExist,
					Text: maelstrom.ErrorCodeText(maelstrom.KeyDoesNotExist),
				}
			} else if currentVal != op.From {
				err = &maelstrom.RPCError{
					Code: maelstrom.PreconditionFailed,
					Text: fmt.Sprintf("expected %d but was %d", op.From, currentVal),
				}
			} else {
				rn.stateMachine[op.Key] = op.To
				reply = &KVCasReply{
					MessageBody: maelstrom.MessageBody{Type: "cas_ok"},
				}
			}
			rn.logger.Info("applied cas", "index", rn.lastApplied, "key", op.Key, "error", err)
		}

		// If we're the leader and have a pending request for this entry, reply to client
		if rn.state == Leader {
			if msg, ok := rn.pendingRequests[rn.lastApplied]; ok {
				delete(rn.pendingRequests, rn.lastApplied)
				if err != nil {
					rn.n.Reply(*msg, err)
				} else if reply != nil {
					rn.n.Reply(*msg, reply)
				}
			}
		}
	}
}

func (rn *RaftNode) heartbeat() {
	rn.replicateLogs()
}

func (rn *RaftNode) broadcast(body any, h maelstrom.HandlerFunc) {
	for _, id := range rn.n.NodeIDs() {
		if id == rn.n.ID() {
			continue
		}
		rn.n.RPC(id, body, h)
	}
}

func (rn *RaftNode) requestVotes() {
	rn.mu.Lock()
	currentTerm := rn.currentTerm
	candidateId := rn.n.ID()
	lastLogIndex := len(rn.logs) - 1
	lastLogTerm := rn.logs[lastLogIndex].Term
	rn.mu.Unlock()

	votes := sync.Map{}
	votes.Store(candidateId, struct{}{})
	voteCount := 1

	body := &RequestVoteArgs{
		MessageBody: maelstrom.MessageBody{
			Type: "request_vote",
		},
		Term:         currentTerm,
		CandidateId:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rn.broadcast(body, func(msg maelstrom.Message) error {
		reply := &RequestVoteReply{}
		if err := json.Unmarshal(msg.Body, reply); err != nil {
			return err
		}

		rn.mu.Lock()
		defer rn.mu.Unlock()

		// Step down if we're behind
		if reply.Term > rn.currentTerm {
			rn.currentTerm = reply.Term
			rn.votedFor = nil
			if rn.state != Follower {
				rn.state = Follower
				rn.matchIndex = make(map[string]int)
				rn.nextIndex = make(map[string]int)
				rn.pendingRequests = make(map[int]*maelstrom.Message)
			}
			return nil
		}

		// Count vote if we're still a candidate in the same term
		if rn.state == Candidate && rn.currentTerm == currentTerm && reply.VoteGranted {
			if _, loaded := votes.LoadOrStore(msg.Src, struct{}{}); !loaded {
				voteCount++
				rn.logger.Info("received vote", "from", msg.Src, "total_votes", voteCount)

				if rn.haveMajority(voteCount) {
					rn.mu.Unlock()
					rn.becomeLeader()
					rn.mu.Lock()
				}
			}
		}

		return nil
	})
}

func (rn *RaftNode) haveMajority(numVotes int) bool {
	return numVotes > len(rn.n.NodeIDs())/2
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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
