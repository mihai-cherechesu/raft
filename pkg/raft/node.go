package raft

import (
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

	ElectionTimeoutLower = 300 * time.Millisecond
	ElectionTimeoutUpper = 500 * time.Millisecond

	HeartbeatTimeout = 150 * time.Millisecond
	StepDownTimeout  = 2000 * time.Millisecond
)

func isMajority(num, total int) bool {
	return num > total/2
}

// RaftNode holds the state for our Raft implementation.
type RaftNode struct {
	*maelstrom.Node
	mu     sync.Mutex
	logger *slog.Logger

	State        int
	StateMachine map[int]int

	currentTerm int
	votedFor    *string
	leaderId    *string
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  map[string]int
	matchIndex map[string]int

	ElectionTimer  *time.Timer
	HeartbeatTimer *time.Timer
	StepDownTimer  *time.Timer
}

type LogEntry struct {
	Term int
	Msg  interface{}
}

// NewRaftNode creates a new RaftNode.
func NewRaftNode() *RaftNode {
	maelstromNode := maelstrom.NewNode()

	rn := &RaftNode{
		Node:         maelstromNode,
		logger:       slog.Default(),
		State:        Follower,
		StateMachine: make(map[int]int),
		logs: []LogEntry{
			{
				Term: 0,
				Msg:  nil,
			},
		},
		nextIndex:   make(map[string]int),
		matchIndex:  make(map[string]int),
		lastApplied: 0,
		commitIndex: 0,
	}

	return rn
}

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

func (rn *RaftNode) BecomeFollower() {
	rn.State = Follower
	rn.matchIndex = make(map[string]int)
	rn.nextIndex = make(map[string]int)
	rn.leaderId = nil

	rn.logger.Info(fmt.Sprintf("node %s became follower for term %d", rn.ID(), rn.currentTerm))
	rn.ResetTimers()
}

func (rn *RaftNode) BecomeCandidate() {
	rn.State = Candidate
	rn.currentTerm++
	rn.votedFor = ptr.To(rn.ID())
	rn.leaderId = nil

	rn.logger.Info(fmt.Sprintf("node %s became candidate for term %d", rn.ID(), rn.currentTerm))
	rn.requestVotes()
	rn.ResetTimers()
}

func (rn *RaftNode) BecomeLeader() {
	rn.State = Leader
	rn.leaderId = ptr.To(rn.ID())

	rn.logger.Info(fmt.Sprintf("node %s became leader for term %d", rn.ID(), rn.currentTerm))
	rn.Heartbeat()
	rn.ResetTimers()
}

func (rn *RaftNode) StepDown(remoteTerm int) bool {
	if remoteTerm > rn.currentTerm {
		rn.State = Follower
		rn.currentTerm = remoteTerm
		rn.votedFor = nil
		rn.ResetTimers()
		return true
	}
	return false
}

func (rn *RaftNode) ResetTimers() {
	rn.ElectionTimer.Reset(rn.ElectionTimeout())
	rn.StepDownTimer.Reset(rn.StepDownTimeout())
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
