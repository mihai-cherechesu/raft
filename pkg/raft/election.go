package raft

import (
	"context"
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	TypeRequestVote = "request_vote"
)

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

func (rn *RaftNode) HandleRequestVote(msg maelstrom.Message) error {
	return nil
}

func (rn *RaftNode) requestVotes() {
	body := &RequestVoteArgs{
		MessageBody: maelstrom.MessageBody{
			Type: TypeRequestVote,
		},
		Term:         rn.currentTerm,
		CandidateId:  rn.ID(),
		LastLogIndex: len(rn.logs) - 1,
		LastLogTerm:  rn.logs[len(rn.logs)-1].Term,
	}

	respCh := make(chan maelstrom.Message, len(rn.NodeIDs())-1)
	for _, node := range rn.NodeIDs() {
		if node == rn.ID() {
			continue
		}

		rn.RPC(node, body, func(msg maelstrom.Message) error {
			respCh <- msg
			return nil
		})
	}

	rn.collectVotes(respCh)
}

func (rn *RaftNode) collectVotes(respCh chan maelstrom.Message) {
	votes := make(map[string]struct{})
	votes[rn.ID()] = struct{}{}

	ctx, cancel := context.WithTimeout(context.Background(), rn.ElectionTimeout())
	defer cancel()

	for {
		select {
		case msg := <-respCh:
			reply := &RequestVoteReply{}
			if err := json.Unmarshal(msg.Body, reply); err != nil {
				continue
			}

			rn.logger.Info("received request vote reply", "vote granted", reply.VoteGranted)
			if ok := rn.StepDown(reply.Term); ok {
				rn.logger.Info("stepped down, term bigger!")
				return
			}

			votes[msg.Src] = struct{}{}
			if ok := isMajority(len(votes), len(rn.NodeIDs())); ok {
				rn.logger.Info("here is the leader!")
				rn.BecomeLeader()
			}

		case <-ctx.Done():
			rn.logger.Info("deadline on collecting votes, returning")
			return
		}
	}
}
