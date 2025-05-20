package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// RaftNode holds the state for our Raft implementation.
type RaftNode struct {
	*maelstrom.Node
}

// NewRaftNode creates a new RaftNode.
func NewRaftNode() *RaftNode {
	maelstromNode := maelstrom.NewNode()

	rn := &RaftNode{
		maelstromNode,
	}
	return rn
}

func main() {
	rn := NewRaftNode()

	rn.Handle("init", rn.handleInit)
	rn.Handle("echo", rn.handleEcho)

	rn.Handle("request_vote", rn.handleRequestVote)
	rn.Handle("append_entries", rn.handleAppendEntries)

	if err := rn.Run(); err != nil {
		log.Fatalf("Node %s: error running Maelstrom node: %s", rn.ID(), err)
	}
}

// handleEcho is the method version of the echo handler.
func (rn *RaftNode) handleEcho(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "echo_ok"
	return rn.Reply(msg, body)
}

func (rn *RaftNode) handleInit(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	replyBody := make(map[string]any)
	replyBody["type"] = "init_ok"
	return rn.Reply(msg, replyBody)
}

func (rn *RaftNode) handleRequestVote(msg maelstrom.Message) error {
	return nil
}

func (rn *RaftNode) handleAppendEntries(msg maelstrom.Message) error {
	return nil
}
