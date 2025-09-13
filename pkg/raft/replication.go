package raft

import maelstrom "github.com/jepsen-io/maelstrom/demo/go"

const (
	TypeAppendEntries = "append_entries"
)

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

func (rn *RaftNode) HandleAppendEntries(msg maelstrom.Message) error {
	return nil
}

func (rn *RaftNode) Heartbeat() {}
