package main

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/mihai-cherechesu/raft/internal/storage"
	"github.com/mihai-cherechesu/raft/pkg/raft"
)

func runEventLoop(rn *raft.RaftNode) {
	rn.ElectionTimer = time.NewTimer(rn.ElectionTimeout())
	rn.HeartbeatTimer = time.NewTimer(rn.HeartbeatTimeout())
	rn.StepDownTimer = time.NewTimer(rn.StepDownTimeout())

	logger := slog.Default()

	msgCh, err := rn.Serve()
	if err != nil {
		logger.Error("error serving", "error", err)
		panic(err)
	}

	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				logger.Info("the chan was closed", "node_id", rn.ID())
				return
			}
			if err := rn.Node.ProcessMessage(msg); err != nil {
				logger.Error(fmt.Sprintf("error processing msg %s: %v", msg.Type(), err))
			}

		case <-rn.ElectionTimer.C:
			logger.Info("election timeout expired", "node_id", rn.ID())
			if rn.State != raft.Leader {
				rn.BecomeCandidate()
			}
			rn.ElectionTimer.Reset(rn.ElectionTimeout())

		case <-rn.StepDownTimer.C:
			logger.Info("step down timeout expired", "node_id", rn.ID())
			if rn.State == raft.Leader {
				rn.BecomeFollower()
			}
			rn.StepDownTimer.Reset(rn.StepDownTimeout())

		case <-rn.HeartbeatTimer.C:
			logger.Info("heartbeat timeout expired", "node_id", rn.ID())
			if rn.State == raft.Leader {
				rn.Heartbeat()
			}
			rn.HeartbeatTimer.Reset(rn.HeartbeatTimeout())
		}
	}
}
func main() {
	rn := raft.NewRaftNode()

	rn.Handle(raft.TypeRequestVote, rn.HandleRequestVote)
	rn.Handle(raft.TypeAppendEntries, rn.HandleAppendEntries)

	rn.Handle(storage.TypeKVRead, storage.HandleKVRead)
	rn.Handle(storage.TypeKVWrite, storage.HandleKVWrite)
	rn.Handle(storage.TypeKVCas, storage.HandleKVCas)

	runEventLoop(rn)
}
