package storage

import maelstrom "github.com/jepsen-io/maelstrom/demo/go"

const (
	TypeKVRead  = "read"
	TypeKVWrite = "write"
	TypeKVCas   = "cas"
)

func HandleKVRead(msg maelstrom.Message) error {
	return nil
}
func HandleKVWrite(msg maelstrom.Message) error {
	return nil
}
func HandleKVCas(msg maelstrom.Message) error {
	return nil
}
