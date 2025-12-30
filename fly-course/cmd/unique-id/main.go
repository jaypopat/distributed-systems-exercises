package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var counter uint64

func main() {

	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		timestamp := time.Now().UnixNano()
		count := atomic.AddUint64(&counter, 1)
		unique_id := fmt.Sprintf("%s-%d-%d", n.ID(), timestamp, count)

		// since goroutines run in parallel, our node can receive multiple messages at the same time
		// so node_id + timestamp isnt unique enough, if we add a counter per node which increments on each response
		// we can combine these to get a unique id

		body["type"] = "generate_ok"
		body["id"] = unique_id

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
