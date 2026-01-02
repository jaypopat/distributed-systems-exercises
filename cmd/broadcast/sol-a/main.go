package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	messages := make(map[int]bool) // since go doesnt have a set, we do this for deduping
	var mu sync.RWMutex

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := int(body["message"].(float64))

		mu.Lock()
		messages[message] = true
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.RLock()
		messageList := make([]int, 0, len(messages))
		for msg := range messages {
			messageList = append(messageList, msg)
		}
		mu.RUnlock()

		body["type"] = "read_ok"
		body["messages"] = messageList
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		response := map[string]any{
			"type": "topology_ok",
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
