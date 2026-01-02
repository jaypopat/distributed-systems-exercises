package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Log struct {
	messages        map[int]any // offset -> message
	nextOffset      int         // sequential
	committedOffset int
}

var (
	kafkaManager map[string]*Log
	mu           sync.RWMutex
)

func main() {

	n := maelstrom.NewNode()
	kafkaManager = make(map[string]*Log)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"].(string)
		message := body["msg"]

		mu.Lock()

		if kafkaManager[key] == nil {
			kafkaManager[key] = &Log{
				messages:        make(map[int]any),
				nextOffset:      0,
				committedOffset: -1,
			}

		}
		log := kafkaManager[key]
		offset := log.nextOffset
		log.messages[offset] = message
		log.nextOffset++

		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})
	})
	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsetsRaw := body["offsets"].(map[string]any)

		offsets := make(map[string]int)

		for topic, val := range offsetsRaw {
			floatVal := val.(float64)
			offsets[topic] = int(floatVal)
		}

		res := make(map[string][]any)

		mu.RLock()

		for topic, start_offset := range offsets {
			log := kafkaManager[topic]
			if log == nil {
				continue
			}

			for offset, val := range log.messages {
				if offset >= start_offset {
					if res[topic] == nil {
						res[topic] = []any{}
					}
					res[topic] = append(res[topic], []any{offset, val})
				}
			}
		}
		mu.RUnlock()

		return n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": res,
		})
	})
	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsetsRaw := body["offsets"].(map[string]any)

		offsets := make(map[string]int)

		for topic, val := range offsetsRaw {
			floatVal := val.(float64)
			offsets[topic] = int(floatVal)
		}
		mu.Lock()
		for topic, offset_till := range offsets {
			kafkaManager[topic].committedOffset = offset_till
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})
	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topicsRaw := body["keys"].([]any)
		topics := make([]string, len(topicsRaw))
		for i, t := range topicsRaw {
			topics[i] = t.(string)
		}

		res := make(map[string]int)

		mu.RLock()
		for _, topic := range topics {
			if log := kafkaManager[topic]; log != nil {
				res[topic] = log.committedOffset
			}
		}
		mu.RUnlock()

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": res,
		})
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
