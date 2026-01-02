package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

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
	kv := maelstrom.NewLinKV(n)

	// we need to represnt our original ds topics logs offsets etc in a kv format
	// we can still keep a local manager per node but fields like next offset,message etc are meant to be in sync hence we do compare and swap with a kv
	// and for next offset we do `topic`-nextoffset = offset -> product-nextoffset=1090
	// lets use cas as well to make sure messages get sent without losing them - same api as grow-only ie CompareAndSwap

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topic := body["key"].(string)
		message := body["msg"]

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var offset int

		// lets get offset from synced kv first
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			currentOffset, err := kv.ReadInt(ctx, fmt.Sprintf("%s-next_offset", topic))
			if err != nil {
				currentOffset = 0
			}

			if err := kv.CompareAndSwap(ctx, fmt.Sprintf("%s-next_offset", topic), currentOffset, currentOffset+1, true); err != nil {
				continue
			}

			offset = currentOffset
			break
		}

		// lets write the message to kv so all nodes can fetch
		if err := kv.Write(ctx, fmt.Sprintf("%s-%d", topic, offset), message); err != nil {
			return err
		}

		// now we cache locally using the same offset
		mu.Lock()
		if kafkaManager[topic] == nil {
			kafkaManager[topic] = &Log{
				messages:        make(map[int]any),
				nextOffset:      0,
				committedOffset: -1,
			}
		}
		kafkaManager[topic].messages[offset] = message
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

		// we have a topic and offset, we need to get all records above this offset
		// topic-offset gives us a msg
		// topic-nextoffset gives us max bound
		// we loop from lower bound to upper bound

		// firstly we check if we have the msg locally if we do we dont hit the kv (ie we pushed it)

		mu.RLock()

		// for topic, start_offset := range offsets {
		// 	log := kafkaManager[topic]
		// 	if log == nil {
		// 		continue
		// 	}

		// 	for offset, val := range log.messages {
		// 		if offset >= start_offset {
		// 			if res[topic] == nil {
		// 				res[topic] = []any{}
		// 			}
		// 			res[topic] = append(res[topic], []any{offset, val})
		// 		}
		// 	}
		// }
		// mu.RUnlock()

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
		// offsetsRaw := body["offsets"].(map[string]any)

		// offsets := make(map[string]int)

		// for topic, val := range offsetsRaw {
		// 	floatVal := val.(float64)
		// 	offsets[topic] = int(floatVal)
		// }
		// mu.Lock()
		// for topic, offset_till := range offsets {
		// 	kafkaManager[topic].committedOffset = offset_till
		// }
		// mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
	})
	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// topicsRaw := body["keys"].([]any)
		// topics := make([]string, len(topicsRaw))
		// for i, t := range topicsRaw {
		// 	topics[i] = t.(string)
		// }

		res := make(map[string]int)

		// mu.RLock()
		// for _, topic := range topics {
		// 	if log := kafkaManager[topic]; log != nil {
		// 		res[topic] = log.committedOffset
		// 	}
		// }
		// mu.RUnlock()

		return n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": res,
		})
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
