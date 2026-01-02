package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	COUNTER_KEY := "grow-only-counter"

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// network partition / someone else wrote -> we keep tryin
			// CompareAndSwap returns err if we are working on a stale read

			val, _ := kv.ReadInt(ctx, COUNTER_KEY)
			incr := int(body["delta"].(float64))

			if err := kv.CompareAndSwap(ctx, COUNTER_KEY, val, val+incr, true); err != nil {
				continue
			}
			break

		}

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		val, err := kv.ReadInt(ctx, COUNTER_KEY)
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": val,
		})
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
