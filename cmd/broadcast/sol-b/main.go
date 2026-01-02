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

	topologyGraph := make(map[string][]string)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := int(body["message"].(float64))

		mu.Lock()

		if messages[message] {
			mu.Unlock()
			return n.Reply(msg, map[string]any{
				"type": "broadcast_ok",
			})
		}

		messages[message] = true

		// get all neighbours for this node
		neighbours := topologyGraph[n.ID()]

		for _, neighbour_node_id := range neighbours {
			// as we get a new message lets send it through
			n.Send(neighbour_node_id, map[string]any{
				"type":    "gossip",
				"message": message,
			})
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("gossip", func(msg maelstrom.Message) error {

		var body map[string]any

		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		// now we see do we have this data point in our local storage
		val := int(body["message"].(float64))

		mu.Lock()

		if messages[val] {
			mu.Unlock()
			return nil // we dont gossip something we've already seen
		}
		messages[val] = true
		mu.Unlock()

		// now we broadcast to all neighbours

		neighbours := topologyGraph[n.ID()]

		for _, neighbour := range neighbours {
			if neighbour == msg.Src {
				continue
			}
			n.Send(neighbour, map[string]any{
				"type":    "gossip",
				"message": val,
			})
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.RLock()
		messageList := make([]int, 0, len(messages))
		for v := range messages {
			messageList = append(messageList, v)
		}
		mu.RUnlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": messageList,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		neighbour_obj := body["topology"].(map[string]any)

		for node, neighbours := range neighbour_obj {
			neighbourSlice := make([]string, 0, len(neighbours.([]any)))
			for _, v := range neighbours.([]any) {
				if s, ok := v.(string); ok {
					neighbourSlice = append(neighbourSlice, s)
				}
			}
			topologyGraph[node] = neighbourSlice
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
