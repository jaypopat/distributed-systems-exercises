package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// our approach in part b is valid and works for multi nodes but if some node goes down/partitioned and comes back up later, we dont have a synced state
// to mitigate against this we have a background function which gossips to a random node at x intervals.
// each node sends a periodic sync gossip to a random node with their state, they get a reply with what they dont have, and we add them to ours
// we kick off a goroutine which sends a message every second for example

func main() {
	n := maelstrom.NewNode()

	messages := make(map[int]bool) // since go doesnt have a set, we do this for deduping
	var mu sync.RWMutex

	topologyGraph := make(map[string][]string)

	ticker := time.NewTicker(time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			// we pick a random node
			node_ids := n.NodeIDs()

			// cant do .filter(x=>x!==n.ID()) hence this verbose iteration below so we dont sync with ourself
			peers := make([]string, 0, len(node_ids))
			for _, id := range node_ids {
				if id == n.ID() {
					continue
				}
				peers = append(peers, id)
			}

			if len(peers) <= 1 {
				continue
			}
			random_node_id := peers[rand.Intn(len(peers))]

			mu.RLock()
			messageList := make([]int, 0, len(messages))
			for v := range messages {
				messageList = append(messageList, v)
			}
			mu.RUnlock()

			n.Send(random_node_id, map[string]any{
				"type":    "periodic-sync",
				"message": messageList,
			})

		}
	}()

	n.Handle("periodic-sync", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		data := body["message"].([]any)
		num_data := make([]int, 0, len(data))
		for _, v := range data {
			num_data = append(num_data, int(v.(float64)))
		}

		mu.Lock()
		// Merge incoming numbers
		for _, v := range num_data {
			if !messages[v] {
				messages[v] = true
			}
		}

		// Compute diff to send back: what the sender is missing
		diff := []int{}
		for v := range messages {
			found := slices.Contains(num_data, v)
			if !found {
				diff = append(diff, v)
			}
		}
		mu.Unlock()

		// Send diff back to the sender
		if len(diff) > 0 {
			return n.Send(msg.Src, map[string]any{
				"type":    "periodic-sync-reply",
				"message": diff,
			})
		}

		return nil
	})
	n.Handle("periodic-sync-reply", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		data := body["message"].([]any)

		mu.Lock()
		defer mu.Unlock()
		for _, v := range data {
			m := int(v.(float64))
			messages[m] = true
		}
		return nil
	})

	// part b code below

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

		if err := json.Unmarshal(msg.Body, &body); err != nil {
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

		// now we broadcast to all neighbours

		neighbours := topologyGraph[n.ID()]
		mu.Unlock()

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
