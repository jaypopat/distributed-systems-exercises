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

// now we need to optimise the topology
// spanning trees would be ideal since we dont need cycles here if we can reach all once, thats ideal - https://www.geeksforgeeks.org/dsa/spanning-tree/
// learned this in networking in college (along with djikstra etc)
// we can use k-ary trees here - this basically lets us split a set in a way no cycles occur and gives us an adjacency list per parent
// 1-9 with k=3 makes 1,2,3 the parents and rest are slotted into the parent bracket using i-1 / k
// implemented that in k-ary_tree.go

func main() {
	n := maelstrom.NewNode()

	messages := make(map[int]bool) // since go doesnt have a set, we do this for deduping
	var mu sync.RWMutex

	var topologyGraph *KaryTree

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
		neighbours := topologyGraph.NeighborsOf(n.ID())

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

		neighbours := topologyGraph.NeighborsOf(n.ID())
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

		topologyGraph = NewKaryTree(n.NodeIDs(), 3)

		response := map[string]any{
			"type": "topology_ok",
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
