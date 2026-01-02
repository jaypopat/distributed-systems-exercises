package main

import "slices"

type KaryTree struct {
	NodeIDs                  []string
	RootID                   string
	K                        int
	parent_to_children       map[string][]string
	children_to_parent_cache map[string]int // easier than doing i-1/k all the time
}

func NewKaryTree(nodeIDs []string, k int) *KaryTree {
	ids := append([]string(nil), nodeIDs...)
	slices.Sort(ids)

	index := make(map[string]int, len(ids))
	for i, id := range ids {
		index[id] = i
	}

	root := ids[0]
	children := make(map[string][]string)

	for i, id := range ids {
		if i == 0 {
			continue
		}
		parentIdx := (i - 1) / k
		parentID := ids[parentIdx]
		children[parentID] = append(children[parentID], id)
	}

	return &KaryTree{
		NodeIDs:                  ids,
		RootID:                   root,
		K:                        k,
		parent_to_children:       children,
		children_to_parent_cache: index,
	}
}

func (t *KaryTree) childrenOf(id string) []string {
	return t.parent_to_children[id]
}

func (t *KaryTree) parentOf(id string) (string, bool) {
	i, ok := t.children_to_parent_cache[id]
	if !ok || i == 0 {
		return "", false
	}
	pIdx := (i - 1) / t.K
	return t.NodeIDs[pIdx], true
}
func (t *KaryTree) NeighborsOf(id string) []string {
	var out []string

	if p, ok := t.parentOf(id); ok {
		out = append(out, p)
	}
	out = append(out, t.childrenOf(id)...)

	return out
}
