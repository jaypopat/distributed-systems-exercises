package main

import (
	"fmt"
	"sync"
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
	fmt.Println("gotta make sol-b more efficient")
}
