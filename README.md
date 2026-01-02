
Distributed systems have always fascinated me, and I wanted to learn how to build them using Go. I’m using fly.io’s Distributed Systems course (https://fly.io/dist-sys/) as a guide to dive into both distributed systems and Go.

## Running

1. Download [Maelstrom](https://github.com/jepsen-io/maelstrom/releases) and extract to project root
2. Build: `make <challenge>` (e.g., `make kafka-a`)
3. Test: `make test-<challenge>` (e.g., `make test-kafka-a`)
4. View results: `make results`
