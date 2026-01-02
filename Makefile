BIN_DIR := bin
MAELSTROM := ./maelstrom/maelstrom/maelstrom

.PHONY: all clean results

all:
	@echo "Use 'make <challenge>' to build or 'make test-<challenge>' to test"

$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

echo: $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-echo ./cmd/echo

unique-id: $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-unique-id ./cmd/unique-id

grow-only: $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-counter ./cmd/grow-only

# Broadcast variants
broadcast-%: $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-broadcast-$* ./cmd/broadcast/sol-$*

# Kafka variants
kafka-%: $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-kafka-$* ./cmd/kafka/sol-$*

# KV variants
kv-%: $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-kv-$* ./cmd/kv/sol-$*

clean:
	rm -rf $(BIN_DIR)

# Tests
test-echo: echo
	$(MAELSTROM) test -w echo --bin ./$(BIN_DIR)/maelstrom-echo --node-count 1 --time-limit 10

test-unique-id: unique-id
	$(MAELSTROM) test -w unique-ids --bin ./$(BIN_DIR)/maelstrom-unique-id --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

test-broadcast-a: broadcast-a
	$(MAELSTROM) test -w broadcast --bin ./$(BIN_DIR)/maelstrom-broadcast-a --node-count 1 --time-limit 20 --rate 10

test-broadcast-b: broadcast-b
	$(MAELSTROM) test -w broadcast --bin ./$(BIN_DIR)/maelstrom-broadcast-b --node-count 5 --time-limit 20 --rate 10

test-broadcast-c: broadcast-c
	$(MAELSTROM) test -w broadcast --bin ./$(BIN_DIR)/maelstrom-broadcast-c --node-count 5 --time-limit 20 --rate 10 --nemesis partition

test-broadcast-d: broadcast-d
	$(MAELSTROM) test -w broadcast --bin ./$(BIN_DIR)/maelstrom-broadcast-d --node-count 25 --time-limit 20 --rate 100 --latency 100

test-broadcast-e: broadcast-e
	$(MAELSTROM) test -w broadcast --bin ./$(BIN_DIR)/maelstrom-broadcast-e --node-count 25 --time-limit 20 --rate 100 --latency 100

test-grow-only: grow-only
	$(MAELSTROM) test -w g-counter --bin ./$(BIN_DIR)/maelstrom-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

test-kafka-a: kafka-a
	$(MAELSTROM) test -w kafka --bin ./$(BIN_DIR)/maelstrom-kafka-a --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

test-kafka-b: kafka-b
	$(MAELSTROM) test -w kafka --bin ./$(BIN_DIR)/maelstrom-kafka-b --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

test-kafka-c: kafka-c
	$(MAELSTROM) test -w kafka --bin ./$(BIN_DIR)/maelstrom-kafka-c --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

test-kv-a: kv-a
	$(MAELSTROM) test -w lin-kv --bin ./$(BIN_DIR)/maelstrom-kv-a --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

test-kv-b: kv-b
	$(MAELSTROM) test -w lin-kv --bin ./$(BIN_DIR)/maelstrom-kv-b --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

test-kv-c: kv-c
	$(MAELSTROM) test -w lin-kv --bin ./$(BIN_DIR)/maelstrom-kv-c --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

results:
	$(MAELSTROM) serve
