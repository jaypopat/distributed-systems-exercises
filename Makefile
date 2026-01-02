BIN_DIR := bin
MAELSTROM := ./maelstrom/maelstrom/maelstrom

# the challenges ive completed, so they can be built and tested
BINARIES := echo unique-id broadcast-a broadcast-b broadcast-c
# grow-only kafka kv

.PHONY: all build clean $(BINARIES) test-echo test-unique-id test-broadcast-a test-broadcast-b test-broadcast-c test-grow-only test-kafka test-kv

all: build

build: $(BINARIES)

echo:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-echo ./cmd/echo

unique-id:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-unique-id ./cmd/unique-id

# broadcast - multi step
broadcast-a:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-broadcast-a ./cmd/broadcast/sol-a

broadcast-b:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-broadcast-b ./cmd/broadcast/sol-b

broadcast-c:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-broadcast-c ./cmd/broadcast/sol-c

broadcast-d:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-broadcast-d ./cmd/broadcast/sol-d



grow-only:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-grow-only ./cmd/grow-only

kafka:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-kafka ./cmd/kafka

kv:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/maelstrom-kv ./cmd/kv

clean:
	rm -rf $(BIN_DIR)

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


test-grow-only: grow-only
	$(MAELSTROM) test -w g-counter --bin ./$(BIN_DIR)/maelstrom-grow-only --node-count 3 --rate 100 --time-limit 20 --nemesis partition

test-kafka: kafka
	$(MAELSTROM) test -w kafka --bin ./$(BIN_DIR)/maelstrom-kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

test-kv: kv
	$(MAELSTROM) test -w lin-kv --bin ./$(BIN_DIR)/maelstrom-kv --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

#####
results:
	$(MAELSTROM) serve
