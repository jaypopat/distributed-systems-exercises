build:
		go build -o bin/maelstrom
run:
		./bin/maelstrom

clean:
		rm -rf bin/maelstrom
echo:
	    ./maelstrom/maelstrom/maelstrom test -w echo --bin ./bin/maelstrom --node-count 1 --time-limit 10
unique-id:
	    ./maelstrom/maelstrom/maelstrom test -w echo --bin ./bin/maelstrom --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
broadcast:
		./maelstrom/maelstrom/maelstrom test -w echo --bin ./bin/maelstrom --node-count 1 --time-limit 20 --rate 10
grow-only:
	    ./maelstrom/maelstrom/maelstrom test -w echo --bin ./bin/maelstrom --node-count 3 --rate 100 --time-limit 20 --nemesis partition
kafka:
		./maelstrom/maelstrom/maelstrom test -w echo --bin ./bin/maelstrom --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
kv:
	    ./maelstrom/maelstrom/maelstrom test -w echo --bin ./bin/maelstrom --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
