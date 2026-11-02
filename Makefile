# marketmux — common entry points.
SYMBOLS ?= BTC-USD,ETH-USD,SOL-USD,XRP-USD,LTC-USD
CAPTURE ?= $(HOME)/quantfactory/_data/marketmux/capture
DURATION ?= 30m

.PHONY: build test race bench lint fuzz demo data replay fanout-bench clean

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

race:
	go test -race ./...

# Go micro-benchmarks (decoders, book, engine, hub, codecs).
bench:
	go test -run '^$$' -bench . -benchmem -count 5 ./internal/... | tee bench/results/micro.txt

lint:
	go vet ./...
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed; CI runs it"

# Time-boxed fuzzing of every fuzz target.
FUZZTIME ?= 60s
fuzz:
	go test -run '^$$' -fuzz FuzzParseFixed -fuzztime $(FUZZTIME) ./internal/model/
	go test -run '^$$' -fuzz FuzzDecoders -fuzztime $(FUZZTIME) ./internal/venue/
	go test -run '^$$' -fuzz FuzzDecodeBatch -fuzztime $(FUZZTIME) ./internal/server/

# Record a bounded live session from public feeds (no keys; <= 30 min, capture capped at 2 GB).
data: build
	bin/marketmux -symbols $(SYMBOLS) -capture $(CAPTURE) -duration $(DURATION) -capture-cap-mb 2048 -stats-out bench/results/live_session.json

# Replay a capture as fast as possible (deterministic; reports output hash).
replay: build
	bin/marketmux -replay $(CAPTURE) -speed 0 -listen "" -grpc "" -symbols $(SYMBOLS)

fanout-bench: build
	bash bench/run_fanout.sh

# Live demo: connect to the venues and render the consolidated book.
demo: build
	@bin/marketmux -symbols BTC-USD,ETH-USD,SOL-USD -duration 75s -listen 127.0.0.1:8080 -grpc "" -log warn > /dev/null & \
	sleep 6; bin/mmwatch -url 'ws://127.0.0.1:8080/ws?policy=conflate' -duration 60s; wait

clean:
	rm -rf bin
