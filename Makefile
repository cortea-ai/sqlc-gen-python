.PHONY: build test

build:
	go build ./...

test: bin/sqlc-gen-python.wasm
	go test ./...

all: bin/sqlc-gen-python bin/sqlc-gen-python.wasm

bin/sqlc-gen-python: bin go.mod go.sum $(wildcard **/*.go)
	cd plugin/sqlc-gen-python && go build -o ../../bin/sqlc-gen-python ./main.go

bin/sqlc-gen-python.wasm: bin/sqlc-gen-python
	cd plugin/sqlc-gen-python && GOOS=wasip1 GOARCH=wasm go build -o ../../bin/sqlc-gen-python.wasm main.go

bin:
	mkdir -p bin
