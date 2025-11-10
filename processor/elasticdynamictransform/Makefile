.DEFAULT_GOAL := build

build:
	go build ./...

test:
	go test -v ./...

lint:
	golangci-lint run

.PHONY: build test lint
