.DEFAULT_GOAL := check

CACHE_BASE ?= $(CURDIR)/.cache
GOCACHE ?= $(CACHE_BASE)/go-build
GOMODCACHE ?= $(CACHE_BASE)/go-mod
export GOCACHE GOMODCACHE

.PHONY: fmt vet test race tidy check distcheck

fmt:
	go fmt ./...

vet:
	go vet ./...

test:
	go test ./...

race:
	go test -race ./...

tidy:
	go mod tidy

check: fmt vet test

distcheck: tidy
	git diff --exit-code -- go.mod go.sum
	go test ./...
