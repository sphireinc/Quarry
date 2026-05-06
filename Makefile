.DEFAULT_GOAL := check

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
