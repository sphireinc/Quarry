.DEFAULT_GOAL := check

GOCACHE := $(CURDIR)/.cache/go-build
GOENV := GOCACHE=$(GOCACHE)

export GOCACHE

.PHONY: fmt fmt-write vet test test-race race staticcheck vulncheck examples tidy check distcheck

fmt:
	@dirs="$$( $(GOENV) go list -f '{{.Dir}}' ./...)"; \
	files="$$(gofmt -l $$dirs)"; \
	if [ -n "$$files" ]; then \
		printf '%s\n' "$$files"; \
		exit 1; \
	fi

fmt-write:
	dirs="$$( $(GOENV) go list -f '{{.Dir}}' ./...)"; \
	gofmt -w $$dirs

vet:
	$(GOENV) go vet ./...

test:
	$(GOENV) go test ./...

test-race:
	$(GOENV) go test -race ./...

race: test-race

staticcheck:
	$(GOENV) go run honnef.co/go/tools/cmd/staticcheck@latest ./...

vulncheck:
	$(GOENV) go run golang.org/x/vuln/cmd/govulncheck@latest ./...

examples:
	$(GOENV) go test ./examples/...

tidy:
	$(GOENV) go mod tidy

check: fmt vet test

distcheck: tidy fmt vet test test-race examples
	git diff --exit-code -- go.mod go.sum
