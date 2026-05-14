.PHONY: build test vet

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...
