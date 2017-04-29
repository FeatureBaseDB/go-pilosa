
.PHONY: all cover doc generate-proto test test-all

all: test

cover:
	go test -cover -tags=integration

doc:
	godoc -url '/pkg/github.com/pilosa/go-client-pilosa' > index.html

generate:
	protoc --go_out=. internal/public.proto

test:
	go test

test-all:
	go test -tags=integration
