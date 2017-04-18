
.PHONY: all cover generate-proto test test-all

all: test

cover:
	go test -cover -tags=integration

generate-proto:
	protoc --go_out=. internal/public.proto

test:
	go test

test-all:
	go test -tags=integration
