
.PHONY: all cover generate-proto test test-all

all: test

cover:
	go test -cover -tags=integration -coverprofile=coverage.out

generate:
	protoc --go_out=. internal/public.proto

test:
	go test

test-all:
	go test -tags=integration
