
.PHONY: all cover fast-cover generate-proto test test-all

all: test

cover:
	go test -cover -tags="integration fullcoverage"

fast-cover:
	go test -cover -tags="integration"

generate:
	protoc --go_out=. gopilosa_pbuf/public.proto

test:
	go test

test-all:
	go test -tags=integration
