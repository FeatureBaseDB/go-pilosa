
.PHONY: all cover fast-cover generate-proto test test-all

VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)

all: test

cover:
	go test -cover -tags="integration fullcoverage" $(TESTFLAGS)

fast-cover:
	go test -cover -tags="integration" $(TESTFLAGS)

generate:
	protoc --go_out=. gopilosa_pbuf/public.proto

test:
	go test $(TESTFLAGS)

test-all:
	go test -tags=integration $(TESTFLAGS)

test-all-race:
	go test -race -tags=integration $(TESTFLAGS)

release:
	printf "package pilosa\nconst Version = \"$(VERSION)\"" > version.go
