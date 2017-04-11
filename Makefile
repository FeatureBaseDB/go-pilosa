
.PHONY: test

all: test

test:
	go test

test-all:
	go test -tags=integration

cover:
	go test -cover -tags=integration
