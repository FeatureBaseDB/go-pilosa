
.PHONY: test

all: test

test:
	go test

integration-test:
	go test -tags=integration

cover:
	go test -cover -tags=integration