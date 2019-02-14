.PHONY: all cover fast-cover generate-proto test test-all test-all-race gometalinter

PILOSA_VERSION ?= master
PILOSA_VERSION_ID := pilosa-$(PILOSA_VERSION)-$(GOOS)-amd64
PILOSA_DOWNLOAD_URL ?= https://s3.amazonaws.com/build.pilosa.com/$(PILOSA_VERSION_ID).tar.gz
VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
GOOS ?= linux
PILOSA_BIND ?= http://:10101
export GO111MODULE=on

all: test

cover:
	PILOSA_BIND=$(PILOSA_BIND) go test ./... -cover $(TESTFLAGS) -tags="fullcoverage" -covermode=count -coverprofile=build/coverage.out

fast-cover:
	PILOSA_BIND=$(PILOSA_BIND) go test ./... -cover $(TESTFLAGS)

generate:
	# This ensures that we don't forget to change the package name if we copy the proto definition from pilosa in order to update it.
	# It is important that the packages have different names because proto can misbehave if pilosa and go-pilosa are imported into the same codebase.
	sed -i -e 's/package internal;/package gopilosa_pbuf;/g' gopilosa_pbuf/public.proto
	protoc --go_out=. gopilosa_pbuf/public.proto

test:
	PILOSA_BIND=$(PILOSA_BIND) go test ./... -tags=nointegration $(TESTFLAGS)

test-all:
	PILOSA_BIND=$(PILOSA_BIND) go test ./... $(TESTFLAGS)

test-all-race:
	PILOSA_BIND=$(PILOSA_BIND) $(MAKE) test-all TESTFLAGS=-race

vendor: go.mod
	go mod vendor

release:
	printf "package pilosa\nconst Version = \"$(VERSION)\"" > version.go

clean:
	rm -rf build vendor

start-pilosa: build/pilosa.pid

stop-pilosa: build/pilosa.pid
	kill `cat $<` && rm $< && rm -rf build/https_data

build/pilosa.pid: build/$(PILOSA_VERSION_ID)/pilosa build/test.pilosa.local.key
	build/$(PILOSA_VERSION_ID)/pilosa server --metric.diagnostics=false -b https://:20101 -d build/https_data --tls.skip-verify --tls.certificate build/test.pilosa.local.crt --tls.key build/test.pilosa.local.key --cluster.disabled static & echo $$! > $@

build/test.pilosa.local.key:
	openssl req -x509 -newkey rsa:4096 -keyout build/test.pilosa.local.key -out build/test.pilosa.local.crt -days 3650 -nodes -subj "/C=US/ST=Texas/L=Austin/O=Pilosa/OU=Com/CN=test.pilosa.local"

build/$(PILOSA_VERSION_ID)/pilosa: build/$(PILOSA_VERSION_ID).tar.gz
	cd build && tar xf $(PILOSA_VERSION_ID).tar.gz
	touch $@

build/$(PILOSA_VERSION_ID).tar.gz:
	mkdir -p build
	cd build && wget $(PILOSA_DOWNLOAD_URL)

gometalinter:
	GO111MODULE=off gometalinter --vendor --disable-all \
            --deadline=120s \
            --enable=deadcode \
            --enable=gochecknoinits \
            --enable=gofmt \
            --enable=goimports \
            --enable=misspell \
            --enable=vet \
            --exclude "^gopilosa_pbuf/.*\.go" \
            ./...

install-gometalinter:
	GO111MODULE=off go get -u github.com/alecthomas/gometalinter
	GO111MODULE=off gometalinter --install
	GO111MODULE=off go get github.com/remyoudompheng/go-misc/deadcode
