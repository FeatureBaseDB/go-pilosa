.PHONY: all cover fast-cover generate-proto test test-all test-all-race start-pilosa stop-pilosa

VERSION := $(shell git describe --tags 2> /dev/null || echo unknown)
GOOS ?= linux
PILOSA_VERSION ?= master
PILOSA_BIND ?= https://:20101
PILOSA_VERSION_ID := pilosa-$(PILOSA_VERSION)-$(GOOS)-amd64
PILOSA_DOWNLOAD_URL ?= https://s3.amazonaws.com/build.pilosa.com/$(PILOSA_VERSION_ID).tar.gz

all: test

cover:
	go test -cover -tags="fullcoverage" $(TESTFLAGS)

fast-cover:
	go test -cover $(TESTFLAGS)

generate:
	protoc --go_out=. gopilosa_pbuf/public.proto

test: vendor
	go test -tags=nointegration $(TESTFLAGS)

test-all: vendor
	go test $(TESTFLAGS)

test-all-race: vendor
	$(MAKE) start-pilosa
	PILOSA_BIND=https://:20101 go test -race $(TESTFLAGS)
	$(MAKE) stop-pilosa

vendor: Gopkg.toml
	dep ensure -vendor-only
	touch vendor

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
