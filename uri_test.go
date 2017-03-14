package pilosa

import (
	"testing"
)

func TestDefaultURI(t *testing.T) {
	uri := NewURI()
	compare(t, uri, "http", "localhost", 15000)
}

func TestURIWithHostPort(t *testing.T) {
	uri := NewURIWithHostPort("db1.pilosa.com", 3333)
	compare(t, uri, "http", "db1.pilosa.com", 3333)
}

func TestURIParseFullAddress(t *testing.T) {
	uri, err := NewURIFromAddress("http+protobuf://db1.pilosa.com:3333")
	if err != nil {
		t.Fatalf("Can't parse address")
	}
	compare(t, uri, "http+protobuf", "db1.pilosa.com", 3333)
}

func TestURIParseHostPort(t *testing.T) {
	uri, err := NewURIFromAddress("db1.pilosa.com:3333")
	if err != nil {
		t.Fatalf("Can't parse address")
	}
	compare(t, uri, "http", "db1.pilosa.com", 3333)
}

func TestURIParseSchemeHost(t *testing.T) {
	uri, err := NewURIFromAddress("https://db1.pilosa.com")
	if err != nil {
		t.Fatalf("Can't parse address")
	}
	compare(t, uri, "https", "db1.pilosa.com", 15000)
}

func TestURIParseHost(t *testing.T) {
	uri, err := NewURIFromAddress("db1.pilosa.com")
	if err != nil {
		t.Fatalf("Can't parse address")
	}
	compare(t, uri, "http", "db1.pilosa.com", 15000)
}

func TestURIParseSchemePort(t *testing.T) {
	uri, err := NewURIFromAddress("https://:3333")
	if err != nil {
		t.Fatalf("Can't parse address")
	}
	compare(t, uri, "https", "localhost", 3333)
}

func TestURIParsePort(t *testing.T) {
	uri, err := NewURIFromAddress(":3333")
	if err != nil {
		t.Fatalf("Can't parse address")
	}
	compare(t, uri, "http", "localhost", 3333)
}

func TestInvalidAddress(t *testing.T) {
	var uri *URI
	var err error
	addresses := []string{"foo:bar", "http://foo:", "foo:", ":bar"}
	for _, addr := range addresses {
		uri, err = NewURIFromAddress(addr)
		if uri != nil || err == nil {
			t.Fatalf("Invalid address should return an error: %s", addr)
		}
	}
}

func TestNormalizedAddress(t *testing.T) {
	uri, err := NewURIFromAddress("http+protobuf://big-data.pilosa.com:6888")
	if err != nil {
		t.Fatalf("Can't parse address")
	}
	if uri.GetNormalizedAddress() != "http://big-data.pilosa.com:6888" {
		t.Fatalf("Normalized address is not normal")
	}
}

func TestEquals(t *testing.T) {
	uri1 := NewURI()
	if uri1.Equals(nil) {
		t.Fatalf("URI should not be equal to nil")
	}
	if !uri1.Equals(NewURI()) {
		t.Fatalf("URI should be equal to another URI with the same scheme, host and port")
	}
}

func compare(t *testing.T, uri *URI, scheme string, host string, port uint16) {
	if uri.GetScheme() != scheme {
		t.Fatalf("Scheme does not match")
	}
	if uri.GetHost() != host {
		t.Fatalf("Host does not match")
	}
	if uri.GetPort() != port {
		t.Fatalf("Port does not match")
	}
}
