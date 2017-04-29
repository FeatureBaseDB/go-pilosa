/* Copyright 2017 Pilosa Corp.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
 * CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 */

package pilosa

import (
	"testing"
)

func TestDefaultURI(t *testing.T) {
	uri := DefaultURI()
	compare(t, uri, "http", "localhost", 10101)
}

func TestURIWithHostPort(t *testing.T) {
	uri, err := NewURIFromHostPort("db1.pilosa.com", 3333)
	if err != nil {
		t.Fatal(err)
	}
	compare(t, uri, "http", "db1.pilosa.com", 3333)
}

func TestURIFromAddress(t *testing.T) {
	var test = []struct {
		address string
		scheme  string
		host    string
		port    uint16
	}{
		{"http+protobuf://db1.pilosa.com:3333", "http+protobuf", "db1.pilosa.com", 3333},
		{"db1.pilosa.com:3333", "http", "db1.pilosa.com", 3333},
		{"https://db1.pilosa.com", "https", "db1.pilosa.com", 10101},
		{"db1.pilosa.com", "http", "db1.pilosa.com", 10101},
		{"https://:3333", "https", "localhost", 3333},
		{":3333", "http", "localhost", 3333},
	}

	for _, item := range test {
		uri, err := NewURIFromAddress(item.address)
		if err != nil {
			t.Fatalf("Can't parse address: %s", item.address)
		}
		compare(t, uri, item.scheme, item.host, item.port)
	}
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
	if uri.Normalize() != "http://big-data.pilosa.com:6888" {
		t.Fatalf("Normalized address is not normal")
	}
}

func TestEquals(t *testing.T) {
	uri1 := DefaultURI()
	if uri1.Equals(nil) {
		t.Fatalf("URI should not be equal to nil")
	}
	if !uri1.Equals(DefaultURI()) {
		t.Fatalf("URI should be equal to another URI with the same scheme, host and port")
	}
}

func compare(t *testing.T, uri *URI, scheme string, host string, port uint16) {
	if uri.Scheme() != scheme {
		t.Fatalf("Scheme does not match: %s != %s", uri.scheme, scheme)
	}
	if uri.Host() != host {
		t.Fatalf("Host does not match: %s != %s", uri.host, host)
	}
	if uri.Port() != port {
		t.Fatalf("Port does not match: %d != %d", uri.port, port)
	}
}
