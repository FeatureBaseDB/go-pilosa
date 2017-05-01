// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

package pilosa

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var addressRegexp = regexp.MustCompile("^(([+a-z]+):\\/\\/)?([0-9a-z.-]+)?(:([0-9]+))?$")

// URI represents a Pilosa URI.
// A Pilosa URI consists of three parts:
// 1) Scheme: Protocol of the URI. Default: http.
// 2) Host: Hostname or IP URI. Default: localhost.
// 3) Port: Port of the URI. Default: 10101.
//
// All parts of the URI are optional. The following are equivalent:
// 	http://localhost:10101
// 	http://localhost
// 	http://:10101
// 	localhost:10101
// 	localhost
// 	:10101
type URI struct {
	scheme string
	host   string
	port   uint16
}

// DefaultURI creates and returns the default URI.
func DefaultURI() *URI {
	return &URI{
		scheme: "http",
		host:   "localhost",
		port:   10101,
	}
}

// NewURIFromHostPort returns a URI with specified host and port.
func NewURIFromHostPort(host string, port uint16) (*URI, error) {
	// TODO: validate host
	return &URI{
		scheme: "http",
		host:   host,
		port:   port,
	}, nil
}

// NewURIFromAddress parses the passed address and returns a URI.
func NewURIFromAddress(address string) (*URI, error) {
	return parseAddress(address)
}

// Scheme returns the scheme of this URI.
func (u *URI) Scheme() string {
	return u.scheme
}

// Host returns the host of this URI.
func (u *URI) Host() string {
	return u.host
}

// Port returns the port of this URI.
func (u *URI) Port() uint16 {
	return u.port
}

// Normalize returns the address in a form usable by a HTTP client.
func (u *URI) Normalize() string {
	scheme := u.scheme
	index := strings.Index(scheme, "+")
	if index >= 0 {
		scheme = scheme[:index]
	}
	return fmt.Sprintf("%s://%s:%d", scheme, u.host, u.port)
}

// Equals returns true if the checked URI is equivalent to this URI.
func (u URI) Equals(other *URI) bool {
	if other == nil {
		return false
	}
	return u.scheme == other.scheme &&
		u.host == other.host &&
		u.port == other.port
}

func parseAddress(address string) (uri *URI, err error) {
	m := addressRegexp.FindStringSubmatch(address)
	if m == nil {
		return nil, errors.New("Invalid address")
	}
	scheme := "http"
	if m[2] != "" {
		scheme = m[2]
	}
	host := "localhost"
	if m[3] != "" {
		host = m[3]
	}
	var port = 10101
	if m[5] != "" {
		port, _ = strconv.Atoi(m[5])
	}
	uri = &URI{
		scheme: scheme,
		host:   host,
		port:   uint16(port),
	}
	return
}
