package pilosa

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var addressRegexp = regexp.MustCompile("^(([+a-z]+):\\/\\/)?([0-9a-z.-]+)?(:([0-9]+))?$")

// URI is a Pilosa server address
type URI struct {
	scheme string
	host   string
	port   uint16
}

// DefaultURI creates and returns the default URI
func DefaultURI() *URI {
	return &URI{
		scheme: "http",
		host:   "localhost",
		port:   10101,
	}
}

// NewURIFromHostPort returns a URI with specified host and port
func NewURIFromHostPort(host string, port uint16) *URI {
	return &URI{
		scheme: "http",
		host:   host,
		port:   port,
	}
}

// NewURIFromAddress parses the passed address and returns a URI
func NewURIFromAddress(address string) (*URI, error) {
	return parseAddress(address)
}

// Scheme returns the scheme of this URI
func (u *URI) Scheme() string {
	return u.scheme
}

// Host returns the host of this URI
func (u *URI) Host() string {
	return u.host
}

// Port returns the port of this URI
func (u *URI) Port() uint16 {
	return u.port
}

// Normalize returns the address in a form usable by a HTTP client
func (u *URI) Normalize() string {
	scheme := u.scheme
	index := strings.Index(scheme, "+")
	if index >= 0 {
		scheme = scheme[:index]
	}
	return fmt.Sprintf("%s://%s:%d", scheme, u.host, u.port)
}

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
