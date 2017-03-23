package pilosa

import "testing"

func TestNewClusterWithHost(t *testing.T) {
	c := NewClusterWithHost(NewURI())
	hosts := c.Hosts()
	if len(hosts) != 1 || !hosts[0].Equals(NewURI()) {
		t.Fail()
	}
}

func TestAddHost(t *testing.T) {
	const addr = "http://localhost:3000"
	c := NewCluster()
	if c.Hosts() == nil {
		t.Fatalf("Hosts should not be nil")
	}
	uri, err := NewURIFromAddress(addr)
	if err != nil {
		t.Fatalf("Cannot parse address")
	}
	target, err := NewURIFromAddress(addr)
	if err != nil {
		t.Fatalf("Cannot parse address")
	}
	c.AddHost(uri)
	hosts := c.Hosts()
	if len(hosts) != 1 || !hosts[0].Equals(target) {
		t.Fail()
	}
}

func TestHosts(t *testing.T) {
	c := NewCluster()
	if c.Host() != nil {
		t.Fatalf("Hosts with empty cluster should return nil")
	}
	c = NewClusterWithHost(NewURI())
	if !c.Host().Equals(NewURI()) {
		t.Fatalf("Host should return a value if there are hosts in the cluster")
	}
}

func TestRemoveHost(t *testing.T) {
	uri, err := NewURIFromAddress("db1.pilosa.com:9999")
	if err != nil {
		t.Fatal(err)
	}
	c := NewClusterWithHost(uri)
	if len(c.hosts) != 1 {
		t.Fatalf("The cluster should contain the host")
	}
	uri, err = NewURIFromAddress("db1.pilosa.com:9999")
	c.RemoveHost(uri)
	if len(c.hosts) != 0 {
		t.Fatalf("The cluster should not contain the host")
	}
}
