package pilosa

import "testing"

func TestNewClusterWithAddress(t *testing.T) {
	c := NewClusterWithHost(NewURI())
	addresses := c.GetHosts()
	if len(addresses) != 1 || !addresses[0].Equals(NewURI()) {
		t.Fail()
	}
}

func TestAddAddress(t *testing.T) {
	const addr = "http://localhost:3000"
	c := NewCluster()
	if c.GetHosts() == nil {
		t.Fatalf("GetAddresses should not be nil")
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
	addresses := c.GetHosts()
	if len(addresses) != 1 || !addresses[0].Equals(target) {
		t.Fail()
	}
}

func TestGetHosts(t *testing.T) {
	c := NewCluster()
	if c.GetHost() != nil {
		t.Fatalf("GetHosts with empty cluster should return nil")
	}
	c = NewClusterWithHost(NewURI())
	if !c.GetHost().Equals(NewURI()) {
		t.Fatalf("GetHost should return a value if there are addresses in the cluster")
	}
}
