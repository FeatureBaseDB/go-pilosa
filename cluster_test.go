package pilosa

import "testing"

func TestNewClusterWithAddress(t *testing.T) {
	c := NewClusterWithAddress(NewURI())
	addresses := c.GetAddresses()
	if len(addresses) != 1 || !addresses[0].Equals(NewURI()) {
		t.Fail()
	}
}

func TestAddAddress(t *testing.T) {
	const addr = "http://localhost:3000"
	c := NewCluster()
	if c.GetAddresses() == nil {
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
	c.AddAddress(uri)
	addresses := c.GetAddresses()
	if len(addresses) != 1 || !addresses[0].Equals(target) {
		t.Fail()
	}
}

func TestGetAddress(t *testing.T) {
	c := NewCluster()
	if c.GetAddress() != nil {
		t.Fatalf("GetAddress with empty cluster should return nil")
	}
	c = NewClusterWithAddress(NewURI())
	if !c.GetAddress().Equals(NewURI()) {
		t.Fatalf("GetAddress should return a value if there are addresses in the cluster")
	}
}
