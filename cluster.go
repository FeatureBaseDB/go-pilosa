package pilosa

// Cluster is a simple ICluster implementation
type Cluster struct {
	addresses []*URI
	nextIndex int
}

// NewCluster creates a Cluster with no addresses
func NewCluster() *Cluster {
	return &Cluster{
		addresses: make([]*URI, 0),
	}
}

// NewClusterWithAddress creates a Cluster with the given address
func NewClusterWithAddress(address *URI) *Cluster {
	cluster := NewCluster()
	cluster.AddAddress(address)
	return cluster
}

// AddAddress adds an address to the cluster
func (c *Cluster) AddAddress(address *URI) {
	c.addresses = append(c.addresses, address)
}

// GetAddress returns the next address in the cluster
func (c *Cluster) GetAddress() *URI {
	if len(c.addresses) == 0 {
		return nil
	}
	// Return the transport, e.g., http from http+protobuf
	uri := c.addresses[c.nextIndex%len(c.addresses)]
	c.nextIndex = (c.nextIndex + 1) % len(c.addresses)
	return uri
}

// RemoveAddress removes an address from the cluster
func (c *Cluster) RemoveAddress(address *URI) {
	// TODO:
}

// GetAddresses returns all addresses in this cluster
func (c *Cluster) GetAddresses() []URI {
	arr := make([]URI, 0, len(c.addresses))
	for _, u := range c.addresses {
		arr = append(arr, *u)
	}
	return arr
}
