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

// Cluster contains hosts in a Pilosa cluster.
type Cluster struct {
	hosts     []*URI
	nextIndex int
}

// DefaultCluster returns the default Cluster.
func DefaultCluster() *Cluster {
	return &Cluster{
		hosts: make([]*URI, 0),
	}
}

// NewClusterWithHost returns a cluster with the given URIs.
func NewClusterWithHost(hosts ...*URI) *Cluster {
	cluster := DefaultCluster()
	for _, host := range hosts {
		cluster.AddHost(host)
	}
	return cluster
}

// AddHost adds a host to the cluster.
func (c *Cluster) AddHost(address *URI) {
	c.hosts = append(c.hosts, address)
}

// Host returns the next host in the cluster.
func (c *Cluster) Host() *URI {
	if len(c.hosts) == 0 {
		return nil
	}
	// Return the transport, e.g., http from http+protobuf
	uri := c.hosts[c.nextIndex%len(c.hosts)]
	c.nextIndex = (c.nextIndex + 1) % len(c.hosts)
	return uri
}

// RemoveHost removes the host with the given URI from the cluster.
func (c *Cluster) RemoveHost(address *URI) {
	for i, uri := range c.hosts {
		if uri.Equals(address) {
			c.hosts = append(c.hosts[:i], c.hosts[i+1:]...)
			break
		}
	}
}

// Hosts returns all hosts in the cluster.
func (c *Cluster) Hosts() []URI {
	arr := make([]URI, 0, len(c.hosts))
	for _, u := range c.hosts {
		arr = append(arr, *u)
	}
	return arr
}
