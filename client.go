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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pilosa/go-pilosa/internal"
)

// Client is the HTTP client for Pilosa server.
type Client struct {
	cluster *Cluster
	client  *http.Client
}

// DefaultClient creates a client with the default address and options.
func DefaultClient() *Client {
	return NewClientWithURI(DefaultURI())
}

// NewClientWithURI creates a client with the given server address.
func NewClientWithURI(uri *URI) *Client {
	return NewClientWithCluster(NewClusterWithHost(uri), nil)
}

// NewClientWithCluster creates a client with the given cluster and options.
// Pass nil for default options.
func NewClientWithCluster(cluster *Cluster, options *ClientOptions) *Client {
	if options == nil {
		options = &ClientOptions{}
	}
	return &Client{
		cluster: cluster,
		client:  newHTTPClient(options.withDefaults()),
	}
}

// Query runs the given query against the server with the given options.
// Pass nil for default options.
func (c *Client) Query(query PQLQuery, options *QueryOptions) (*QueryResponse, error) {
	if err := query.Error(); err != nil {
		return nil, err
	}
	if options == nil {
		options = &QueryOptions{}
	}
	data := makeRequestData(query.serialize(), options)
	path := fmt.Sprintf("/index/%s/query", query.Index().name)
	_, buf, err := c.httpRequest("POST", path, data, rawResponse)
	if err != nil {
		return nil, err
	}
	iqr := &internal.QueryResponse{}
	err = proto.Unmarshal(buf, iqr)
	if err != nil {
		return nil, err
	}
	queryResponse, err := newQueryResponseFromInternal(iqr)
	if err != nil {
		return nil, err
	}
	if !queryResponse.Success {
		return nil, NewError(queryResponse.ErrorMessage)
	}
	return queryResponse, nil
}

// CreateIndex creates an index on the server using the given Index struct.
func (c *Client) CreateIndex(index *Index) error {
	data := []byte(index.options.String())
	path := fmt.Sprintf("/index/%s", index.name)
	_, _, err := c.httpRequest("POST", path,
		data, noResponse)
	if err != nil {
		return err
	}
	if index.options.TimeQuantum != TimeQuantumNone {
		err = c.patchIndexTimeQuantum(index)
	}
	return err

}

// CreateFrame creates a frame on the server using the given Frame struct.
func (c *Client) CreateFrame(frame *Frame) error {
	data := []byte(frame.options.String())
	path := fmt.Sprintf("/index/%s/frame/%s", frame.index.name, frame.name)
	_, _, err := c.httpRequest("POST", path, data, noResponse)
	if err != nil {
		return err
	}
	if frame.options.TimeQuantum != TimeQuantumNone {
		err = c.patchFrameTimeQuantum(frame)
	}
	return err

}

// EnsureIndex creates an index on the server if it does not exist.
func (c *Client) EnsureIndex(index *Index) error {
	err := c.CreateIndex(index)
	if err == ErrorIndexExists {
		return nil
	}
	return err
}

// EnsureFrame creates a frame on the server if it doesn't exists.
func (c *Client) EnsureFrame(frame *Frame) error {
	err := c.CreateFrame(frame)
	if err == ErrorFrameExists {
		return nil
	}
	return err
}

// DeleteIndex deletes an index on the server.
func (c *Client) DeleteIndex(index *Index) error {
	path := fmt.Sprintf("/index/%s", index.name)
	_, _, err := c.httpRequest("DELETE", path, nil, noResponse)
	return err

}

// DeleteFrame deletes a frame on the server.
func (c *Client) DeleteFrame(frame *Frame) error {
	path := fmt.Sprintf("/index/%s/frame/%s", frame.index.name, frame.name)
	_, _, err := c.httpRequest("DELETE", path, nil, noResponse)
	return err
}

// SyncSchema updates a schema with the indexes and frames on the server and
// creates the indexes and frames in the schema on the server side.
// This function does not delete indexes and the frames on the server side nor in the schema.
func (c *Client) SyncSchema(schema *Schema) error {
	var err error
	serverSchema, err := c.Schema()
	if err != nil {
		return err
	}

	// find out local - remote schema
	diffSchema := schema.diff(serverSchema)
	// create the indexes and frames which doesn't exist on the server side
	for indexName, index := range diffSchema.indexes {
		if _, ok := serverSchema.indexes[indexName]; !ok {
			c.EnsureIndex(index)
		}
		for _, frame := range index.frames {
			c.EnsureFrame(frame)
		}
	}

	// find out remote - local schema
	diffSchema = serverSchema.diff(schema)
	for indexName, index := range diffSchema.indexes {
		if serverIndex, ok := schema.indexes[indexName]; !ok {
			schema.indexes[indexName] = index
		} else {
			for frameName, frame := range index.frames {
				serverIndex.frames[frameName] = frame
			}
		}
	}

	return nil
}

// Schema returns the indexes and frames on the server.
func (c *Client) Schema() (*Schema, error) {
	status, err := c.status()
	if err != nil {
		return nil, err
	}
	if len(status.Nodes) == 0 {
		return nil, errors.New("Status should contain at least 1 node")
	}
	schema := NewSchema()
	for _, indexInfo := range status.Nodes[0].Indexes {
		options := &IndexOptions{
			ColumnLabel: indexInfo.Meta.ColumnLabel,
			TimeQuantum: TimeQuantum(indexInfo.Meta.TimeQuantum),
		}
		index, err := schema.Index(indexInfo.Name, options)
		if err != nil {
			return nil, err
		}
		for _, frameInfo := range indexInfo.Frames {
			frameOptions := &FrameOptions{
				RowLabel:       frameInfo.Meta.RowLabel,
				CacheSize:      frameInfo.Meta.CacheSize,
				CacheType:      CacheType(frameInfo.Meta.CacheType),
				InverseEnabled: frameInfo.Meta.InverseEnabled,
				TimeQuantum:    TimeQuantum(frameInfo.Meta.TimeQuantum),
			}
			_, err := index.Frame(frameInfo.Name, frameOptions)
			if err != nil {
				return nil, err
			}
		}

	}
	return schema, nil
}

// ImportFrame imports bits from the given CSV iterator
func (c *Client) ImportFrame(frame *Frame, bitIterator BitIterator, batchSize uint) error {
	const sliceWidth = 1048576
	linesLeft := true
	bitGroup := map[uint64][]Bit{}
	var currentBatchSize uint
	indexName := frame.index.name
	frameName := frame.name

	for linesLeft {
		bit, err := bitIterator.NextBit()
		if err == io.EOF {
			linesLeft = false
		} else if err != nil {
			return err
		}

		slice := bit.ColumnID / sliceWidth
		if sliceArray, ok := bitGroup[slice]; ok {
			bitGroup[slice] = append(sliceArray, bit)
		} else {
			bitGroup[slice] = []Bit{bit}
		}
		currentBatchSize++
		// if the batch is full or there's no line left, start importing bits
		if currentBatchSize >= batchSize || !linesLeft {
			for slice, bits := range bitGroup {
				if len(bits) > 0 {
					err := c.importBits(indexName, frameName, slice, bits)
					if err != nil {
						return err
					}
				}
			}
			bitGroup = map[uint64][]Bit{}
			currentBatchSize = 0
		}
	}

	return nil
}

func (c *Client) importBits(indexName string, frameName string, slice uint64, bits []Bit) error {
	sort.Sort(bitsForSort(bits))
	nodes, err := c.fetchFragmentNodes(indexName, slice)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		uri, err := NewURIFromAddress(node.Host)
		if err != nil {
			return err
		}
		client := NewClientWithURI(uri)
		err = client.importNode(bitsToImportRequest(indexName, frameName, slice, bits))
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) fetchFragmentNodes(indexName string, slice uint64) ([]FragmentNode, error) {
	path := fmt.Sprintf("/fragment/nodes?slice=%d&index=%s", slice, indexName)
	_, body, err := c.httpRequest("GET", path, []byte{}, errorCheckedResponse)
	if err != nil {
		return nil, err
	}
	fragmentNodes := []FragmentNode{}
	err = json.Unmarshal(body, &fragmentNodes)
	if err != nil {
		return nil, err
	}
	return fragmentNodes, nil
}

func (c *Client) importNode(request *internal.ImportRequest) error {
	data, _ := proto.Marshal(request)
	// request.Marshal never returns an error
	_, _, err := c.httpRequest("POST", "/import", data, noResponse)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) patchIndexTimeQuantum(index *Index) error {
	data := []byte(fmt.Sprintf(`{"timeQuantum": "%s"}`, index.options.TimeQuantum))
	path := fmt.Sprintf("/index/%s/time-quantum", index.name)
	_, _, err := c.httpRequest("PATCH", path, data, noResponse)
	return err
}

func (c *Client) patchFrameTimeQuantum(frame *Frame) error {
	data := []byte(fmt.Sprintf(`{"index": "%s", "frame": "%s", "timeQuantum": "%s"}`,
		frame.index.name, frame.name, frame.options.TimeQuantum))
	path := fmt.Sprintf("/index/%s/frame/%s/time-quantum", frame.index.name, frame.name)
	_, _, err := c.httpRequest("PATCH", path, data, noResponse)
	return err
}

func (c *Client) status() (*Status, error) {
	_, data, err := c.httpRequest("GET", "/status", nil, errorCheckedResponse)
	if err != nil {
		return nil, err
	}
	statusRoot := &StatusRoot{}
	err = json.Unmarshal(data, statusRoot)
	if err != nil {
		return nil, err
	}
	return statusRoot.Status, nil
}

func (c *Client) httpRequest(method string, path string, data []byte, returnResponse returnClientInfo) (*http.Response, []byte, error) {
	addr := c.cluster.Host()
	if addr == nil {
		return nil, nil, ErrorEmptyCluster
	}
	if data == nil {
		data = []byte{}
	}
	request, err := http.NewRequest(method, addr.Normalize()+path, bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	// both Content-Type and Accept headers must be set for protobuf content
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Accept", "application/x-protobuf")
	response, err := c.client.Do(request)
	if err != nil {
		c.cluster.RemoveHost(addr)
		return nil, nil, err
	}
	defer response.Body.Close()
	// TODO: Optimize buffer creation
	buf, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, nil, err
	}
	if returnResponse == rawResponse {
		return response, buf, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		msg := string(buf)
		err = matchError(msg)
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, NewError(fmt.Sprintf("Server error (%d) %s: %s", response.StatusCode, response.Status, msg))
	}
	if returnResponse == noResponse {
		return nil, nil, nil
	}
	return response, buf, nil
}

func newHTTPClient(options *ClientOptions) *http.Client {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: options.ConnectTimeout,
		}).Dial,
		MaxIdleConnsPerHost: options.PoolSizePerRoute,
		MaxIdleConns:        options.TotalPoolSize,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   options.SocketTimeout,
	}
}

func makeRequestData(query string, options *QueryOptions) []byte {
	request := &internal.QueryRequest{
		Query:       query,
		ColumnAttrs: options.Columns,
	}
	r, _ := proto.Marshal(request)
	// request.Marshal never returns an error
	return r
}

func matchError(msg string) error {
	switch msg {
	case "index already exists\n":
		return ErrorIndexExists
	case "frame already exists\n":
		return ErrorFrameExists
	}
	return nil
}

func bitsToImportRequest(indexName string, frameName string, slice uint64, bits []Bit) *internal.ImportRequest {
	rowIDs := make([]uint64, 0, len(bits))
	columnIDs := make([]uint64, 0, len(bits))
	timestamps := make([]int64, 0, len(bits))
	for _, bit := range bits {
		rowIDs = append(rowIDs, bit.RowID)
		columnIDs = append(columnIDs, bit.ColumnID)
		timestamps = append(timestamps, bit.Timestamp)
	}
	return &internal.ImportRequest{
		Index:      indexName,
		Frame:      frameName,
		Slice:      slice,
		RowIDs:     rowIDs,
		ColumnIDs:  columnIDs,
		Timestamps: timestamps,
	}
}

// ClientOptions control the properties of client connection to the server
type ClientOptions struct {
	SocketTimeout    time.Duration
	ConnectTimeout   time.Duration
	PoolSizePerRoute int
	TotalPoolSize    int
}

func (options *ClientOptions) withDefaults() (updated *ClientOptions) {
	// copy options so the original is not updated
	updated = &ClientOptions{}
	*updated = *options
	// impose defaults
	if updated.SocketTimeout <= 0 {
		updated.SocketTimeout = time.Second * 300
	}
	if updated.ConnectTimeout <= 0 {
		updated.ConnectTimeout = time.Second * 30
	}
	if updated.PoolSizePerRoute <= 0 {
		updated.PoolSizePerRoute = 10
	}
	if updated.TotalPoolSize <= 100 {
		updated.TotalPoolSize = 100
	}
	return
}

// QueryOptions contains options to customize the Query function.
type QueryOptions struct {
	// Columns enables returning columns in the query response.
	Columns bool
}

type returnClientInfo uint

const (
	noResponse returnClientInfo = iota
	rawResponse
	errorCheckedResponse
)

type FragmentNode struct {
	Host         string
	InternalHost string
}

type StatusRoot struct {
	Status *Status `json:"status"`
}

type Status struct {
	Nodes []StatusNode
}

type StatusNode struct {
	Host    string
	Indexes []StatusIndex
}

type StatusIndex struct {
	Name   string
	Meta   StatusMeta
	Frames []StatusFrame
	Slices []uint64
}

type StatusFrame struct {
	Name string
	Meta StatusMeta
}

type StatusMeta struct {
	ColumnLabel    string
	RowLabel       string
	CacheType      string
	CacheSize      uint
	InverseEnabled bool
	TimeQuantum    string
}
