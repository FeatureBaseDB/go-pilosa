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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	pbuf "github.com/pilosa/go-pilosa/gopilosa_pbuf"
	"github.com/pkg/errors"
)

const maxHosts = 10
const sliceWidth = 1048576

// // both Content-Type and Accept headers must be set for protobuf content
var protobufHeaders = map[string]string{
	"Content-Type": "application/x-protobuf",
	"Accept":       "application/x-protobuf",
}

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

// NewClientFromAddresses creates a client for a cluster specified by `hosts`. Each
// string in `hosts` is the string representation of a URI. E.G
// node0.pilosa.com:10101
func NewClientFromAddresses(addresses []string, options *ClientOptions) (*Client, error) {
	uris := make([]*URI, len(addresses))
	for i, address := range addresses {
		uri, err := NewURIFromAddress(address)
		if err != nil {
			return nil, err
		}
		uris[i] = uri
	}
	cluster := NewClusterWithHost(uris...)
	client := NewClientWithCluster(cluster, options)
	return client, nil
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
	data, err := makeRequestData(query.serialize(), options)
	if err != nil {
		return nil, errors.Wrap(err, "making request data")
	}
	path := fmt.Sprintf("/index/%s/query", query.Index().name)
	_, buf, err := c.httpRequest("POST", path, data, protobufHeaders, rawResponse)
	if err != nil {
		return nil, err
	}
	iqr := &pbuf.QueryResponse{}
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
	_, _, err := c.httpRequest("POST", path, data, nil, noResponse)
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
	_, _, err := c.httpRequest("POST", path, data, nil, noResponse)
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
	_, _, err := c.httpRequest("DELETE", path, nil, nil, noResponse)
	return err

}

// CreateIntField creates an integer range field.
// *Experimental*: This feature may be removed or its interface may be modified in the future.
func (c *Client) CreateIntField(frame *Frame, name string, min int, max int) error {
	// TODO: refactor the code below when we have more fields types
	field, err := newIntRangeField(name, min, max)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/index/%s/frame/%s/field/%s",
		frame.index.name, frame.name, name)
	data := []byte(encodeMap(field))
	_, _, err = c.httpRequest("POST", path, data, nil, noResponse)
	if err != nil {
		return err
	}
	return nil
}

// DeleteField delete a range field.
// *Experimental*: This feature may be removed or its interface may be modified in the future.
func (c *Client) DeleteField(frame *Frame, name string) error {
	path := fmt.Sprintf("/index/%s/frame/%s/field/%s",
		frame.index.name, frame.name, name)
	_, _, err := c.httpRequest("DELETE", path, nil, nil, noResponse)
	if err != nil {
		return err
	}
	return nil
}

// DeleteFrame deletes a frame on the server.
func (c *Client) DeleteFrame(frame *Frame) error {
	path := fmt.Sprintf("/index/%s/frame/%s", frame.index.name, frame.name)
	_, _, err := c.httpRequest("DELETE", path, nil, nil, noResponse)
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
		if localIndex, ok := schema.indexes[indexName]; !ok {
			schema.indexes[indexName] = index
		} else {
			for frameName, frame := range index.frames {
				localIndex.frames[frameName] = frame
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
			fields := make(map[string]rangeField)
			frameOptions := &FrameOptions{
				RowLabel:       frameInfo.Meta.RowLabel,
				CacheSize:      frameInfo.Meta.CacheSize,
				CacheType:      CacheType(frameInfo.Meta.CacheType),
				InverseEnabled: frameInfo.Meta.InverseEnabled,
				TimeQuantum:    TimeQuantum(frameInfo.Meta.TimeQuantum),
				RangeEnabled:   frameInfo.Meta.RangeEnabled,
			}
			for _, fieldInfo := range frameInfo.Meta.Fields {
				fields[fieldInfo.Name] = map[string]interface{}{
					"Name": fieldInfo.Name,
					"Type": fieldInfo.Type,
					"Max":  fieldInfo.Max,
					"Min":  fieldInfo.Min,
				}
			}
			frameOptions.fields = fields
			fram, err := index.Frame(frameInfo.Name, frameOptions)
			if err != nil {
				return nil, err
			}
			for name, _ := range fields {
				ff := fram.Field(name)
				if ff.err != nil {
					return nil, errors.Wrap(err, "fielding frame")
				}
			}
		}
	}
	return schema, nil
}

// ImportFrame imports bits from the given CSV iterator.
func (c *Client) ImportFrame(frame *Frame, bitIterator BitIterator, batchSize uint) error {
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

// ImportValueFrame imports field values from the given CSV iterator.
func (c *Client) ImportValueFrame(frame *Frame, field string, valueIterator ValueIterator, batchSize uint) error {
	linesLeft := true
	valGroup := map[uint64][]FieldValue{}
	var currentBatchSize uint
	indexName := frame.index.name
	frameName := frame.name
	fieldName := field

	for linesLeft {
		val, err := valueIterator.NextValue()
		if err == io.EOF {
			linesLeft = false
		} else if err != nil {
			return err
		}

		slice := val.ColumnID / sliceWidth
		if sliceArray, ok := valGroup[slice]; ok {
			valGroup[slice] = append(sliceArray, val)
		} else {
			valGroup[slice] = []FieldValue{val}
		}
		currentBatchSize++
		// if the batch is full or there's no line left, start importing values
		if currentBatchSize >= batchSize || !linesLeft {
			for slice, vals := range valGroup {
				if len(vals) > 0 {
					err := c.importValues(indexName, frameName, slice, fieldName, vals)
					if err != nil {
						return err
					}
				}
			}
			valGroup = map[uint64][]FieldValue{}
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
		uri.SetScheme(node.Scheme)
		err = c.importNode(uri, bitsToImportRequest(indexName, frameName, slice, bits))
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) importValues(indexName string, frameName string, slice uint64, fieldName string, vals []FieldValue) error {
	sort.Sort(valsForSort(vals))
	nodes, err := c.fetchFragmentNodes(indexName, slice)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		uri, err := NewURIFromAddress(node.Host)
		if err != nil {
			return err
		}
		uri.SetScheme(node.Scheme)
		err = c.importValueNode(uri, valsToImportRequest(indexName, frameName, slice, fieldName, vals))
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) fetchFragmentNodes(indexName string, slice uint64) ([]fragmentNode, error) {
	path := fmt.Sprintf("/fragment/nodes?slice=%d&index=%s", slice, indexName)
	_, body, err := c.httpRequest("GET", path, []byte{}, nil, errorCheckedResponse)
	if err != nil {
		return nil, err
	}
	fragmentNodes := []fragmentNode{}
	err = json.Unmarshal(body, &fragmentNodes)
	if err != nil {
		return nil, err
	}
	return fragmentNodes, nil
}

func (c *Client) importNode(uri *URI, request *pbuf.ImportRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return errors.Wrap(err, "marshaling to protobuf")
	}
	resp, err := c.doRequest(uri, "POST", "/import", protobufHeaders, bytes.NewReader(data))
	if err = anyError(resp, err); err != nil {
		return errors.Wrap(err, "doing import request")
	}
	return errors.Wrap(resp.Body.Close(), "closing import response body")
}

func (c *Client) importValueNode(uri *URI, request *pbuf.ImportValueRequest) error {
	data, _ := proto.Marshal(request)
	// request.Marshal never returns an error
	_, err := c.doRequest(uri, "POST", "/import-value", protobufHeaders, bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "doing /import-value request")
	}

	return nil
}

// ExportFrame exports bits for a frame.
func (c *Client) ExportFrame(frame *Frame, view string) (BitIterator, error) {
	status, err := c.status()
	if err != nil {
		return nil, err
	}
	sliceURIs := c.statusToNodeSlicesForIndex(status, frame.index.Name())
	return NewCSVBitIterator(newExportReader(c, sliceURIs, frame, view)), nil
}

// Views fetches and returns the views of a frame
func (c *Client) Views(frame *Frame) ([]string, error) {
	path := fmt.Sprintf("/index/%s/frame/%s/views", frame.index.name, frame.name)
	_, body, err := c.httpRequest("GET", path, nil, nil, errorCheckedResponse)
	if err != nil {
		return nil, err
	}
	viewsInfo := viewsInfo{}
	err = json.Unmarshal(body, &viewsInfo)
	if err != nil {
		return nil, err
	}
	return viewsInfo.Views, nil
}

func (c *Client) patchIndexTimeQuantum(index *Index) error {
	data := []byte(fmt.Sprintf(`{"timeQuantum": "%s"}`, index.options.TimeQuantum))
	path := fmt.Sprintf("/index/%s/time-quantum", index.name)
	_, _, err := c.httpRequest("PATCH", path, data, nil, noResponse)
	return err
}

func (c *Client) patchFrameTimeQuantum(frame *Frame) error {
	data := []byte(fmt.Sprintf(`{"index": "%s", "frame": "%s", "timeQuantum": "%s"}`,
		frame.index.name, frame.name, frame.options.TimeQuantum))
	path := fmt.Sprintf("/index/%s/frame/%s/time-quantum", frame.index.name, frame.name)
	_, _, err := c.httpRequest("PATCH", path, data, nil, noResponse)
	return err
}

func (c *Client) status() (*Status, error) {
	_, data, err := c.httpRequest("GET", "/status", nil, nil, errorCheckedResponse)
	if err != nil {
		return nil, errors.Wrap(err, "requesting /status")
	}
	root := &statusRoot{}
	err = json.Unmarshal(data, root)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling /status data")
	}
	return root.Status, nil
}

// HttpGet sends an HTTP request to the Pilosa server.
// **NOTE**: This function is experimental and may be removed in later revisions.
func (c *Client) HttpRequest(method string, path string, data []byte, headers map[string]string) (*http.Response, []byte, error) {
	return c.httpRequest(method, path, data, headers, rawResponse)
}

// httpRequest makes a request to the cluster - use this when you want the
// client to choose a host, and it doesn't matter if the request goes to a
// specific host
func (c *Client) httpRequest(method string, path string, data []byte, headers map[string]string, returnResponse returnClientInfo) (*http.Response, []byte, error) {
	if data == nil {
		data = []byte{}
	}

	// try at most maxHosts non-failed hosts; protect against broken cluster.removeHost
	var response *http.Response
	var err error
	for i := 0; i < maxHosts; i++ {
		reader := bytes.NewReader(data)
		// get a host from the cluster
		host := c.cluster.Host()
		if host == nil {
			return nil, nil, ErrorEmptyCluster
		}

		response, err = c.doRequest(host, method, path, headers, reader)
		if err == nil {
			break
		}
		c.cluster.RemoveHost(host)
	}
	if response == nil {
		return nil, nil, ErrorTriedMaxHosts
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

// anyError checks an http Response and error to see if anything went wrong with
// a request (either locally, or on the server) and returns a single error if
// so.
func anyError(resp *http.Response, err error) error {
	if err != nil {
		return errors.Wrap(err, "unable to perform request")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrapf(err, "bad status '%s' and err reading body", resp.Status)
		}
		msg := string(buf)
		err = matchError(msg)
		if err != nil {
			return err
		}
		return errors.Errorf("Server error %s body:'%s'", resp.Status, msg)
	}
	return nil
}

// doRequest creates and performs an http request.
func (c *Client) doRequest(host *URI, method, path string, headers map[string]string, reader io.Reader) (*http.Response, error) {
	req, err := makeRequest(host, method, path, headers, reader)
	if err != nil {
		return nil, errors.Wrap(err, "building request")
	}
	return c.client.Do(req)
}

// statusToNodeSlicesForIndex finds the hosts which contains slices for the given index
func (c *Client) statusToNodeSlicesForIndex(status *Status, indexName string) map[uint64]*URI {
	result := make(map[uint64]*URI)
	for _, node := range status.Nodes {
		for _, index := range node.Indexes {
			if index.Name != indexName {
				continue
			}
			for _, slice := range index.Slices {
				uri, err := NewURIFromAddress(node.Host)
				// err will always be nil, but prevent a panic in the odd chance the server returns an invalid URI
				if err == nil {
					uri.SetScheme(node.Scheme)
					result[slice] = uri
				}
			}
			break
		}
	}
	return result
}

func makeRequest(host *URI, method, path string, headers map[string]string, reader io.Reader) (*http.Request, error) {
	request, err := http.NewRequest(method, host.Normalize()+path, reader)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		request.Header.Set(k, v)
	}

	return request, nil
}

func newHTTPClient(options *ClientOptions) *http.Client {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: options.ConnectTimeout,
		}).Dial,
		TLSClientConfig:     options.TLSConfig,
		MaxIdleConnsPerHost: options.PoolSizePerRoute,
		MaxIdleConns:        options.TotalPoolSize,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   options.SocketTimeout,
	}
}

func makeRequestData(query string, options *QueryOptions) ([]byte, error) {
	request := &pbuf.QueryRequest{
		Query:        query,
		ColumnAttrs:  options.Columns,
		ExcludeAttrs: options.ExcludeAttrs,
		ExcludeBits:  options.ExcludeBits,
	}
	r, err := proto.Marshal(request)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling request to protobuf")
	}
	return r, nil
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

func bitsToImportRequest(indexName string, frameName string, slice uint64, bits []Bit) *pbuf.ImportRequest {
	rowIDs := make([]uint64, 0, len(bits))
	columnIDs := make([]uint64, 0, len(bits))
	timestamps := make([]int64, 0, len(bits))
	for _, bit := range bits {
		rowIDs = append(rowIDs, bit.RowID)
		columnIDs = append(columnIDs, bit.ColumnID)
		timestamps = append(timestamps, bit.Timestamp)
	}
	return &pbuf.ImportRequest{
		Index:      indexName,
		Frame:      frameName,
		Slice:      slice,
		RowIDs:     rowIDs,
		ColumnIDs:  columnIDs,
		Timestamps: timestamps,
	}
}

func valsToImportRequest(indexName string, frameName string, slice uint64, fieldName string, vals []FieldValue) *pbuf.ImportValueRequest {
	columnIDs := make([]uint64, 0, len(vals))
	values := make([]uint64, 0, len(vals))
	for _, val := range vals {
		columnIDs = append(columnIDs, val.ColumnID)
		values = append(values, val.Value)
	}
	return &pbuf.ImportValueRequest{
		Index:     indexName,
		Frame:     frameName,
		Slice:     slice,
		Field:     fieldName,
		ColumnIDs: columnIDs,
		Values:    values,
	}
}

// ClientOptions control the properties of client connection to the server
type ClientOptions struct {
	SocketTimeout    time.Duration
	ConnectTimeout   time.Duration
	PoolSizePerRoute int
	TotalPoolSize    int
	TLSConfig        *tls.Config
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
		updated.ConnectTimeout = time.Second * 60
	}
	if updated.PoolSizePerRoute <= 0 {
		updated.PoolSizePerRoute = 10
	}
	if updated.TotalPoolSize <= 100 {
		updated.TotalPoolSize = 100
	}
	if updated.TLSConfig == nil {
		updated.TLSConfig = &tls.Config{}
	}
	return
}

// QueryOptions contains options to customize the Query function.
type QueryOptions struct {
	// Columns enables returning columns in the query response.
	Columns bool
	// ExcludeAttrs inhibits returning attributes
	ExcludeAttrs bool
	// ExcludeBits inhibits returning bits
	ExcludeBits bool
}

type returnClientInfo uint

const (
	noResponse returnClientInfo = iota
	rawResponse
	errorCheckedResponse
)

type fragmentNode struct {
	Scheme       string
	Host         string
	InternalHost string
}

type statusRoot struct {
	Status *Status `json:"status"`
}

// Status contains the status information from a Pilosa server.
type Status struct {
	Nodes []StatusNode
}

// StatusNode contains node information.
type StatusNode struct {
	Scheme  string
	Host    string
	Indexes []StatusIndex
}

// StatusIndex contains index information.
type StatusIndex struct {
	Name   string
	Meta   StatusMeta
	Frames []StatusFrame
	Slices []uint64
}

// StatusFrame contains frame information.
type StatusFrame struct {
	Name string
	Meta StatusMeta
}

// StatusMeta contains options for a frame or an index.
type StatusMeta struct {
	ColumnLabel    string
	RowLabel       string
	CacheType      string
	CacheSize      uint
	InverseEnabled bool
	RangeEnabled   bool
	Fields         []StatusField
	TimeQuantum    string
}

type StatusField struct {
	Name string
	Type string
	Max  int64
	Min  int64
}

type viewsInfo struct {
	Views []string `json:"views"`
}

type exportReader struct {
	client       *Client
	sliceURIs    map[uint64]*URI
	frame        *Frame
	body         []byte
	bodyIndex    int
	currentSlice uint64
	sliceCount   uint64
	view         string
}

func newExportReader(client *Client, sliceURIs map[uint64]*URI, frame *Frame, view string) *exportReader {
	return &exportReader{
		client:     client,
		sliceURIs:  sliceURIs,
		frame:      frame,
		sliceCount: uint64(len(sliceURIs)),
		view:       view,
	}
}

// Read updates the passed array with the exported CSV data and returns the number of bytes read
func (r *exportReader) Read(p []byte) (n int, err error) {
	if r.currentSlice >= r.sliceCount {
		err = io.EOF
		return
	}
	if r.body == nil {
		uri, _ := r.sliceURIs[r.currentSlice]
		headers := map[string]string{
			"Accept": "text/csv",
		}
		path := fmt.Sprintf("/export?index=%s&frame=%s&slice=%d&view=%s",
			r.frame.index.Name(), r.frame.Name(), r.currentSlice, r.view)
		resp, err := r.client.doRequest(uri, "GET", path, headers, nil)
		if err = anyError(resp, err); err != nil {
			return 0, errors.Wrap(err, "doing export request")
		}
		defer resp.Body.Close()
		r.body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return 0, errors.Wrap(err, "reading response body")
		}
		r.bodyIndex = 0
	}
	n = copy(p, r.body[r.bodyIndex:])
	r.bodyIndex += n
	if n >= len(r.body) {
		r.body = nil
		r.currentSlice++
	}
	return
}
