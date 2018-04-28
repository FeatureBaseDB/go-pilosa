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
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	pbuf "github.com/pilosa/go-pilosa/gopilosa_pbuf"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const maxHosts = 10
const sliceWidth = 1048576
const pilosaMinVersion = ">=0.9.0"

// Client is the HTTP client for Pilosa server.
type Client struct {
	cluster *Cluster
	client  *http.Client
	// User-Agent header cache. Not used until cluster-resize support branch is merged
	// and user agent is saved here in NewClient
	userAgent string
}

// DefaultClient creates a client with the default address and options.
func DefaultClient() *Client {
	return NewClientWithURI(DefaultURI())
}

// NewClientWithURI creates a client with the given server address.
// **DEPRECATED** Use `NewClient(uri)` instead.
func NewClientWithURI(uri *URI) *Client {
	return NewClientWithCluster(NewClusterWithHost(uri), nil)
}

// NewClientFromAddresses creates a client for a cluster specified by `hosts`. Each
// string in `hosts` is the string representation of a URI. E.G
// node0.pilosa.com:10101
// **DEPRECATED** Use `NewClient([]string{address1, address2, ...}, option1, option2, ...)` instead.
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
// **DEPRECATED** Use `NewClient(cluster, option1, option2, ...)` instead.
func NewClientWithCluster(cluster *Cluster, options *ClientOptions) *Client {
	if options == nil {
		options = &ClientOptions{}
	}
	return &Client{
		cluster: cluster,
		client:  newHTTPClient(options.withDefaults()),
	}
}

// NewClient creates a client with the given address, URI, or cluster and options.
func NewClient(addrUriOrCluster interface{}, options ...ClientOption) (*Client, error) {
	var cluster *Cluster
	clientOptions := &ClientOptions{}
	err := clientOptions.addOptions(options...)
	if err != nil {
		return nil, err
	}

	switch u := addrUriOrCluster.(type) {
	case string:
		uri, err := NewURIFromAddress(u)
		if err != nil {
			return nil, err
		}
		cluster = NewClusterWithHost(uri)
	case []string:
		return NewClientFromAddresses(u, clientOptions)
	case *URI:
		cluster = NewClusterWithHost(u)
	case []*URI:
		cluster = NewClusterWithHost(u...)
	case *Cluster:
		cluster = u
	case nil:
		cluster = NewClusterWithHost()
	default:
		return nil, ErrAddrURIClusterExpected
	}

	return NewClientWithCluster(cluster, clientOptions), nil
}

// Query runs the given query against the server with the given options.
// Pass nil for default options.
func (c *Client) Query(query PQLQuery, options ...interface{}) (*QueryResponse, error) {
	if err := query.Error(); err != nil {
		return nil, err
	}
	queryOptions := &QueryOptions{}
	err := queryOptions.addOptions(options...)
	if err != nil {
		return nil, err
	}
	data, err := makeRequestData(query.serialize(), queryOptions)
	if err != nil {
		return nil, errors.Wrap(err, "making request data")
	}
	path := fmt.Sprintf("/index/%s/query", query.Index().name)
	_, buf, err := c.httpRequest("POST", path, data, defaultProtobufHeaders())
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
	return queryResponse, nil
}

// CreateIndex creates an index on the server using the given Index struct.
func (c *Client) CreateIndex(index *Index) error {
	data := []byte("")
	path := fmt.Sprintf("/index/%s", index.name)
	response, _, err := c.httpRequest("POST", path, data, nil)
	if err != nil {
		if response != nil && response.StatusCode == 409 {
			return ErrIndexExists
		}
		return err
	}
	return nil

}

// CreateFrame creates a frame on the server using the given Frame struct.
func (c *Client) CreateFrame(frame *Frame) error {
	data := []byte(frame.options.String())
	path := fmt.Sprintf("/index/%s/frame/%s", frame.index.name, frame.name)
	response, _, err := c.httpRequest("POST", path, data, nil)
	if err != nil {
		if response != nil && response.StatusCode == 409 {
			return ErrFrameExists
		}
		return err
	}
	return nil
}

// EnsureIndex creates an index on the server if it does not exist.
func (c *Client) EnsureIndex(index *Index) error {
	err := c.CreateIndex(index)
	if err == ErrIndexExists {
		return nil
	}
	return err
}

// EnsureFrame creates a frame on the server if it doesn't exists.
func (c *Client) EnsureFrame(frame *Frame) error {
	err := c.CreateFrame(frame)
	if err == ErrFrameExists {
		return nil
	}
	return err
}

// DeleteIndex deletes an index on the server.
func (c *Client) DeleteIndex(index *Index) error {
	path := fmt.Sprintf("/index/%s", index.name)
	_, _, err := c.httpRequest("DELETE", path, nil, nil)
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
	_, _, err = c.httpRequest("POST", path, data, nil)
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
	_, _, err := c.httpRequest("DELETE", path, nil, nil)
	if err != nil {
		return err
	}
	return nil
}

// DeleteFrame deletes a frame on the server.
func (c *Client) DeleteFrame(frame *Frame) error {
	path := fmt.Sprintf("/index/%s/frame/%s", frame.index.name, frame.name)
	_, _, err := c.httpRequest("DELETE", path, nil, nil)
	return err
}

// SyncSchema updates a schema with the indexes and frames on the server and
// creates the indexes and frames in the schema on the server side.
// This function does not delete indexes and the frames on the server side nor in the schema.
func (c *Client) SyncSchema(schema *Schema) error {
	serverSchema, err := c.Schema()
	if err != nil {
		return err
	}

	return c.syncSchema(schema, serverSchema)
}

func (c *Client) syncSchema(schema *Schema, serverSchema *Schema) error {
	var err error

	// find out local - remote schema
	diffSchema := schema.diff(serverSchema)
	// create the indexes and frames which doesn't exist on the server side
	for indexName, index := range diffSchema.indexes {
		if _, ok := serverSchema.indexes[indexName]; !ok {
			err = c.EnsureIndex(index)
			if err != nil {
				return err
			}
		}
		for _, frame := range index.frames {
			err = c.EnsureFrame(frame)
			if err != nil {
				return err
			}
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
	var indexes []StatusIndex
	indexes, err := c.readSchema()
	if err != nil {
		return nil, err
	}
	schema := NewSchema()
	for _, indexInfo := range indexes {
		index, err := schema.Index(indexInfo.Name)
		if err != nil {
			return nil, err
		}
		for _, frameInfo := range indexInfo.Frames {
			fields := make(map[string]rangeField)
			frameOptions := &FrameOptions{
				CacheSize:      frameInfo.Options.CacheSize,
				CacheType:      CacheType(frameInfo.Options.CacheType),
				InverseEnabled: frameInfo.Options.InverseEnabled,
				TimeQuantum:    TimeQuantum(frameInfo.Options.TimeQuantum),
				RangeEnabled:   frameInfo.Options.RangeEnabled,
			}
			for _, fieldInfo := range frameInfo.Options.Fields {
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
			for name := range fields {
				ff := fram.Field(name)
				if ff.err != nil {
					return nil, errors.Wrap(ff.err, "fielding frame")
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
		} else {
			slice := bit.ColumnID / sliceWidth
			if sliceArray, ok := bitGroup[slice]; ok {
				bitGroup[slice] = append(sliceArray, bit)
			} else {
				bitGroup[slice] = []Bit{bit}
			}
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

// ImportValueFrame imports field values from the given iterator.
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
		} else {
			slice := val.ColumnID / sliceWidth
			if sliceArray, ok := valGroup[slice]; ok {
				valGroup[slice] = append(sliceArray, val)
			} else {
				valGroup[slice] = []FieldValue{val}
			}
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
		return errors.Wrap(err, "fetching fragment nodes")
	}

	eg := errgroup.Group{}
	for _, node := range nodes {
		uri := &URI{
			scheme: node.Scheme,
			host:   node.Host,
			port:   node.Port,
		}
		eg.Go(func() error {
			return c.importNode(uri, bitsToImportRequest(indexName, frameName, slice, bits))
		})
	}
	err = eg.Wait()
	return errors.Wrap(err, "importing to nodes")
}

func (c *Client) importValues(indexName string, frameName string, slice uint64, fieldName string, vals []FieldValue) error {
	sort.Sort(valsForSort(vals))
	nodes, err := c.fetchFragmentNodes(indexName, slice)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		uri := &URI{
			scheme: node.Scheme,
			host:   node.Host,
			port:   node.Port,
		}
		err = c.importValueNode(uri, valsToImportRequest(indexName, frameName, slice, fieldName, vals))
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) fetchFragmentNodes(indexName string, slice uint64) ([]fragmentNode, error) {
	path := fmt.Sprintf("/fragment/nodes?slice=%d&index=%s", slice, indexName)
	_, body, err := c.httpRequest("GET", path, []byte{}, nil)
	if err != nil {
		return nil, err
	}
	fragmentNodes := []fragmentNode{}
	var fragmentNodeURIs []fragmentNodeRoot
	err = json.Unmarshal(body, &fragmentNodeURIs)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling fragment node URIs")
	}
	for _, nodeURI := range fragmentNodeURIs {
		fragmentNodes = append(fragmentNodes, nodeURI.URI)
	}
	return fragmentNodes, nil
}

func (c *Client) importNode(uri *URI, request *pbuf.ImportRequest) error {
	data, err := proto.Marshal(request)
	if err != nil {
		return errors.Wrap(err, "marshaling to protobuf")
	}
	resp, err := c.doRequest(uri, "POST", "/import", defaultProtobufHeaders(), bytes.NewReader(data))
	if err = anyError(resp, err); err != nil {
		return errors.Wrap(err, "doing import request")
	}
	return errors.Wrap(resp.Body.Close(), "closing import response body")
}

func (c *Client) importValueNode(uri *URI, request *pbuf.ImportValueRequest) error {
	data, _ := proto.Marshal(request)
	// request.Marshal never returns an error
	_, err := c.doRequest(uri, "POST", "/import-value", defaultProtobufHeaders(), bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "doing /import-value request")
	}

	return nil
}

// ExportFrame exports bits for a frame.
func (c *Client) ExportFrame(frame *Frame, view string) (BitIterator, error) {
	var slicesMax map[string]uint64
	var err error

	status, err := c.Status()
	if err != nil {
		return nil, err
	}
	slicesMax, err = c.slicesMax()
	if err != nil {
		return nil, err
	}
	status.indexMaxSlice = slicesMax
	sliceURIs, err := c.statusToNodeSlicesForIndex(status, frame.index.Name())
	if err != nil {
		return nil, err
	}
	return NewCSVBitIterator(newExportReader(c, sliceURIs, frame, view)), nil
}

// Views fetches and returns the views of a frame
func (c *Client) Views(frame *Frame) ([]string, error) {
	path := fmt.Sprintf("/index/%s/frame/%s/views", frame.index.name, frame.name)
	_, body, err := c.httpRequest("GET", path, nil, nil)
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

// Status returns the serves status.
func (c *Client) Status() (Status, error) {
	_, data, err := c.httpRequest("GET", "/status", nil, nil)
	if err != nil {
		return Status{}, errors.Wrap(err, "requesting /status")
	}
	status := Status{}
	err = json.Unmarshal(data, &status)
	if err != nil {
		return Status{}, errors.Wrap(err, "unmarshaling /status data")
	}
	return status, nil
}

func (c *Client) readSchema() ([]StatusIndex, error) {
	_, data, err := c.httpRequest("GET", "/schema", nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "requesting /schema")
	}
	schemaInfo := SchemaInfo{}
	err = json.Unmarshal(data, &schemaInfo)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling /schema data")
	}
	return schemaInfo.Indexes, nil
}

func (c *Client) slicesMax() (map[string]uint64, error) {
	_, data, err := c.httpRequest("GET", "/slices/max", nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "requesting /slices/max")
	}
	m := map[string]map[string]uint64{}
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling /slices/max data")
	}
	return m["standard"], nil
}

// HttpRequest sends an HTTP request to the Pilosa server.
// **NOTE**: This function is experimental and may be removed in later revisions.
func (c *Client) HttpRequest(method string, path string, data []byte, headers map[string]string) (*http.Response, []byte, error) {
	return c.httpRequest(method, path, data, headers)
}

// httpRequest makes a request to the cluster - use this when you want the
// client to choose a host, and it doesn't matter if the request goes to a
// specific host
func (c *Client) httpRequest(method string, path string, data []byte, headers map[string]string) (*http.Response, []byte, error) {
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
			return nil, nil, ErrEmptyCluster
		}

		response, err = c.doRequest(host, method, path, c.augmentHeaders(headers), reader)
		if err == nil {
			break
		}
		c.cluster.RemoveHost(host)
	}
	if response == nil {
		return nil, nil, ErrTriedMaxHosts
	}
	defer response.Body.Close()
	// TODO: Optimize buffer creation
	buf, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		err := NewError(fmt.Sprintf("Server error (%d) %s: %s", response.StatusCode, response.Status, string(buf)))
		return response, buf, err
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
func (c *Client) statusToNodeSlicesForIndex(status Status, indexName string) (map[uint64]*URI, error) {
	result := make(map[uint64]*URI)
	if maxSlice, ok := status.indexMaxSlice[indexName]; ok {
		for slice := 0; slice <= int(maxSlice); slice++ {
			fragmentNodes, err := c.fetchFragmentNodes(indexName, uint64(slice))
			if err != nil {
				return nil, err
			}
			if len(fragmentNodes) == 0 {
				return nil, ErrNoFragmentNodes
			}
			node := fragmentNodes[0]
			uri := &URI{
				host:   node.Host,
				port:   node.Port,
				scheme: node.Scheme,
			}

			result[uint64(slice)] = uri
		}
	} else {
		return nil, ErrNoSlice
	}
	return result, nil
}

func (c *Client) augmentHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		headers = map[string]string{}
	}

	// TODO: move the following block to NewClient once cluster-resize support branch is merged.
	version := Version
	if strings.HasPrefix(version, "v") {
		version = version[1:]
	}

	headers["User-Agent"] = fmt.Sprintf("go-pilosa/%s", version)
	return headers
}

func defaultProtobufHeaders() map[string]string {
	return map[string]string{
		"Content-Type": "application/x-protobuf",
		"Accept":       "application/x-protobuf",
	}
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
		Slices:       options.Slices,
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
	values := make([]int64, 0, len(vals))
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

// ClientOptions control the properties of client connection to the server.
type ClientOptions struct {
	SocketTimeout    time.Duration
	ConnectTimeout   time.Duration
	PoolSizePerRoute int
	TotalPoolSize    int
	TLSConfig        *tls.Config
}

func (co *ClientOptions) addOptions(options ...ClientOption) error {
	for _, option := range options {
		err := option(co)
		if err != nil {
			return err
		}
	}
	return nil
}

// ClientOption is used when creating a PilosaClient struct.
type ClientOption func(options *ClientOptions) error

// SocketTimeout is the maximum idle socket time in nanoseconds
func SocketTimeout(timeout time.Duration) ClientOption {
	return func(options *ClientOptions) error {
		options.SocketTimeout = timeout
		return nil
	}
}

// ConnectTimeout is the maximum time to connect in nanoseconds.
func ConnectTimeout(timeout time.Duration) ClientOption {
	return func(options *ClientOptions) error {
		options.ConnectTimeout = timeout
		return nil
	}
}

// PoolSizePerRoute is the maximum number of active connections in the pool to a host.
func PoolSizePerRoute(size int) ClientOption {
	return func(options *ClientOptions) error {
		options.PoolSizePerRoute = size
		return nil
	}
}

// TotalPoolSize is the maximum number of connections in the pool.
func TotalPoolSize(size int) ClientOption {
	return func(options *ClientOptions) error {
		options.TotalPoolSize = size
		return nil
	}
}

// TLSConfig contains the TLS configuration.
func TLSConfig(config *tls.Config) ClientOption {
	return func(options *ClientOptions) error {
		options.TLSConfig = config
		return nil
	}
}

func (co *ClientOptions) withDefaults() (updated *ClientOptions) {
	// copy options so the original is not updated
	updated = &ClientOptions{}
	*updated = *co
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
	if updated.TotalPoolSize <= 0 {
		updated.TotalPoolSize = 100
	}
	if updated.TLSConfig == nil {
		updated.TLSConfig = &tls.Config{}
	}
	return
}

// QueryOptions contains options to customize the Query function.
type QueryOptions struct {
	// Slices restricts query to a subset of slices. Queries all slices if nil.
	Slices []uint64
	// Columns enables returning columns in the query response.
	Columns bool
	// ExcludeAttrs inhibits returning attributes
	ExcludeAttrs bool
	// ExcludeBits inhibits returning bits
	ExcludeBits bool
}

func (qo *QueryOptions) addOptions(options ...interface{}) error {
	for i, option := range options {
		switch o := option.(type) {
		case nil:
			if i != 0 {
				return ErrInvalidQueryOption
			}
			continue
		case *QueryOptions:
			if i != 0 {
				return ErrInvalidQueryOption
			}
			*qo = *o
		case QueryOption:
			err := o(qo)
			if err != nil {
				return err
			}
		default:
			return ErrInvalidQueryOption
		}
	}
	return nil
}

// QueryOption is used when using options with a client.Query,
type QueryOption func(options *QueryOptions) error

// ColumnAttrs enables returning column attributes in the result.
func ColumnAttrs(enable bool) QueryOption {
	return func(options *QueryOptions) error {
		options.Columns = enable
		return nil
	}
}

// Slices restricts the set of slices on which a query operates.
func Slices(slices ...uint64) QueryOption {
	return func(options *QueryOptions) error {
		options.Slices = append(options.Slices, slices...)
		return nil
	}
}

// ExcludeAttrs enables discarding attributes from a result,
func ExcludeAttrs(enable bool) QueryOption {
	return func(options *QueryOptions) error {
		options.ExcludeAttrs = enable
		return nil
	}
}

// ExcludeBits enables discarding bits from a result,
func ExcludeBits(enable bool) QueryOption {
	return func(options *QueryOptions) error {
		options.ExcludeBits = enable
		return nil
	}
}

// SkipVersionCheck disables version checking
// *DEPRECATED*
func SkipVersionCheck() ClientOption {
	return func(options *ClientOptions) error {
		log.Println("The SkipVersionCheck client option is deprecated and will be removed - it has no effect and should be removed from your code")
		return nil
	}
}

// LegacyMode enables legacy mode
// *DEPRECATED*
func LegacyMode(enable bool) ClientOption {
	return func(options *ClientOptions) error {
		log.Println("The LegacyMode client option is deprecated and will be removed - it has no effect and should be removed from your code")
		return nil
	}
}

type fragmentNodeRoot struct {
	URI fragmentNode `json:"uri"`
}

type fragmentNode struct {
	Scheme string `json:"scheme"`
	Host   string `json:"host"`
	Port   uint16 `json:"port"`
}

// Status contains the status information from a Pilosa server.
type Status struct {
	Nodes         []StatusNode `json:"nodes"`
	indexMaxSlice map[string]uint64
}

// StatusNode contains node information.
type StatusNode struct {
	Scheme  string        `json:"scheme"`
	Host    string        `json:"host"`
	Port    int           `json:"port"`
	Indexes []StatusIndex `json:"indexes"`
}

type SchemaInfo struct {
	Indexes []StatusIndex `json:"indexes"`
}

// StatusIndex contains index information.
type StatusIndex struct {
	Name    string        `json:"name"`
	Options StatusOptions `json:"options"`
	Frames  []StatusFrame `json:"frames"`
	Slices  []uint64      `json:"slices"`
}

// StatusFrame contains frame information.
type StatusFrame struct {
	Name    string        `json:"name"`
	Options StatusOptions `json:"options"`
}

// StatusOptions contains options for a frame or an index.
type StatusOptions struct {
	CacheType      string        `json:"cacheType"`
	CacheSize      uint          `json:"cacheSize"`
	InverseEnabled bool          `json:"inverseEnabled"`
	RangeEnabled   bool          `json:"rangeEnabled"`
	Fields         []StatusField `json:"fields"`
	TimeQuantum    string        `json:"timeQuantum"`
}

// StatusField contains a field in the status.
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
