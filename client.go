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
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	pbuf "github.com/pilosa/go-pilosa/gopilosa_pbuf"
	"github.com/pilosa/go-pilosa/lru"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// PQLVersion is the version of PQL expected by the client
const PQLVersion = "1.0"

// DefaultShardWidth is used if an index doesn't have it defined.
const DefaultShardWidth = 1 << 20

const maxHosts = 10

// Client is the HTTP client for Pilosa server.
type Client struct {
	cluster *Cluster
	client  *http.Client
	// User-Agent header cache. Not used until cluster-resize support branch is merged
	// and user agent is saved here in NewClient
	userAgent          string
	importThreadCount  int
	importManager      *recordImportManager
	logger             *log.Logger
	coordinatorURI     *URI
	coordinatorLock    *sync.RWMutex
	manualFragmentNode *fragmentNode
	manualServerURI    *URI
	tracer             opentracing.Tracer
	// Number of retries if an HTTP request fails
	retries           int
	minRetrySleepTime time.Duration
	maxRetrySleepTime time.Duration

	importLogEncoder encoder
	logLock          sync.Mutex

	shardNodes shardNodes
	tick       *time.Ticker
	done       chan struct{}
}

func (c *Client) getURIsForShard(index string, shard uint64) ([]*URI, error) {
	uris, ok := c.shardNodes.Get(index, shard)
	if ok {
		return uris, nil
	}
	fragmentNodes, err := c.fetchFragmentNodes(index, shard)
	if err != nil {
		return nil, errors.Wrap(err, "trying to look up nodes for shard")
	}
	uris = make([]*URI, 0, len(fragmentNodes))
	for _, fn := range fragmentNodes {
		uris = append(uris, fn.URI())
	}
	c.shardNodes.Put(index, shard, uris)
	return uris, nil
}

func (c *Client) runChangeDetection() {
	for {
		select {
		case <-c.tick.C:
			c.detectClusterChanges()
		case <-c.done:
			return
		}
	}
}

func (c *Client) Close() error {
	c.tick.Stop()
	close(c.done)
	return nil
}

// detectClusterChanges chooses a random index and shard from the
// shardNodes cache and deletes it. It then looks it up from Pilosa to
// see if it still matches, and if not it drops the whole cache.
func (c *Client) detectClusterChanges() {
	c.shardNodes.mu.Lock()
	// we rely on Go's random map iteration order to get a random
	// element. If it doesn't end up being random, it shouldn't
	// actually matter.
	for index, shardMap := range c.shardNodes.data {
		for shard, uris := range shardMap {
			delete(shardMap, shard)
			c.shardNodes.data[index] = shardMap
			c.shardNodes.mu.Unlock()
			newURIs, err := c.getURIsForShard(index, shard) // refetch URIs from server.
			if err != nil {
				c.logger.Printf("problem invalidating shard node cache: %v", err)
				return
			}
			if len(uris) != len(newURIs) {
				c.logger.Printf("invalidating shard node cache old: %s, new: %s", URIs(uris), URIs(newURIs))
				c.shardNodes.Invalidate()
				return
			}
			for i := range uris {
				u1, u2 := uris[i], newURIs[i]
				if *u1 != *u2 {
					c.logger.Printf("invalidating shard node cache, uri mismatch at %d old: %s, new: %s", i, URIs(uris), URIs(newURIs))
					c.shardNodes.Invalidate()
					return
				}
			}
			break
		}
		break
	}
}

// DefaultClient creates a client with the default address and options.
func DefaultClient() *Client {
	return newClientWithCluster(NewClusterWithHost(DefaultURI()), nil)
}

func newClientFromAddresses(addresses []string, options *ClientOptions) (*Client, error) {
	uris := make([]*URI, len(addresses))
	for i, address := range addresses {
		uri, err := NewURIFromAddress(address)
		if err != nil {
			return nil, err
		}
		uris[i] = uri
	}
	cluster := NewClusterWithHost(uris...)
	client := newClientWithCluster(cluster, options)
	return client, nil
}

func newClientWithCluster(cluster *Cluster, options *ClientOptions) *Client {
	client := newClientWithOptions(options)
	client.cluster = cluster
	return client
}

func newClientWithURI(uri *URI, options *ClientOptions) *Client {
	client := newClientWithOptions(options)
	if options.manualServerAddress {
		fragmentNode := newFragmentNodeFromURI(uri)
		client.manualFragmentNode = &fragmentNode
		client.manualServerURI = uri
		client.cluster = NewClusterWithHost()
	}
	client.cluster = NewClusterWithHost(uri)
	return client
}

func newClientWithOptions(options *ClientOptions) *Client {
	if options == nil {
		options = &ClientOptions{}
	}
	options = options.withDefaults()

	c := &Client{
		client:          newHTTPClient(options.withDefaults()),
		logger:          log.New(os.Stderr, "go-pilosa ", log.Flags()),
		coordinatorLock: &sync.RWMutex{},

		shardNodes: newShardNodes(),
		tick:       time.NewTicker(time.Minute),
		done:       make(chan struct{}, 0),
	}
	if options.importLogWriter != nil {
		c.importLogEncoder = newImportLogEncoder(options.importLogWriter)
	}
	if options.tracer == nil {
		c.tracer = NoopTracer{}
	} else {
		c.tracer = options.tracer
	}
	c.retries = *options.retries
	c.minRetrySleepTime = 100 * time.Millisecond
	c.maxRetrySleepTime = 2 * time.Minute
	c.importManager = newRecordImportManager(c)
	go c.runChangeDetection()
	return c

}

// NewClient creates a client with the given address, URI, or cluster and options.
func NewClient(addrURIOrCluster interface{}, options ...ClientOption) (*Client, error) {
	var cluster *Cluster
	clientOptions := &ClientOptions{}
	err := clientOptions.addOptions(options...)
	if err != nil {
		return nil, err
	}

	switch u := addrURIOrCluster.(type) {
	case string:
		uri, err := NewURIFromAddress(u)
		if err != nil {
			return nil, err
		}
		return newClientWithURI(uri, clientOptions), nil
	case []string:
		if len(u) == 1 {
			uri, err := NewURIFromAddress(u[0])
			if err != nil {
				return nil, err
			}
			return newClientWithURI(uri, clientOptions), nil
		} else if clientOptions.manualServerAddress {
			return nil, ErrSingleServerAddressRequired
		}
		return newClientFromAddresses(u, clientOptions)
	case *URI:
		uriCopy := *u
		return newClientWithURI(&uriCopy, clientOptions), nil
	case []*URI:
		if len(u) == 1 {
			uriCopy := *u[0]
			return newClientWithURI(&uriCopy, clientOptions), nil
		} else if clientOptions.manualServerAddress {
			return nil, ErrSingleServerAddressRequired
		}
		cluster = NewClusterWithHost(u...)
	case *Cluster:
		cluster = u
	case nil:
		cluster = NewClusterWithHost()
	default:
		return nil, ErrAddrURIClusterExpected
	}

	return newClientWithCluster(cluster, clientOptions), nil
}

// Query runs the given query against the server with the given options.
// Pass nil for default options.
func (c *Client) Query(query PQLQuery, options ...interface{}) (*QueryResponse, error) {
	span := c.tracer.StartSpan("Client.Query")
	defer span.Finish()

	if err := query.Error(); err != nil {
		return nil, err
	}
	queryOptions := &QueryOptions{}
	err := queryOptions.addOptions(options...)
	if err != nil {
		return nil, err
	}
	serializedQuery := query.Serialize()
	data, err := makeRequestData(serializedQuery.String(), queryOptions)
	if err != nil {
		return nil, errors.Wrap(err, "making request data")
	}
	useCoordinator := serializedQuery.HasWriteKeys()
	path := fmt.Sprintf("/index/%s/query", query.Index().name)
	_, buf, err := c.httpRequest("POST", path, data, defaultProtobufHeaders(), useCoordinator)
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
	span := c.tracer.StartSpan("Client.CreateIndex")
	defer span.Finish()

	data := []byte(index.options.String())
	path := fmt.Sprintf("/index/%s", index.name)
	response, _, err := c.httpRequest("POST", path, data, nil, false)
	if err != nil {
		if response != nil && response.StatusCode == 409 {
			return ErrIndexExists
		}
		return err
	}
	return nil

}

// CreateField creates a field on the server using the given Field struct.
func (c *Client) CreateField(field *Field) error {
	span := c.tracer.StartSpan("Client.CreateField")
	defer span.Finish()

	data := []byte(field.options.String())
	path := fmt.Sprintf("/index/%s/field/%s", field.index.name, field.name)
	response, _, err := c.httpRequest("POST", path, data, nil, false)
	if err != nil {
		if response != nil && response.StatusCode == 409 {
			return ErrFieldExists
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
	return errors.Wrap(err, "creating index")
}

// EnsureField creates a field on the server if it doesn't exists.
func (c *Client) EnsureField(field *Field) error {
	err := c.CreateField(field)
	if err == ErrFieldExists {
		return nil
	}
	return err
}

// DeleteIndex deletes an index on the server.
func (c *Client) DeleteIndex(index *Index) error {
	return c.DeleteIndexByName(index.Name())
}

// DeleteIndexByName deletes the named index on the server.
func (c *Client) DeleteIndexByName(index string) error {
	span := c.tracer.StartSpan("Client.DeleteIndex")
	defer span.Finish()

	path := fmt.Sprintf("/index/%s", index)
	_, _, err := c.httpRequest("DELETE", path, nil, nil, false)
	return err
}

// DeleteField deletes a field on the server.
func (c *Client) DeleteField(field *Field) error {
	span := c.tracer.StartSpan("Client.DeleteField")
	defer span.Finish()

	path := fmt.Sprintf("/index/%s/field/%s", field.index.name, field.name)
	_, _, err := c.httpRequest("DELETE", path, nil, nil, false)
	return err
}

// SyncSchema updates a schema with the indexes and fields on the server and
// creates the indexes and fields in the schema on the server side.
// This function does not delete indexes and the fields on the server side nor in the schema.
func (c *Client) SyncSchema(schema *Schema) error {
	span := c.tracer.StartSpan("Client.SyncSchema")
	defer span.Finish()

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
	// create the indexes and fields which doesn't exist on the server side
	for indexName, index := range diffSchema.indexes {
		if _, ok := serverSchema.indexes[indexName]; !ok {
			err = c.EnsureIndex(index)
			if err != nil {
				return errors.Wrap(err, "ensuring index")
			}
		}
		for _, field := range index.fields {
			err = c.EnsureField(field)
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
			for fieldName, field := range index.fields {
				localIndex.fields[fieldName] = field
			}
		}
	}

	return nil
}

// Schema returns the indexes and fields on the server.
func (c *Client) Schema() (*Schema, error) {
	span := c.tracer.StartSpan("Client.Schema")
	defer span.Finish()

	var indexes []SchemaIndex
	indexes, err := c.readSchema()
	if err != nil {
		return nil, err
	}
	schema := NewSchema()
	for _, indexInfo := range indexes {
		index := schema.indexWithOptions(indexInfo.Name, indexInfo.ShardWidth, indexInfo.Options.asIndexOptions())
		for _, fieldInfo := range indexInfo.Fields {
			index.fieldWithOptions(fieldInfo.Name, fieldInfo.Options.asFieldOptions())
		}
	}
	return schema, nil
}

// ImportField imports records from the given iterator.
func (c *Client) ImportField(field *Field, iterator RecordIterator, options ...ImportOption) error {
	span := c.tracer.StartSpan("Client.ImportField")
	defer span.Finish()

	importOptions := ImportOptions{}
	for _, option := range options {
		if err := option(&importOptions); err != nil {
			return err
		}
	}
	importOptions = importOptions.withDefaults()
	if importOptions.importRecordsFunction == nil {
		// if import records function was not already set, set it.
		if field.options != nil && field.options.fieldType == FieldTypeInt {
			importRecordsFunction(c.importValues)(&importOptions)
		} else {
			// Check whether roaring imports is available
			if importOptions.wantRoaring != nil && *importOptions.wantRoaring == true {
				importOptions.hasRoaring = c.hasRoaringImportSupport(field)
			}
			importRecordsFunction(c.importColumns)(&importOptions)
		}
	}
	// if the index doesn't have its shardWidth set, we need to get it from the schema
	if field.index.shardWidth == 0 {
		indexes, err := c.readSchema()
		if err != nil {
			return err
		}
		for _, index := range indexes {
			if index.Name != field.index.name {
				continue
			}
			indexCopy := field.index.copy()
			indexCopy.shardWidth = index.ShardWidth
			field = field.copy()
			field.index = indexCopy
			break
		}
		if field.index.shardWidth == 0 {
			// the index does not have shard width, use the default
			field.index.shardWidth = DefaultShardWidth
		}
	}
	return c.importManager.run(field, iterator, importOptions)
}

func (c *Client) importColumns(field *Field,
	shard uint64,
	records []Record,
	nodes []fragmentNode,
	options *ImportOptions,
	state *importState) error {

	if len(nodes) == 0 {
		return errors.New("No nodes to import to")
	}

	if options.hasRoaring {
		return c.importColumnsRoaring(field, shard, records, nodes, options, state)
	}

	if !options.skipSort {
		sort.Sort(recordSort(records))
	}

	importReq := columnsToImportRequest(field, shard, records)
	path, data, err := importPathData(field, shard, importReq, options)
	if err != nil {
		return err
	}
	c.logImport(importReq.GetIndex(), path, shard, false, data)

	eg := errgroup.Group{}
	for _, node := range nodes {
		uri := node.URI()
		eg.Go(func() error {
			return c.importData(uri, path, data)
		})
	}
	err = eg.Wait()
	return errors.Wrap(err, "importing columns to nodes")
}

func (c *Client) importColumnsRoaring(field *Field,
	shard uint64,
	records []Record,
	nodes []fragmentNode,
	options *ImportOptions,
	state *importState) error {

	var err error
	shardWidth := field.index.shardWidth
	columns := make([]Column, 0, len(records))
	for _, rec := range records {
		columns = append(columns, rec.(Column))
	}
	if field.options.keys {
		// attempt to translate row keys
		if state.rowKeyIDMap == nil {
			// create the row key cache if it doesn't exist
			state.rowKeyIDMap, err = lru.NewLRU(options.rowKeyCacheSize)
			if err != nil {
				panic(errors.Wrap(err, "while creating rowKey to ID map"))
			}
		}
		err = c.translateRecordsRowKeys(state.rowKeyIDMap, field, columns)
		if err != nil {
			return errors.Wrap(err, "translating records row keys")
		}
	}
	if field.index.options.keys {
		// attempt to translate column keys
		if state.columnKeyIDMap == nil {
			// create the column key cache if it doesn't exist
			state.columnKeyIDMap, err = lru.NewLRU(options.columnKeyCacheSize)
			if err != nil {
				panic(errors.Wrap(err, "while creating columnKey to ID map"))
			}
		}
		err = c.translateRecordsColumnKeys(state.columnKeyIDMap, field.index, columns)
		if err != nil {
			return errors.Wrap(err, "translating records column keys")
		}
	}
	uri := nodes[0].URI()
	var views viewImports
	if field.options.fieldType == FieldTypeTime {
		views = columnsToBitmapTimeField(field.options.timeQuantum, shardWidth, columns, field.options.noStandardView)
	} else {
		views = columnsToBitmap(shardWidth, columns)
	}
	return c.importRoaringBitmap(uri, field, shard, views, options)
}

func (c *Client) translateRecordsRowKeys(rowKeyIDMap *lru.LRU, field *Field, columns []Column) error {
	uniqueMissingKeys := map[string]struct{}{}
	for _, col := range columns {
		if !rowKeyIDMap.Contains(col.RowKey) {
			uniqueMissingKeys[col.RowKey] = struct{}{}
		}
	}
	keys := make([]string, 0, len(uniqueMissingKeys))
	for key := range uniqueMissingKeys {
		keys = append(keys, key)
	}
	if len(keys) > 0 {
		// translate missing keys
		ids, err := c.TranslateRowKeys(field, keys)
		if err != nil {
			return err
		}
		for i, key := range keys {
			// we have to add without evicting here lest a key which was in cache prior to adding those not in cache is evicted.
			rowKeyIDMap.AddNoEvict(key, ids[i])
		}
	}
	// replace RowKeys with RowIDs
	for i, col := range columns {
		if rowID, ok := rowKeyIDMap.Get(col.RowKey); ok {
			col.RowID = rowID
			columns[i] = col
		} else {
			return fmt.Errorf("Key '%s' does not exist in the rowKey to ID map", col.RowKey)
		}
	}

	// since we called AddNoEvict, we clean up at the end
	rowKeyIDMap.Cleanup()
	return nil
}

func (c *Client) translateRecordsColumnKeys(columnKeyIDMap *lru.LRU, index *Index, columns []Column) error {
	uniqueMissingKeys := map[string]struct{}{}
	for _, col := range columns {
		if !columnKeyIDMap.Contains(col.ColumnKey) {
			uniqueMissingKeys[col.ColumnKey] = struct{}{}
		}
	}
	keys := make([]string, 0, len(uniqueMissingKeys))
	for key := range uniqueMissingKeys {
		keys = append(keys, key)
	}
	if len(keys) > 0 {
		// translate missing keys
		ids, err := c.TranslateColumnKeys(index, keys)
		if err != nil {
			return err
		}
		for i, key := range keys {
			columnKeyIDMap.Add(key, ids[i])
		}
	}
	// replace ColumnKeys with ColumnIDs
	for i, col := range columns {
		if columnID, ok := columnKeyIDMap.Get(col.ColumnKey); ok {
			col.ColumnID = columnID
			columns[i] = col
		} else {
			return fmt.Errorf("Key '%s' does not exist in the columnKey to ID map", col.ColumnKey)
		}
	}
	return nil
}

func (c *Client) hasRoaringImportSupport(field *Field) bool {
	if field.options.fieldType != FieldTypeSet &&
		field.options.fieldType != FieldTypeDefault &&
		field.options.fieldType != FieldTypeBool &&
		field.options.fieldType != FieldTypeTime {
		// Roaring imports is available for only set, bool and time fields.
		return false
	}
	// Check whether the roaring import endpoint exists
	path := makeRoaringImportPath(field, 0, url.Values{})
	// err may contain an HTTP error, but we don't use it.
	resp, _, _ := c.httpRequest("GET", path, nil, nil, false)
	if resp == nil {
		return false
	}
	if resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusOK {
		// Roaring import endpoint exists
		return true
	}
	return false
}

func (c *Client) importValues(field *Field,
	shard uint64,
	vals []Record,
	nodes []fragmentNode,
	options *ImportOptions,
	state *importState) error {

	sort.Sort(recordSort(vals))

	importReq := valsToImportRequest(field, shard, vals)
	path, data, err := importPathData(field, shard, importReq, options)
	if err != nil {
		return err
	}
	c.logImport(importReq.GetIndex(), path, shard, false, data)

	eg := errgroup.Group{}
	for _, node := range nodes {
		uri := node.URI()
		eg.Go(func() error {
			return c.importData(uri, path, data)
		})
	}
	err = eg.Wait()
	return errors.Wrap(err, "importing values to nodes")
}

// Import imports data for a single shard using the regular import
// endpoint rather than import-roaring. This is good for e.g. mutex or
// bool fields where import-roaring is not supported.
func (c *Client) Import(index, field string, shard uint64, vals, ids []uint64, clear bool) error {
	path, data, err := c.EncodeImport(index, field, shard, vals, ids, clear)
	if err != nil {
		return errors.Wrap(err, "encoding import request")
	}
	err = c.DoImport(index, shard, path, data)
	return errors.Wrap(err, "doing import")
}

// EncodeImport computes the HTTP path and payload for an import
// request. It is typically followed by a call to DoImport.
func (c *Client) EncodeImport(index, field string, shard uint64, vals, ids []uint64, clear bool) (path string, data []byte, err error) {
	msg := &pbuf.ImportRequest{
		Index:     index,
		Field:     field,
		Shard:     shard,
		RowIDs:    vals,
		ColumnIDs: ids,
	}
	data, err = proto.Marshal(msg)
	if err != nil {
		return "", nil, errors.Wrap(err, "marshaling Import to protobuf")
	}
	path = fmt.Sprintf("/index/%s/field/%s/import?clear=%s&ignoreKeyCheck=true", index, field, strconv.FormatBool(clear))
	return path, data, nil
}

// DoImport takes a path and data payload (normally from EncodeImport
// or EncodeImportValues), logs the import, finds all nodes which own
// this shard, and concurrently imports to those nodes.
func (c *Client) DoImport(index string, shard uint64, path string, data []byte) error {
	// TODO: figure out if the forwarding logic in Pilosa is the same
	// for importing values as it is for importing mutex/bool. We may
	// need to change that...
	c.logImport(index, path, shard, false, data)

	uris, err := c.getURIsForShard(index, shard)
	if err != nil {
		return errors.Wrap(err, "getting uris")
	}

	eg := errgroup.Group{}
	for _, uri := range uris {
		eg.Go(func() error {
			return c.importData(uri, path, data)
		})
	}
	return errors.Wrap(eg.Wait(), "importing to nodes")
}

// ImportValues takes the given integer values and column ids (which
// must all be in the given shard) and imports them into the given
// index,field,shard on all nodes which should hold that shard. It
// assumes that the ids have been translated from keys if necessary
// and so tells Pilosa to ignore checking if the index uses column
// keys. ImportValues wraps EncodeImportValues and DoImportValues —
// these are broken out and exported so that performance conscious
// users can re-use the same vals and ids byte buffers for local
// encoding, while performing the imports concurrently.
func (c *Client) ImportValues(index, field string, shard uint64, vals []int64, ids []uint64, clear bool) error {
	path, data, err := c.EncodeImportValues(index, field, shard, vals, ids, clear)
	if err != nil {
		return errors.Wrap(err, "encoding import-values request")
	}
	err = c.DoImportValues(index, shard, path, data)
	return errors.Wrap(err, "doing import values")
}

// EncodeImportValues computes the HTTP path and payload for an
// import-values request. It is typically followed by a call to
// DoImportValues.
func (c *Client) EncodeImportValues(index, field string, shard uint64, vals []int64, ids []uint64, clear bool) (path string, data []byte, err error) {
	msg := &pbuf.ImportValueRequest{
		Index:     index,
		Field:     field,
		Shard:     shard,
		ColumnIDs: ids,
		Values:    vals,
	}
	data, err = proto.Marshal(msg)
	if err != nil {
		return "", nil, errors.Wrap(err, "marshaling ImportValue to protobuf")
	}
	path = fmt.Sprintf("/index/%s/field/%s/import?clear=%s&ignoreKeyCheck=true", index, field, strconv.FormatBool(clear))
	return path, data, nil
}

// DoImportValues is deprecated. Use DoImport.
func (c *Client) DoImportValues(index string, shard uint64, path string, data []byte) error {
	return c.DoImport(index, shard, path, data)
}

func importPathData(field *Field, shard uint64, msg proto.Message, options *ImportOptions) (path string, data []byte, err error) {
	data, err = proto.Marshal(msg)
	if err != nil {
		return "", nil, errors.Wrap(err, "marshaling to protobuf")
	}
	params := url.Values{}
	params.Add("clear", strconv.FormatBool(options.clear))
	path = fmt.Sprintf("/index/%s/field/%s/import?%s", field.index.Name(), field.Name(), params.Encode())
	return path, data, nil
}

func (c *Client) fetchFragmentNodes(indexName string, shard uint64) ([]fragmentNode, error) {
	if c.manualFragmentNode != nil {
		return []fragmentNode{*c.manualFragmentNode}, nil
	}
	path := fmt.Sprintf("/internal/fragment/nodes?shard=%d&index=%s", shard, indexName)
	_, body, err := c.httpRequest("GET", path, []byte{}, nil, false)
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

func (c *Client) fetchCoordinatorNode() (fragmentNode, error) {
	if c.manualFragmentNode != nil {
		return *c.manualFragmentNode, nil
	}
	status, err := c.Status()
	if err != nil {
		return fragmentNode{}, err
	}
	for _, node := range status.Nodes {
		if node.IsCoordinator {
			nodeURI := node.URI
			return fragmentNode{
				Scheme: nodeURI.Scheme,
				Host:   nodeURI.Host,
				Port:   nodeURI.Port,
			}, nil
		}
	}
	return fragmentNode{}, errors.New("Coordinator node not found")
}

func (c *Client) importData(uri *URI, path string, data []byte) error {
	resp, err := c.doRequest(uri, "POST", path, defaultProtobufHeaders(), data)
	if err = anyError(resp, err); err != nil {
		return errors.Wrapf(err, "import to %s", uri.HostPort())
	}
	defer resp.Body.Close()

	return nil
}

// ImportRoaringBitmap can import pre-made bitmaps for a number of
// different views into the given field/shard. If the view name in the
// map is an empty string, the standard view will be used.
func (c *Client) ImportRoaringBitmap(field *Field, shard uint64, views map[string]*roaring.Bitmap, clear bool) error {
	uris, err := c.getURIsForShard(field.index.Name(), shard)
	if err != nil {
		return errors.Wrap(err, "getting URIs for import")
	}
	err = c.importRoaringBitmap(uris[0], field, shard, views, &ImportOptions{clear: clear})
	return errors.Wrap(err, "importing bitmap")
}

func (c *Client) importRoaringBitmap(uri *URI, field *Field, shard uint64, views viewImports, options *ImportOptions) error {
	protoViews := []*pbuf.ImportRoaringRequestView{}
	for name, bmp := range views {
		buf := &bytes.Buffer{}
		_, err := bmp.WriteTo(buf)
		if err != nil {
			return errors.Wrap(err, "marshalling bitmap")
		}
		protoViews = append(protoViews, &pbuf.ImportRoaringRequestView{
			Name: name,
			Data: buf.Bytes(),
		})
	}
	params := url.Values{}
	params.Add("clear", strconv.FormatBool(options.clear))
	path := makeRoaringImportPath(field, shard, params)
	req := &pbuf.ImportRoaringRequest{
		Clear: options.clear,
		Views: protoViews,
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	c.logImport(field.index.Name(), path, shard, true, data)

	resp, err := c.doRequest(uri, "POST", path, defaultProtobufHeaders(), data)
	if err = anyError(resp, err); err != nil {
		return errors.Wrapf(err, "roaring import to %s", uri.HostPort())
	}
	defer resp.Body.Close()

	return nil
}

// ExportField exports columns for a field.
func (c *Client) ExportField(field *Field) (io.Reader, error) {
	span := c.tracer.StartSpan("Client.ExportField")
	defer span.Finish()

	var shardsMax map[string]uint64
	var err error

	status, err := c.Status()
	if err != nil {
		return nil, err
	}
	shardsMax, err = c.shardsMax()
	if err != nil {
		return nil, err
	}
	status.indexMaxShard = shardsMax
	shardURIs, err := c.statusToNodeShardsForIndex(status, field.index.Name())
	if err != nil {
		return nil, err
	}

	return newExportReader(c, shardURIs, field), nil
}

// Info returns the server's configuration/host information.
func (c *Client) Info() (Info, error) {
	span := c.tracer.StartSpan("Client.Info")
	defer span.Finish()

	_, data, err := c.httpRequest("GET", "/info", nil, nil, false)
	if err != nil {
		return Info{}, errors.Wrap(err, "requesting /info")
	}
	info := Info{}
	err = json.Unmarshal(data, &info)
	if err != nil {
		return Info{}, errors.Wrap(err, "unmarshaling /info data")
	}
	return info, nil
}

// Status returns the server's status.
func (c *Client) Status() (Status, error) {
	span := c.tracer.StartSpan("Client.Status")
	defer span.Finish()

	_, data, err := c.httpRequest("GET", "/status", nil, nil, false)
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

func (c *Client) readSchema() ([]SchemaIndex, error) {
	_, data, err := c.httpRequest("GET", "/schema", nil, nil, false)
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

func (c *Client) shardsMax() (map[string]uint64, error) {
	_, data, err := c.httpRequest("GET", "/internal/shards/max", nil, nil, false)
	if err != nil {
		return nil, errors.Wrap(err, "requesting /internal/shards/max")
	}
	m := map[string]map[string]uint64{}
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshaling /internal/shards/max data")
	}
	return m["standard"], nil
}

// HttpRequest sends an HTTP request to the Pilosa server.
// **NOTE**: This function is experimental and may be removed in later revisions.
func (c *Client) HttpRequest(method string, path string, data []byte, headers map[string]string) (*http.Response, []byte, error) {
	span := c.tracer.StartSpan("Client.HttpRequest")
	defer span.Finish()

	return c.httpRequest(method, path, data, headers, false)
}

// httpRequest makes a request to the cluster - use this when you want the
// client to choose a host, and it doesn't matter if the request goes to a
// specific host
func (c *Client) httpRequest(method string, path string, data []byte, headers map[string]string, useCoordinator bool) (*http.Response, []byte, error) {
	if data == nil {
		data = []byte{}
	}

	// try at most maxHosts non-failed hosts; protect against broken cluster.removeHost
	var response *http.Response
	var err error
	for i := 0; i < maxHosts; i++ {
		host, err := c.host(useCoordinator)
		if err != nil {
			return nil, nil, err
		}
		response, err = c.doRequest(host, method, path, c.augmentHeaders(headers), data)
		if err == nil {
			break
		}
		if c.manualServerURI == nil {
			if useCoordinator {
				c.coordinatorLock.Lock()
				c.coordinatorURI = nil
				c.coordinatorLock.Unlock()
			} else {
				c.cluster.RemoveHost(host)
			}
		}
		// TODO: exponential backoff
		time.Sleep(1 * time.Second)
	}
	if response == nil {
		return nil, nil, ErrTriedMaxHosts
	}
	defer response.Body.Close()
	warning := response.Header.Get("warning")
	if warning != "" {
		c.logger.Println(warning)
	}
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

func (c *Client) host(useCoordinator bool) (*URI, error) {
	if c.manualServerURI != nil {
		return c.manualServerURI, nil
	}
	var host *URI
	if useCoordinator {
		c.coordinatorLock.RLock()
		host = c.coordinatorURI
		c.coordinatorLock.RUnlock()
		if host == nil {
			c.coordinatorLock.Lock()
			if c.coordinatorURI == nil {
				node, err := c.fetchCoordinatorNode()
				if err != nil {
					c.coordinatorLock.Unlock()
					return nil, errors.Wrap(err, "fetching coordinator node")
				}
				host = URIFromAddress(fmt.Sprintf("%s://%s:%d", node.Scheme, node.Host, node.Port))
			} else {
				host = c.coordinatorURI
			}
			c.coordinatorURI = host
			c.coordinatorLock.Unlock()
		}
	} else {
		// get a host from the cluster
		host = c.cluster.Host()
		if host == nil {
			return nil, ErrEmptyCluster
		}
	}
	return host, nil
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
func (c *Client) doRequest(host *URI, method, path string, headers map[string]string, data []byte) (*http.Response, error) {
	var resp *http.Response
	var err error
	var req *http.Request
	var content []byte

	tries := 1 + c.retries
	sleepTime := c.minRetrySleepTime
	for tries > 0 {
		req, err = makeRequest(host, method, path, headers, data)
		if err != nil {
			return nil, errors.Wrap(err, "building request")
		}
		tries--
		resp, err = c.client.Do(req)
		if err == nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return resp, nil
			}
			// Pilosa nodes sometimes return 400, we retry in that case.
			// No need to retry in other 4xx cases.
			if resp.StatusCode > 400 && resp.StatusCode < 500 {
				return resp, nil
			}
			content, err = ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil, err
			}
			err = errors.New(strings.TrimSpace(string(content)))
		}
		if tries == 0 {
			break
		}
		var statusCode int
		if resp != nil {
			statusCode = resp.StatusCode
		}
		c.logger.Printf("request failed with: '%v' status: %d, retrying %d more time(s) after %v ", err, statusCode, tries, sleepTime)
		time.Sleep(sleepTime)
		sleepTime *= 2
		if sleepTime > c.maxRetrySleepTime {
			sleepTime = c.maxRetrySleepTime
		}
	}
	return nil, errors.Wrap(err, "max retries exceeded")
}

// statusToNodeShardsForIndex finds the hosts which contains shards for the given index
func (c *Client) statusToNodeShardsForIndex(status Status, indexName string) (map[uint64]*URI, error) {
	result := make(map[uint64]*URI)
	if maxShard, ok := status.indexMaxShard[indexName]; ok {
		for shard := 0; shard <= int(maxShard); shard++ {
			fragmentNodes, err := c.fetchFragmentNodes(indexName, uint64(shard))
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

			result[uint64(shard)] = uri
		}
	} else {
		return nil, ErrNoShard
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

func (c *Client) TranslateRowKeys(field *Field, keys []string) ([]uint64, error) {
	req := &pbuf.TranslateKeysRequest{
		Index: field.index.name,
		Field: field.name,
		Keys:  keys,
	}
	return c.translateKeys(req, keys)
}

func (c *Client) TranslateColumnKeys(index *Index, keys []string) ([]uint64, error) {
	req := &pbuf.TranslateKeysRequest{
		Index: index.name,
		Keys:  keys,
	}
	return c.translateKeys(req, keys)
}

func (c *Client) translateKeys(req *pbuf.TranslateKeysRequest, keys []string) ([]uint64, error) {
	reqData, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "marshalling traslate keys request")
	}
	resp, respData, err := c.httpRequest("POST", "/internal/translate/keys", reqData, defaultProtobufHeaders(), true)
	if err := anyError(resp, err); err != nil {
		return nil, err
	}
	idsResp := &pbuf.TranslateKeysResponse{}
	err = proto.Unmarshal(respData, idsResp)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshalling traslate keys response")
	}
	return idsResp.IDs, nil
}

func (c *Client) logImport(index, path string, shard uint64, isRoaring bool, data []byte) {
	if c.importLogEncoder == nil {
		return
	}
	c.logLock.Lock()
	go func() {
		defer c.logLock.Unlock()
		l := &importLog{
			Index:     index,
			Path:      path,
			Shard:     shard,
			IsRoaring: isRoaring,
			Timestamp: time.Now().UnixNano(),
			Data:      data,
		}
		// Encode is actually threadsafe (with gob), but the lock above is
		// helpful for knowing when we've finished all these goroutines.
		err := c.importLogEncoder.Encode(l)
		if err != nil {
			c.logger.Printf("writing to import log: %v", err)
		}
	}()
}

// ExperimentalReplayImport takes a data stream which was previously recorded by the import
// logging functionality and imports it to Pilosa. The target cluster need not
// be of the same size as the original cluster, but it must already have the
// necessary schema in place. It is an experimental method and may be changed or
// removed.
func (c *Client) ExperimentalReplayImport(r io.Reader, concurrency int) error {
	span := c.tracer.StartSpan("Client.ExperimentalReplayImport")
	defer span.Finish()

	// make work channel
	work := make(chan *importLog, concurrency*2)

	// spawn <concurrency> workers to read from channel
	eg := &errgroup.Group{}
	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			for log := range work {
				// regular import doesn't forward to replicas, so we have to get all
				// the nodes.
				nodes, err := c.fetchFragmentNodes(log.Index, log.Shard)
				if err != nil {
					return errors.Wrap(err, "fetching fragment nodes")
				}

				if !log.IsRoaring {
					for _, node := range nodes {
						resp, err := c.doRequest(node.URI(), "POST", log.Path, defaultProtobufHeaders(), log.Data)
						if err = anyError(resp, err); err != nil {
							return errors.Wrap(err, "doing import")
						}
						resp.Body.Close()
					}
				} else {
					// import-roaring forwards on to all replicas, so we only import to
					// one node.
					resp, err := c.doRequest(nodes[0].URI(), "POST", log.Path, defaultProtobufHeaders(), log.Data)
					if err = anyError(resp, err); err != nil {
						return errors.Wrap(err, "doing import")
					}
					resp.Body.Close()
				}
			}
			return nil
		})
	}

	// wait on importers asynchronously in case they return early with errors
	waitErrCh := make(chan error)
	go func() {
		waitErrCh <- eg.Wait()
		close(waitErrCh)
	}()

	// populate work channel
	dec := newImportLogDecoder(r)
	var err error
	for {
		log := importLog{}
		err = dec.Decode(&log)
		if err != nil {
			err = errors.Wrap(err, "decoding")
			break
		}
		select {
		case work <- &log:
			continue
		case err = <-waitErrCh:
			err = errors.Wrap(err, "waiting")
		}
		break
	}

	// close work channel (now workers can exit)
	close(work)
	<-waitErrCh // make sure workers are complete
	if errors.Cause(err) == io.EOF {
		return nil
	}
	return err
}

func defaultProtobufHeaders() map[string]string {
	return map[string]string{
		"Content-Type": "application/x-protobuf",
		"Accept":       "application/x-protobuf",
		"PQL-Version":  PQLVersion,
	}
}

func makeRequest(host *URI, method, path string, headers map[string]string, data []byte) (*http.Request, error) {
	request, err := http.NewRequest(method, host.Normalize()+path, bytes.NewReader(data))
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
		Query:           query,
		Shards:          options.Shards,
		ColumnAttrs:     options.ColumnAttrs,
		ExcludeRowAttrs: options.ExcludeRowAttrs,
		ExcludeColumns:  options.ExcludeColumns,
	}
	r, err := proto.Marshal(request)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling request to protobuf")
	}
	return r, nil
}

func makeRoaringImportPath(field *Field, shard uint64, params url.Values) string {
	return fmt.Sprintf("/index/%s/field/%s/import-roaring/%d?%s",
		field.index.name, field.name, shard, params.Encode())
}

func columnsToImportRequest(field *Field, shard uint64, records []Record) *pbuf.ImportRequest {
	var rowIDs []uint64
	var columnIDs []uint64
	var rowKeys []string
	var columnKeys []string

	recordCount := len(records)
	timestamps := make([]int64, 0, recordCount)
	hasRowKeys := field.options.keys
	hasColKeys := field.index.options.keys

	if hasRowKeys {
		rowKeys = make([]string, 0, recordCount)
	} else {
		rowIDs = make([]uint64, 0, recordCount)
	}

	if hasColKeys {
		columnKeys = make([]string, 0, recordCount)
	} else {
		columnIDs = make([]uint64, 0, recordCount)
	}

	for _, record := range records {
		column := record.(Column)
		if hasRowKeys {
			rowKeys = append(rowKeys, column.RowKey)
		} else {
			rowIDs = append(rowIDs, column.RowID)
		}

		if hasColKeys {
			columnKeys = append(columnKeys, column.ColumnKey)
		} else {
			columnIDs = append(columnIDs, column.ColumnID)
		}

		timestamps = append(timestamps, column.Timestamp)
	}

	return &pbuf.ImportRequest{
		Index:      field.index.name,
		Field:      field.name,
		Shard:      shard,
		RowIDs:     rowIDs,
		ColumnIDs:  columnIDs,
		RowKeys:    rowKeys,
		ColumnKeys: columnKeys,
		Timestamps: timestamps,
	}
}

type viewImports map[string]*roaring.Bitmap

func columnsToBitmap(shardWidth uint64, columns []Column) viewImports {
	bmp := roaring.NewBTreeBitmap()
	for _, col := range columns {
		bmp.DirectAdd(col.RowID*shardWidth + (col.ColumnID % shardWidth))
	}
	return map[string]*roaring.Bitmap{"": bmp}
}

func columnsToBitmapTimeField(quantum TimeQuantum, shardWidth uint64, columns []Column, noStandardView bool) viewImports {
	var standard *roaring.Bitmap
	views := viewImports{}
	if !noStandardView {
		standard = roaring.NewBTreeBitmap()
		views[""] = standard
	}
	for _, col := range columns {
		b := col.RowID*shardWidth + (col.ColumnID % shardWidth)
		if standard != nil {
			standard.DirectAdd(b)
		}
		// TODO: cache time views
		timeViews := viewsByTime(time.Unix(0, col.Timestamp).UTC(), quantum)
		for _, name := range timeViews {
			view, ok := views[name]
			if !ok {
				view = roaring.NewBTreeBitmap()
				views[name] = view
			}
			view.DirectAdd(b)
		}
	}
	return views
}

// viewsByTime returns a list of views for a given timestamp.
func viewsByTime(t time.Time, q TimeQuantum) []string { // nolint: unparam
	a := make([]string, 0, len(q))
	for _, unit := range q {
		view := viewByTimeUnit(t, unit)
		if view == "" {
			continue
		}
		a = append(a, view)
	}
	return a
}

// viewByTimeUnit returns the view name for time with a given quantum unit.
func viewByTimeUnit(t time.Time, unit rune) string {
	switch unit {
	case 'Y':
		return t.Format("2006")
	case 'M':
		return t.Format("200601")
	case 'D':
		return t.Format("20060102")
	case 'H':
		return t.Format("2006010215")
	default:
		return ""
	}
}

func valsToImportRequest(field *Field, shard uint64, vals []Record) *pbuf.ImportValueRequest {
	var columnIDs []uint64
	var columnKeys []string
	values := make([]int64, 0, len(vals))
	keys := field.index.options.keys
	if keys {
		columnKeys = make([]string, 0, len(vals))

	} else {
		columnIDs = make([]uint64, 0, len(vals))

	}
	for _, record := range vals {
		val := record.(FieldValue)
		if keys {
			columnKeys = append(columnKeys, val.ColumnKey)
		} else {
			columnIDs = append(columnIDs, val.ColumnID)
		}
		values = append(values, val.Value)
	}
	return &pbuf.ImportValueRequest{
		Index:      field.index.name,
		Field:      field.name,
		Shard:      shard,
		ColumnIDs:  columnIDs,
		ColumnKeys: columnKeys,
		Values:     values,
	}
}

// ClientOptions control the properties of client connection to the server.
type ClientOptions struct {
	SocketTimeout       time.Duration
	ConnectTimeout      time.Duration
	PoolSizePerRoute    int
	TotalPoolSize       int
	TLSConfig           *tls.Config
	manualServerAddress bool
	tracer              opentracing.Tracer
	retries             *int

	importLogWriter io.Writer
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

// OptClientSocketTimeout is the maximum idle socket time in nanoseconds
func OptClientSocketTimeout(timeout time.Duration) ClientOption {
	return func(options *ClientOptions) error {
		options.SocketTimeout = timeout
		return nil
	}
}

// OptClientConnectTimeout is the maximum time to connect in nanoseconds.
func OptClientConnectTimeout(timeout time.Duration) ClientOption {
	return func(options *ClientOptions) error {
		options.ConnectTimeout = timeout
		return nil
	}
}

// OptClientPoolSizePerRoute is the maximum number of active connections in the pool to a host.
func OptClientPoolSizePerRoute(size int) ClientOption {
	return func(options *ClientOptions) error {
		options.PoolSizePerRoute = size
		return nil
	}
}

// OptClientTotalPoolSize is the maximum number of connections in the pool.
func OptClientTotalPoolSize(size int) ClientOption {
	return func(options *ClientOptions) error {
		options.TotalPoolSize = size
		return nil
	}
}

// OptClientTLSConfig contains the TLS configuration.
func OptClientTLSConfig(config *tls.Config) ClientOption {
	return func(options *ClientOptions) error {
		options.TLSConfig = config
		return nil
	}
}

// OptClientManualServerAddress forces the client use only the manual server address
func OptClientManualServerAddress(enabled bool) ClientOption {
	return func(options *ClientOptions) error {
		options.manualServerAddress = enabled
		return nil
	}
}

// ExperimentalOptClientLogImports writes all imports to the given writer in
// such a way that they can easily be replayed into another Pilosa cluster. It
// is an experimental option and may be changed or removed.
func ExperimentalOptClientLogImports(loc io.Writer) ClientOption {
	return func(options *ClientOptions) error {
		options.importLogWriter = loc
		return nil
	}
}

// OptClientTracer sets the Open Tracing tracer
// See: https://opentracing.io
func OptClientTracer(tracer opentracing.Tracer) ClientOption {
	return func(options *ClientOptions) error {
		options.tracer = tracer
		return nil
	}
}

// OptClientRetries sets the number of retries on HTTP request failures.
func OptClientRetries(retries int) ClientOption {
	return func(options *ClientOptions) error {
		if retries < 0 {
			return errors.New("retries must be non-negative")
		}
		options.retries = &retries
		return nil
	}
}

type versionInfo struct {
	Version string `json:"version"`
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
	if updated.retries == nil {
		retries := 2
		updated.retries = &retries
	}
	return
}

// QueryOptions contains options to customize the Query function.
type QueryOptions struct {
	// Shards restricts query to a subset of shards. Queries all shards if nil.
	Shards []uint64
	// ColumnAttrs enables returning columns in the query response.
	ColumnAttrs bool
	// ExcludeRowAttrs inhibits returning attributes
	ExcludeRowAttrs bool
	// ExcludeColumns inhibits returning columns
	ExcludeColumns bool
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

// OptQueryColumnAttrs enables returning column attributes in the result.
func OptQueryColumnAttrs(enable bool) QueryOption {
	return func(options *QueryOptions) error {
		options.ColumnAttrs = enable
		return nil
	}
}

// OptQueryShards restricts the set of shards on which a query operates.
func OptQueryShards(shards ...uint64) QueryOption {
	return func(options *QueryOptions) error {
		options.Shards = append(options.Shards, shards...)
		return nil
	}
}

// OptQueryExcludeAttrs enables discarding attributes from a result,
func OptQueryExcludeAttrs(enable bool) QueryOption {
	return func(options *QueryOptions) error {
		options.ExcludeRowAttrs = enable
		return nil
	}
}

// OptQueryExcludeColumns enables discarding columns from a result,
func OptQueryExcludeColumns(enable bool) QueryOption {
	return func(options *QueryOptions) error {
		options.ExcludeColumns = enable
		return nil
	}
}

type importState struct {
	rowKeyIDMap    *lru.LRU
	columnKeyIDMap *lru.LRU
}

// ImportOptions are the options for controlling the importer
type ImportOptions struct {
	threadCount           int
	timeout               time.Duration
	batchSize             int
	statusChan            chan<- ImportStatusUpdate
	importRecordsFunction func(field *Field,
		shard uint64,
		records []Record,
		nodes []fragmentNode,
		options *ImportOptions,
		state *importState) error
	wantRoaring        *bool
	hasRoaring         bool
	clear              bool
	rowKeyCacheSize    int
	columnKeyCacheSize int
	skipSort           bool
}

func (opt *ImportOptions) withDefaults() (updated ImportOptions) {
	updated = *opt

	if updated.threadCount <= 0 {
		updated.threadCount = 1
	}
	if updated.timeout <= 0 {
		updated.timeout = 100 * time.Millisecond
	}
	if updated.batchSize <= 0 {
		updated.batchSize = 100000
	}
	// roaring import is default. See: https://github.com/pilosa/go-pilosa/issues/226
	if updated.wantRoaring == nil {
		wantRoaring := true
		updated.wantRoaring = &wantRoaring
	}
	updated.rowKeyCacheSize = updated.batchSize
	updated.columnKeyCacheSize = updated.batchSize
	return
}

// ImportOption is used when running imports.
type ImportOption func(options *ImportOptions) error

// OptImportThreadCount is the number of goroutines allocated for import.
func OptImportThreadCount(count int) ImportOption {
	return func(options *ImportOptions) error {
		options.threadCount = count
		return nil
	}
}

// OptImportBatchSize is the number of records read before importing them.
func OptImportBatchSize(batchSize int) ImportOption {
	return func(options *ImportOptions) error {
		options.batchSize = batchSize
		return nil
	}
}

// OptImportStatusChannel is a channel which carry import status information.
// Make sure to read from this channel, otherwise the import process will stall.
func OptImportStatusChannel(statusChan chan<- ImportStatusUpdate) ImportOption {
	return func(options *ImportOptions) error {
		options.statusChan = statusChan
		return nil
	}
}

// OptImportClear sets clear import, which clears bits instead of setting them.
func OptImportClear(clear bool) ImportOption {
	return func(options *ImportOptions) error {
		options.clear = clear
		return nil
	}
}

// OptImportRoaring enables importing using roaring bitmaps which is more performant.
func OptImportRoaring(enable bool) ImportOption {
	return func(options *ImportOptions) error {
		options.wantRoaring = &enable
		return nil
	}
}

// OptImportSort tells the importer whether or not to sort batches of records, on
// by default. Sorting imposes some performance cost, especially on data that's
// already sorted, but dramatically improves performance in pathological
// cases. It is enabled by default because the pathological cases are awful,
// and the performance hit is comparatively small, but the performance cost can
// be significant if you know your data is sorted.
func OptImportSort(sorting bool) ImportOption {
	return func(options *ImportOptions) error {
		// skipSort is expressed negatively because we want to
		// keep sorting enabled by default, so the zero value should
		// be that default behavior. The client option expresses it
		// positively because that's easier for API users.
		options.skipSort = !sorting
		return nil
	}
}

func importRecordsFunction(fun func(field *Field,
	shard uint64,
	records []Record,
	nodes []fragmentNode,
	options *ImportOptions,
	state *importState) error) ImportOption {
	return func(options *ImportOptions) error {
		options.importRecordsFunction = fun
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

func newFragmentNodeFromURI(uri *URI) fragmentNode {
	return fragmentNode{
		Scheme: uri.scheme,
		Host:   uri.host,
		Port:   uri.port,
	}
}

func (node fragmentNode) URI() *URI {
	return &URI{
		scheme: node.Scheme,
		host:   node.Host,
		port:   node.Port,
	}
}

// Info contains the configuration/host information from a Pilosa server.
type Info struct {
	ShardWidth       uint64 `json:"shardWidth"`       // width of each shard
	Memory           uint64 `json:"memory"`           // approximate host physical memory
	CPUType          string `json:"cpuType"`          // "brand name string" from cpuid
	CPUPhysicalCores int    `json:"CPUPhysicalCores"` // physical cores (cpuid)
	CPULogicalCores  int    `json:"CPULogicalCores"`  // logical cores cpuid
	CPUMHz           uint64 `json:"CPUMHz"`           // estimated clock speed
}

// Status contains the status information from a Pilosa server.
type Status struct {
	Nodes         []StatusNode `json:"nodes"`
	State         string       `json:"state"`
	LocalID       string       `json:"localID"`
	indexMaxShard map[string]uint64
}

// StatusNode contains information about a node in the cluster.
type StatusNode struct {
	ID            string    `json:"id"`
	URI           StatusURI `json:"uri"`
	IsCoordinator bool      `json:"isCoordinator"`
}

// StatusURI contains node information.
type StatusURI struct {
	Scheme string `json:"scheme"`
	Host   string `json:"host"`
	Port   uint16 `json:"port"`
}

// SchemaInfo contains the indexes.
type SchemaInfo struct {
	Indexes []SchemaIndex `json:"indexes"`
}

// SchemaIndex contains index information.
type SchemaIndex struct {
	Name       string        `json:"name"`
	Options    SchemaOptions `json:"options"`
	Fields     []SchemaField `json:"fields"`
	Shards     []uint64      `json:"shards"`
	ShardWidth uint64        `json:"shardWidth"`
}

// SchemaField contains field information.
type SchemaField struct {
	Name    string        `json:"name"`
	Options SchemaOptions `json:"options"`
}

// SchemaOptions contains options for a field or an index.
type SchemaOptions struct {
	FieldType      FieldType `json:"type"`
	CacheType      string    `json:"cacheType"`
	CacheSize      uint      `json:"cacheSize"`
	TimeQuantum    string    `json:"timeQuantum"`
	Min            int64     `json:"min"`
	Max            int64     `json:"max"`
	Scale          int64     `json:"scale"`
	Keys           bool      `json:"keys"`
	NoStandardView bool      `json:"noStandardView"`
	TrackExistence bool      `json:"trackExistence"`
}

func (so SchemaOptions) asIndexOptions() *IndexOptions {
	return &IndexOptions{
		keys:              so.Keys,
		keysSet:           true,
		trackExistence:    so.TrackExistence,
		trackExistenceSet: true,
	}
}

func (so SchemaOptions) asFieldOptions() *FieldOptions {
	return &FieldOptions{
		fieldType:      so.FieldType,
		cacheSize:      int(so.CacheSize),
		cacheType:      CacheType(so.CacheType),
		timeQuantum:    TimeQuantum(so.TimeQuantum),
		min:            so.Min,
		max:            so.Max,
		scale:          so.Scale,
		keys:           so.Keys,
		noStandardView: so.NoStandardView,
	}
}

type exportReader struct {
	client       *Client
	shardURIs    map[uint64]*URI
	field        *Field
	body         []byte
	bodyIndex    int
	currentShard uint64
	shardCount   uint64
}

func newExportReader(client *Client, shardURIs map[uint64]*URI, field *Field) *exportReader {
	return &exportReader{
		client:     client,
		shardURIs:  shardURIs,
		field:      field,
		shardCount: uint64(len(shardURIs)),
	}
}

// Read updates the passed array with the exported CSV data and returns the number of bytes read
func (r *exportReader) Read(p []byte) (n int, err error) {
	if r.currentShard >= r.shardCount {
		err = io.EOF
		return
	}
	if r.body == nil {
		uri, _ := r.shardURIs[r.currentShard]
		headers := map[string]string{
			"Accept": "text/csv",
		}
		path := fmt.Sprintf("/export?index=%s&field=%s&shard=%d",
			r.field.index.Name(), r.field.Name(), r.currentShard)
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
		r.currentShard++
	}
	return
}
