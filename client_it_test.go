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
//
// +build !nointegration

package pilosa

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	pbuf "github.com/pilosa/go-pilosa/gopilosa_pbuf"
	"github.com/pkg/errors"
)

var index *Index
var keysIndex *Index
var testField *Field

func TestMain(m *testing.M) {
	Setup()
	r := m.Run()
	TearDown()
	os.Exit(r)
}

func Setup() {
	testSchema := NewSchema()
	index = testSchema.Index("go-testindex")
	keysIndex = testSchema.Index("go-testinindex-keys", OptIndexKeys(true))
	testField = index.Field("test-field")
	client := getClient()
	err := client.SyncSchema(testSchema)
	if err != nil {
		panic(err)
	}
}

func TearDown() {
	client := getClient()
	err := client.DeleteIndex(index)
	if err != nil {
		panic(err)
	}
	err = client.DeleteIndex(keysIndex)
	if err != nil {
		panic(err)
	}
}

func Reset() {
	client := getClient()
	client.DeleteIndex(index)
	Setup()
}

func TestCreateDefaultClient(t *testing.T) {
	client := DefaultClient()
	if client == nil {
		t.Fatal()
	}
}

func TestClientReturnsResponse(t *testing.T) {
	client := getClient()
	response, err := client.Query(testField.Row(1))
	if err != nil {
		t.Fatalf("Error querying: %s", err)
	}
	if response == nil {
		t.Fatalf("Response should not be nil")
	}
}

func TestQueryWithShards(t *testing.T) {
	Reset()
	const shardWidth = 1048576
	client := getClient()
	if _, err := client.Query(testField.Set(1, 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(testField.Set(1, shardWidth)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(testField.Set(1, shardWidth*3)); err != nil {
		t.Fatal(err)
	}

	response, err := client.Query(testField.Row(1), OptQueryShards(0, 3))
	if err != nil {
		t.Fatal(err)
	}
	if columns := response.Result().Row().Columns; !reflect.DeepEqual(columns, []uint64{100, shardWidth * 3}) {
		t.Fatalf("Unexpected results: %#v", columns)
	}
}

func TestQueryWithColumns(t *testing.T) {
	Reset()
	client := getClient()
	targetAttrs := map[string]interface{}{
		"name":       "some string",
		"age":        int64(95),
		"registered": true,
		"height":     1.83,
	}
	_, err := client.Query(testField.Set(1, 100))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(index.SetColumnAttrs(100, targetAttrs))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(response.Column(), ColumnItem{}) {
		t.Fatalf("No columns should be returned if it wasn't explicitly requested")
	}
	response, err = client.Query(testField.Row(1), &QueryOptions{ColumnAttrs: true})
	if err != nil {
		t.Fatal(err)
	}
	columns := response.Columns()
	if len(columns) != 1 {
		t.Fatalf("Column count should be == 1")
	}
	if columns[0].ID != 100 {
		t.Fatalf("Column ID should be == 100")
	}
	if !reflect.DeepEqual(columns[0].Attributes, targetAttrs) {
		t.Fatalf("Column attrs does not match")
	}

	if !reflect.DeepEqual(response.Column(), columns[0]) {
		t.Fatalf("Columns() should be equivalent to first column in the response")
	}
}

func TestSetRowAttrs(t *testing.T) {
	Reset()
	client := getClient()
	targetAttrs := map[string]interface{}{
		"name":       "some string",
		"age":        int64(95),
		"registered": true,
		"height":     1.83,
	}
	_, err := client.Query(testField.Set(1, 100))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(testField.SetRowAttrs(1, targetAttrs))
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(testField.Row(1), &QueryOptions{ColumnAttrs: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(targetAttrs, response.Result().Row().Attributes) {
		t.Fatalf("Row attributes should be set")
	}
}

func TestOrmCount(t *testing.T) {
	client := getClient()
	countField := index.Field("count-test")
	err := client.EnsureField(countField)
	if err != nil {
		t.Fatal(err)
	}
	qry := index.BatchQuery(
		countField.Set(10, 20),
		countField.Set(10, 21),
		countField.Set(15, 25),
	)
	client.Query(qry)
	response, err := client.Query(index.Count(countField.Row(10)))
	if err != nil {
		t.Fatal(err)
	}
	if response.Result().Count() != 2 {
		t.Fatalf("Count should be 2")
	}
}

func TestIntersectReturns(t *testing.T) {
	client := getClient()
	field := index.Field("segments")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	qry1 := index.BatchQuery(
		field.Set(2, 10),
		field.Set(2, 15),
		field.Set(3, 10),
		field.Set(3, 20),
	)
	client.Query(qry1)
	qry2 := index.Intersect(field.Row(2), field.Row(3))
	response, err := client.Query(qry2)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results()) != 1 {
		t.Fatal("There must be 1 result")
	}
	if !reflect.DeepEqual(response.Result().Row().Columns, []uint64{10}) {
		t.Fatal("Returned columns must be: [10]")
	}
}

func TestTopNReturns(t *testing.T) {
	client := getClient()
	field := index.Field("topn_test")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	qry := index.BatchQuery(
		field.Set(10, 5),
		field.Set(10, 10),
		field.Set(10, 15),
		field.Set(20, 5),
		field.Set(30, 5),
	)
	client.Query(qry)
	// XXX: The following is required to make this test pass. See: https://github.com/pilosa/pilosa/issues/625
	client.HttpRequest("POST", "/recalculate-caches", nil, nil)
	response, err := client.Query(field.TopN(2))
	if err != nil {
		t.Fatal(err)
	}
	items := response.Result().CountItems()
	if len(items) != 2 {
		t.Fatalf("There should be 2 count items: %v", items)
	}
	item := items[0]
	if item.ID != 10 {
		t.Fatalf("Item[0] ID should be 10")
	}
	if item.Count != 3 {
		t.Fatalf("Item[0] Count should be 3")
	}

	client.Query(field.SetRowAttrs(10, map[string]interface{}{"foo": "bar"}))
	response, err = client.Query(field.FilterAttrTopN(5, nil, "foo", "bar"))
	items = response.Result().CountItems()
	if len(items) != 1 {
		t.Fatalf("There should be 1 count item: %v", items)
	}
	item = items[0]
	if item.ID != 10 {
		t.Fatalf("Item[0] ID should be 10")
	}
	if item.Count != 3 {
		t.Fatalf("Item[0] Count should be 3")
	}

}

func TestCreateDeleteIndexField(t *testing.T) {
	client := getClient()
	index1 := NewIndex("to-be-deleted")
	field1 := index1.Field("foo")
	err := client.CreateIndex(index1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateField(field1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.DeleteField(field1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.DeleteIndex(index1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnsureIndexExists(t *testing.T) {
	client := getClient()
	err := client.EnsureIndex(index)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnsureFieldExists(t *testing.T) {
	client := getClient()
	err := client.EnsureField(testField)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateFieldWithTimeQuantum(t *testing.T) {
	client := getClient()
	field := index.Field("field-with-timequantum", OptFieldTypeTime(TimeQuantumYear))
	err := client.CreateField(field)
	if err != nil {
		t.Fatal(err)
	}
}

func TestErrorCreatingIndex(t *testing.T) {
	client := getClient()
	err := client.CreateIndex(index)
	if err == nil {
		t.Fatal()
	}
}

func TestErrorCreatingField(t *testing.T) {
	client := getClient()
	err := client.CreateField(testField)
	if err == nil {
		t.Fatal()
	}
}

func TestIndexAlreadyExists(t *testing.T) {
	client := getClient()
	err := client.CreateIndex(index)
	if err != ErrIndexExists {
		t.Fatal(err)
	}
}

func TestQueryWithEmptyClusterFails(t *testing.T) {
	client, _ := NewClient(DefaultCluster())
	attrs := map[string]interface{}{"a": 1}
	_, err := client.Query(index.SetColumnAttrs(0, attrs))
	if err != ErrEmptyCluster {
		t.Fatal(err)
	}
}

func TestFailoverFail(t *testing.T) {
	uri, _ := NewURIFromAddress("does-not-resolve.foo.bar")
	cluster := NewClusterWithHost(uri, uri, uri, uri)
	client, _ := NewClient(cluster)
	attrs := map[string]interface{}{"a": 1}
	_, err := client.Query(index.SetColumnAttrs(0, attrs))
	if err != ErrTriedMaxHosts {
		t.Fatalf("ErrTriedMaxHosts error should be returned")
	}
}

func TestCoordinatorFailoverFail(t *testing.T) {
	content := `{"state":"NORMAL","nodes":[{"id":"827c7196-8875-4467-bee2-3604a4346f2b","uri":{"scheme":"http","host":"nonexistent","port":15000},"isCoordinator":true}],"localID":"827c7196-8875-4467-bee2-3604a4346f2b"}`
	server := getMockServer(200, []byte(content), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.Query(keysIndex.SetColumnAttrs("foo", map[string]interface{}{"foo": "bar"}))
	if err != ErrTriedMaxHosts {
		t.Fatalf("ErrTriedMaxHosts error should be returned")
	}
}

func TestQueryFailsIfAddressNotResolved(t *testing.T) {
	uri, _ := NewURIFromAddress("nonexisting.domain.pilosa.com:3456")
	client, _ := NewClient(uri)
	_, err := client.Query(index.RawQuery("bar"))
	if err == nil {
		t.Fatal()
	}
}

func TestQueryFails(t *testing.T) {
	client := getClient()
	_, err := client.Query(index.RawQuery("Invalid query"))
	if err == nil {
		t.Fatal()
	}
}

func TestInvalidHttpRequest(t *testing.T) {
	client := getClient()
	_, _, err := client.httpRequest("INVALID METHOD", "/foo", nil, nil, false)
	if err == nil {
		t.Fatal()
	}
}

func TestErrorResponseNotRead(t *testing.T) {
	server := getMockServer(500, []byte("Unknown error"), 512)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := NewClient(uri)
	response, err := client.Query(testField.Row(1))
	if err == nil {
		t.Fatalf("Got response: %v", response)
	}
}

func TestResponseNotRead(t *testing.T) {
	server := getMockServer(200, []byte("some content"), 512)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := NewClient(uri)
	response, err := client.Query(testField.Row(1))
	if err == nil {
		t.Fatalf("Got response: %v", response)
	}
}

func TestSchema(t *testing.T) {
	client := getClient()
	schema, err := client.Schema()
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.indexes) < 1 {
		t.Fatalf("There should be at least 1 index in the schema")
	}
	f := index.Field("schema-test-field",
		OptFieldTypeSet(CacheTypeLRU, 9999),
	)
	err = client.EnsureField(f)
	if err != nil {
		t.Fatal(err)
	}
	schema, err = client.Schema()
	if err != nil {
		t.Fatal(err)
	}
	f = schema.indexes[index.Name()].fields["schema-test-field"]
	if f == nil {
		t.Fatal("Field should not be nil")
	}
	opt := f.options
	if opt.cacheType != CacheTypeLRU {
		t.Fatalf("cache type %s != %s", CacheTypeLRU, opt.cacheType)
	}
	if opt.cacheSize != 9999 {
		t.Fatalf("cache size 9999 != %d", opt.cacheSize)
	}
}

func TestSync(t *testing.T) {
	client := getClient()
	remoteIndex := NewIndex("remote-index-1")
	err := client.EnsureIndex(remoteIndex)
	if err != nil {
		t.Fatal(err)
	}
	remoteField := remoteIndex.Field("remote-field-1")
	err = client.EnsureField(remoteField)
	if err != nil {
		t.Fatal(err)
	}
	schema1 := NewSchema()
	index11 := schema1.Index("diff-index1")
	index11.Field("field1-1")
	index11.Field("field1-2")
	index12 := schema1.Index("diff-index2")
	index12.Field("field2-1")
	schema1.Index(remoteIndex.Name())

	err = client.SyncSchema(schema1)
	if err != nil {
		t.Fatal(err)
	}
	client.DeleteIndex(remoteIndex)
	client.DeleteIndex(index11)
	client.DeleteIndex(index12)
}

func TestSyncFailure(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client, _ := NewClient(uri)
	err = client.SyncSchema(NewSchema())
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestErrorRetrievingSchema(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client, _ := NewClient(uri)
	_, err = client.Schema()
	if err == nil {
		t.Fatal("should have failed")
	}
}

type ColumnGenerator struct {
	numRows    uint64
	numColumns uint64
	rowIndex   uint64
	colIndex   uint64
}

func (gen *ColumnGenerator) NextRecord() (Record, error) {
	column := Column{RowID: gen.rowIndex, ColumnID: gen.colIndex}
	if gen.rowIndex >= gen.numRows {
		return Column{}, io.EOF
	}
	gen.colIndex += 1
	if gen.colIndex >= gen.numColumns {
		gen.colIndex = 0
		gen.rowIndex += 1
	}
	return column, nil
}

// GivenColumnGenerator iterates over the set of columns provided in New().
// This is being used because Goveralls would run out of memory
// when providing ColumnGenerator with a large number of columns (3 * shardWidth).
type GivenColumnGenerator struct {
	ii      int
	records []Record
}

func (gen *GivenColumnGenerator) NextRecord() (Record, error) {
	if len(gen.records) > gen.ii {
		rec := gen.records[gen.ii]
		gen.ii++
		return rec, nil
	}
	return Column{}, io.EOF
}

func NewGivenColumnGenerator(recs []Record) *GivenColumnGenerator {
	return &GivenColumnGenerator{
		ii:      0,
		records: recs,
	}
}

func TestImportWithTimeout(t *testing.T) {
	client := getClient()
	iterator := &ColumnGenerator{numRows: 100, numColumns: 1000}
	field := index.Field("importfield-timeout")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	statusChan := make(chan ImportStatusUpdate, 10000)
	err = client.ImportField(field, iterator, OptImportStatusChannel(statusChan), OptImportThreadCount(8), OptImportStrategy(TimeoutImport), OptImportTimeout(10*time.Millisecond), OptImportBatchSize(1000))
	if err != nil {
		t.Fatal(err)
	}
}

func TestImportWithBatchSize(t *testing.T) {
	client := getClient()
	iterator := &ColumnGenerator{numRows: 10, numColumns: 1000}
	field := index.Field("importfield-batchsize")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	statusChan := make(chan ImportStatusUpdate, 10)
	err = client.ImportField(field, iterator, OptImportStatusChannel(statusChan), OptImportThreadCount(1), OptImportStrategy(BatchImport), OptImportBatchSize(1000))
	if err != nil {
		t.Fatal(err)
	}
}

// Ensure that the client does not send batches of zero records to Pilosa.
// In our case it should send:
// batch 1: shard[0,1]
// batch 2: shard[1,2]
// In other words, we want to ensure that batch 2 is not sending shard[0,1,2] where shard 0 contains 0 records.
func TestImportWithBatchSizeExpectingZero(t *testing.T) {
	const shardWidth = 1048576
	client := getClient()

	iterator := NewGivenColumnGenerator(
		[]Record{
			Column{RowID: 1, ColumnID: 1},
			Column{RowID: 1, ColumnID: 2},
			Column{RowID: 1, ColumnID: 3},
			Column{RowID: 1, ColumnID: shardWidth + 1},
			Column{RowID: 1, ColumnID: shardWidth + 2},
			Column{RowID: 1, ColumnID: shardWidth + 3},

			Column{RowID: 1, ColumnID: shardWidth + 4},
			Column{RowID: 1, ColumnID: shardWidth + 5},
			Column{RowID: 1, ColumnID: shardWidth + 6},
			Column{RowID: 1, ColumnID: 2*shardWidth + 1},
			Column{RowID: 1, ColumnID: 2*shardWidth + 2},
			Column{RowID: 1, ColumnID: 2*shardWidth + 3},
		},
	)

	field := index.Field("importfield-batchsize-zero")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	statusChan := make(chan ImportStatusUpdate, 10)
	err = client.ImportField(field, iterator, OptImportStatusChannel(statusChan), OptImportThreadCount(1), OptImportStrategy(BatchImport), OptImportBatchSize(6))
	if err != nil {
		t.Fatal(err)
	}
}

func failingImportColumns(field *Field, shard uint64, records []Record, nodes []fragmentNode) error {
	if len(records) > 0 {
		return errors.New("some error")
	}
	return nil
}

func TestImportWithTimeoutFails(t *testing.T) {
	client := getClient()
	iterator := &ColumnGenerator{numRows: 10, numColumns: 1000}
	field := index.Field("importfield-timeout")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	statusChan := make(chan ImportStatusUpdate, 10)
	err = client.ImportField(field, iterator, OptImportStatusChannel(statusChan), OptImportThreadCount(1), OptImportStrategy(TimeoutImport), OptImportTimeout(1*time.Millisecond), importRecordsFunction(failingImportColumns))
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestImportWithBatchSizeFails(t *testing.T) {
	client := getClient()
	iterator := &ColumnGenerator{numRows: 10, numColumns: 1000}
	field := index.Field("importfield-batchsize")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	statusChan := make(chan ImportStatusUpdate, 10)
	err = client.ImportField(field, iterator, OptImportStatusChannel(statusChan), OptImportThreadCount(1), OptImportStrategy(BatchImport), OptImportBatchSize(1000), importRecordsFunction(failingImportColumns))
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestExportReaderFailure(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	field := index.Field("exportfield")
	shardURIs := map[uint64]*URI{
		0: uri,
	}
	client, _ := NewClient(uri)
	reader := newExportReader(client, shardURIs, field)
	buf := make([]byte, 1000)
	_, err = reader.Read(buf)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestExportReaderReadBodyFailure(t *testing.T) {
	server := getMockServer(200, []byte("not important"), 100)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	field := index.Field("exportfield")
	shardURIs := map[uint64]*URI{0: uri}
	client, _ := NewClient(uri)
	reader := newExportReader(client, shardURIs, field)
	buf := make([]byte, 1000)
	_, err = reader.Read(buf)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestFetchFragmentNodes(t *testing.T) {
	client := getClient()
	nodes, err := client.fetchFragmentNodes(index.Name(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("1 node should be returned")
	}
	// running the same for coverage
	nodes, err = client.fetchFragmentNodes(index.Name(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("1 node should be returned")
	}
}

func TestFetchStatus(t *testing.T) {
	client := getClient()
	status, err := client.Status()
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Nodes) == 0 {
		t.Fatalf("There should be at least 1 host in the status")
	}
}

func TestRangeQuery(t *testing.T) {
	client := getClient()
	field := index.Field("test-rangefield", OptFieldTypeTime(TimeQuantumMonthDayHour))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(index.BatchQuery(
		field.SetTimestamp(10, 100, time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)),
		field.SetTimestamp(10, 100, time.Date(2018, time.January, 1, 0, 0, 0, 0, time.UTC)),
		field.SetTimestamp(10, 100, time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)),
	))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2017, time.January, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2018, time.January, 5, 0, 0, 0, 0, time.UTC)
	resp, err := client.Query(field.Range(10, start, end))
	if err != nil {
		t.Fatal(err)
	}
	target := []uint64{100}
	if !reflect.DeepEqual(resp.Result().Row().Columns, target) {
		t.Fatalf("%v != %v", target, resp.Result().Row().Columns)
	}
}

func TestRangeField(t *testing.T) {
	client := getClient()
	field := index.Field("rangefield", OptFieldTypeInt(10, 20))
	field2 := index.Field("rangefield-set")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field2)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Query(index.BatchQuery(
		field2.Set(1, 10),
		field2.Set(1, 100),
		field.SetIntValue(10, 11),
		field.SetIntValue(100, 15),
	))
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Query(field.Sum(field2.Row(1)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result().Value() != 26 {
		t.Fatalf("Sum 26 != %d", resp.Result().Value())
	}
	if resp.Result().Count() != 2 {
		t.Fatalf("Count 2 != %d", resp.Result().Count())
	}

	resp, err = client.Query(field.Min(field2.Row(1)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result().Value() != 11 {
		t.Fatalf("Min 11 != %d", resp.Result().Value())
	}
	if resp.Result().Count() != 1 {
		t.Fatalf("Count 1 != %d", resp.Result().Count())
	}

	resp, err = client.Query(field.Max(field2.Row(1)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result().Value() != 15 {
		t.Fatalf("Max 15 != %d", resp.Result().Value())
	}
	if resp.Result().Count() != 1 {
		t.Fatalf("Count 1 != %d", resp.Result().Count())
	}

	resp, err = client.Query(field.LT(15))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Result().Row().Columns) != 1 {
		t.Fatalf("Count 1 != %d", len(resp.Result().Row().Columns))
	}
	if resp.Result().Row().Columns[0] != 10 {
		t.Fatalf("Column 10 != %d", resp.Result().Row().Columns[0])
	}
}

func TestExcludeAttrsColumns(t *testing.T) {
	client := getClient()
	field := index.Field("excludecolumnsattrsfield")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	attrs := map[string]interface{}{
		"foo": "bar",
	}
	_, err = client.Query(index.BatchQuery(
		field.Set(1, 100),
		field.SetRowAttrs(1, attrs),
	))
	if err != nil {
		t.Fatal(err)
	}

	// test exclude columns.
	resp, err := client.Query(field.Row(1), &QueryOptions{ExcludeColumns: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Result().Row().Columns) != 0 {
		t.Fatalf("columns should be excluded")
	}
	if len(resp.Result().Row().Attributes) != 1 {
		t.Fatalf("attributes should be included")
	}

	// test exclude attributes.
	resp, err = client.Query(field.Row(1), &QueryOptions{ExcludeRowAttrs: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Result().Row().Columns) != 1 {
		t.Fatalf("columns should be included")
	}
	if len(resp.Result().Row().Attributes) != 0 {
		t.Fatalf("attributes should be excluded")
	}
}

func TestMultipleClientKeyQuery(t *testing.T) {
	client := getClient()
	field := keysIndex.Field("multiple-client-field")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}

	const goroutineCount = 10
	wg := &sync.WaitGroup{}
	wg.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func(rowID uint64) {
			client.Query(field.Set(rowID, "col"))
			wg.Done()
		}(uint64(i))
	}
	wg.Wait()
}

func TestImportFailsOnImportColumnsError(t *testing.T) {
	server := getMockServer(500, []byte{}, 0)
	defer server.Close()
	client, _ := NewClient(server.URL)
	index := NewIndex("foo")
	field := index.Field("bar")
	nodes := fragmentNodesFromURL(server.URL)
	err := client.importColumns(field, 0, []Record{}, nodes)
	if err == nil {
		t.Fatalf("importColumns should fail when fetch fragment nodes fails")
	}
}

func TestValueImportFailsOnImportValueError(t *testing.T) {
	server := getMockServer(500, []byte{}, 0)
	defer server.Close()
	client, _ := NewClient(server.URL)
	index := NewIndex("foo")
	field := index.Field("bar")
	nodes := fragmentNodesFromURL(server.URL)
	err := client.importValues(field, 0, nil, nodes)
	if err == nil {
		t.Fatalf("importValues should fail when fetch fragment nodes fails")
	}
}

func TestImportColumnsFailInvalidNodeAddress(t *testing.T) {
	data := []byte(`[{"host":"10101:","internalHost":"doesn'tmatter"}]`)
	server := getMockServer(200, data, len(data))
	defer server.Close()
	client, _ := NewClient(server.URL)
	index := NewIndex("foo")
	field := index.Field("bar")
	nodes := fragmentNodesFromURL("zzz://doesntmatter:10101")
	err := client.importColumns(field, 0, []Record{}, nodes)
	if err == nil {
		t.Fatalf("importColumns should fail on invalid node host")
	}
}

func TestImportValuesFailInvalidNodeAddress(t *testing.T) {
	data := []byte(`[{"host":"10101:","internalHost":"doesn'tmatter"}]`)
	server := getMockServer(200, data, len(data))
	defer server.Close()
	client, _ := NewClient(server.URL)
	index := NewIndex("foo")
	field := index.Field("bar")
	nodes := fragmentNodesFromURL("zzz://doesntmatter:10101")
	err := client.importValues(field, 0, nil, nodes)
	if err == nil {
		t.Fatalf("importValues should fail on invalid node host")
	}
}

func TestDecodingFragmentNodesFails(t *testing.T) {
	server := getMockServer(200, []byte("notjson"), 7)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.fetchFragmentNodes("foo", 0)
	if err == nil {
		t.Fatalf("fetchFragmentNodes should fail when response from /fragment/nodes cannot be decoded")
	}
}

func TestImportNodeFails(t *testing.T) {
	server := getMockServer(500, []byte{}, 0)
	defer server.Close()
	uri, _ := NewURIFromAddress(server.URL)
	client, _ := NewClient(uri)
	importRequest := &pbuf.ImportRequest{
		ColumnIDs:  []uint64{},
		RowIDs:     []uint64{},
		Timestamps: []int64{},
		Index:      "foo",
		Field:      "bar",
		Shard:      0,
	}
	err := client.importNode(uri, importRequest)
	if err == nil {
		t.Fatalf("importNode should fail when posting to /import fails")
	}
}

func TestImportNodeProtobufMarshalFails(t *testing.T) {
	// even though this function isn't really an integration test,
	// it needs to access importNode which is not
	// available to client_test.go
	client := getClient()
	uri, err := NewURIFromAddress("http://does-not-matter.foo.bar")
	if err != nil {
		t.Fatal(err)
	}
	err = client.importNode(uri, nil)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestQueryUnmarshalFails(t *testing.T) {
	server := getMockServer(200, []byte(`{}`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	field := NewSchema().Index("foo").Field("bar")
	_, err := client.Query(field.Row(1))
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func TestResponseWithInvalidType(t *testing.T) {
	qr := &pbuf.QueryResponse{
		Err: "",
		ColumnAttrSets: []*pbuf.ColumnAttrSet{
			{
				ID: 0,
				Attrs: []*pbuf.Attr{
					{
						Type:        9999,
						StringValue: "NOVAL",
					},
				},
			},
		},
		Results: []*pbuf.QueryResult{},
	}
	data, err := proto.Marshal(qr)
	if err != nil {
		t.Fatal(err)
	}
	server := getMockServer(200, data, -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err = client.Query(testField.Row(1))
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestStatusFails(t *testing.T) {
	server := getMockServer(404, nil, 0)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.Status()
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestStatusUnmarshalFails(t *testing.T) {
	server := getMockServer(200, []byte("foo"), 3)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.Status()
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestStatusToNodeShardsForIndex(t *testing.T) {
	client := getClient()
	status := Status{
		Nodes: []StatusNode{
			{
				URI: StatusURI{
					Scheme: "https",
					Host:   "localhost",
					Port:   10101,
				},
			},
		},
		indexMaxShard: map[string]uint64{
			index.Name(): 0,
		},
	}
	shardMap, err := client.statusToNodeShardsForIndex(status, index.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(shardMap) != 1 {
		t.Fatalf("len(shardMap) %d != %d", 1, len(shardMap))
	}
	if _, ok := shardMap[0]; !ok {
		t.Fatalf("shard map should have the correct shard")
	}
}

func TestHttpRequest(t *testing.T) {
	client := getClient()
	_, _, err := client.HttpRequest("GET", "/status", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSyncSchemaCantCreateIndex(t *testing.T) {
	server := getMockServer(404, nil, 0)
	defer server.Close()
	client, _ := NewClient(server.URL)
	schema = NewSchema()
	schema.Index("foo")
	err := client.syncSchema(schema, NewSchema())
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestSyncSchemaCantCreateField(t *testing.T) {
	server := getMockServer(404, nil, 0)
	defer server.Close()
	client, _ := NewClient(server.URL)
	schema = NewSchema()
	index := schema.Index("foo")
	index.Field("foofield")
	serverSchema := NewSchema()
	serverSchema.Index("foo")
	err := client.syncSchema(schema, serverSchema)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestExportFieldFailure(t *testing.T) {
	paths := map[string]mockResponseItem{
		"/status": {
			content:       []byte(`{"state":"NORMAL","nodes":[{"scheme":"http","host":"localhost","port":10101}]}`),
			statusCode:    404,
			contentLength: -1,
		},
		"/internal/shards/max": {
			content:       []byte(`{"standard":{"go-testindex": 0}}`),
			statusCode:    404,
			contentLength: -1,
		},
	}
	server := getMockPathServer(paths)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.ExportField(testField)
	if err == nil {
		t.Fatal("should have failed")
	}
	statusItem := paths["/status"]
	statusItem.statusCode = 200
	paths["/status"] = statusItem
	_, err = client.ExportField(testField)
	if err == nil {
		t.Fatal("should have failed")
	}
	statusItem = paths["/internal/shards/max"]
	statusItem.statusCode = 200
	paths["/internal/shards/max"] = statusItem
	_, err = client.ExportField(testField)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestShardsMaxDecodeFailure(t *testing.T) {
	server := getMockServer(200, []byte(`{`), 0)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.shardsMax()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestReadSchemaDecodeFailure(t *testing.T) {
	server := getMockServer(200, []byte(`{`), 0)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.readSchema()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestStatusToNodeShardsForIndexFailure(t *testing.T) {
	server := getMockServer(200, []byte(`[]`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	// no shard
	status := Status{
		indexMaxShard: map[string]uint64{},
	}
	_, err := client.statusToNodeShardsForIndex(status, "foo")
	if err == nil {
		t.Fatal("should have failed")
	}

	// no fragment nodes
	status = Status{
		indexMaxShard: map[string]uint64{
			"foo": 0,
		},
	}
	_, err = client.statusToNodeShardsForIndex(status, "foo")
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestUserAgent(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		version := Version
		if strings.HasPrefix(version, "v") {
			version = version[1:]
		}
		targetUserAgent := fmt.Sprintf("go-pilosa/%s", version)
		if targetUserAgent != r.UserAgent() {
			t.Fatalf("UserAgent %s != %s", targetUserAgent, r.UserAgent())
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, _, err := client.HttpRequest("GET", "/version", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientRace(t *testing.T) {
	uri, err := NewURIFromAddress(getPilosaBindAddress())
	if err != nil {
		panic(err)
	}
	client, err := NewClient(uri, OptClientTLSConfig(&tls.Config{InsecureSkipVerify: true}))
	if err != nil {
		panic(err)
	}
	f := func() {
		client.Query(testField.Row(1))
	}
	for i := 0; i < 10; i++ {
		go f()
	}
}

func TestImportFieldWithoutImportFunFails(t *testing.T) {
	client := DefaultClient()
	err := client.ImportField(&Field{}, nil, importRecordsFunction(nil))
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestFetchCoordinatorFails(t *testing.T) {
	server := getMockServer(404, []byte(`[]`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.fetchCoordinatorNode()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestFetchCoordinatorCoordinatorNotFound(t *testing.T) {
	server := getMockServer(200, []byte(`{"state":"NORMAL","nodes":[{"id":"0f5c2ffc-1244-47d0-a83d-f5a25abba9bc","uri":{"scheme":"http","host":"localhost","port":10101}}],"localID":"0f5c2ffc-1244-47d0-a83d-f5a25abba9bc"}`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.fetchCoordinatorNode()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestServerWarning(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content, err := proto.Marshal(&pbuf.QueryResponse{})
		if err != nil {
			t.Fatal(err)
		}
		w.Header().Set("warning", `299 pilosa/2.0 "FAKE WARNING: Deprecated PQL version: PQL v2 will remove support for SetBit() in Pilosa 2.1. Please update your client to support Set() (See https://docs.pilosa.com/pql#versioning)." "Sat, 25 Aug 2019 23:34:45 GMT"`)
		w.WriteHeader(200)
		io.Copy(w, bytes.NewReader(content))
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.Query(testField.Row(1))
	if err != nil {
		t.Fatal(err)
	}
}

func TestValueCSVImportFailure(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	uri := URIFromAddress(server.URL)
	err := client.importValueNode(uri, nil)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestRowIDColumnIDImport(t *testing.T) {
	client := getClient()
	iterator := NewArrayRecordIterator([]Record{
		Column{RowID: 10, ColumnID: 7},
		Column{RowID: 10, ColumnID: 5},
		Column{RowID: 2, ColumnID: 3},
		Column{RowID: 7, ColumnID: 1},
	})
	field := index.Field("importfield-rowid-colid")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	err = client.ImportField(field, iterator)
	if err != nil {
		t.Fatal(err)
	}

	target := []uint64{3, 1, 5}
	bq := index.BatchQuery(
		field.Row(2),
		field.Row(7),
		field.Row(10),
	)
	response, err := client.Query(bq)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results()) != 3 {
		t.Fatalf("Result count should be 3")
	}
	for i, result := range response.Results() {
		br := result.Row()
		if len(br.Columns) < 1 {
			t.Fatalf("1 or more keys should be returned")
		}
		if target[i] != br.Columns[0] {
			t.Fatalf("%d != %d", target[i], br.Columns[0])
		}
	}
}

func TestRowIDColumnIDImportFails(t *testing.T) {
	server := getMockServer(200, []byte(`{}`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	iterator := NewArrayRecordIterator([]Record{
		Column{RowID: 10, ColumnID: 7},
		Column{RowID: 10, ColumnID: 5},
		Column{RowID: 2, ColumnID: 3},
		Column{RowID: 7, ColumnID: 1},
	})
	field := NewSchema().Index("foo").Field("bar")
	err := client.ImportField(field, iterator)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestCSVRowIDColumnKeyImport(t *testing.T) {
	client := getClient()
	iterator := NewArrayRecordIterator([]Record{
		Column{RowID: 10, ColumnKey: "five"},
		Column{RowID: 2, ColumnKey: "three"},
		Column{RowID: 7, ColumnKey: "one"},
	})
	field := keysIndex.Field("importfield-rowid-colkey")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	err = client.ImportField(field, iterator)
	if err != nil {
		t.Fatal(err)
	}

	target := []string{"three", "one", "five"}
	bq := keysIndex.BatchQuery(
		field.Row(2),
		field.Row(7),
		field.Row(10),
	)

	response, err := client.Query(bq)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results()) != 3 {
		t.Fatalf("Result count should be 3")
	}
	for i, result := range response.Results() {
		br := result.Row()
		if len(br.Keys) < 1 {
			t.Fatalf("1 or more keys should be returned")
		}
		if target[i] != br.Keys[0] {
			t.Fatalf("%s != %s", target[i], br.Keys[0])
		}
	}
}

func TestRowIDColumnKeyImportFails(t *testing.T) {
	server := getMockServer(200, []byte(`{}`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	iterator := NewArrayRecordIterator([]Record{
		Column{RowID: 10, ColumnKey: "five"},
		Column{RowID: 2, ColumnKey: "three"},
		Column{RowID: 7, ColumnKey: "one"},
	})
	field := NewSchema().Index("foo", OptIndexKeys(true)).Field("bar")
	err := client.ImportField(field, iterator)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestRowKeyColumnIDImport(t *testing.T) {
	client := getClient()
	iterator := NewArrayRecordIterator([]Record{
		Column{RowKey: "ten", ColumnID: 7},
		Column{RowKey: "ten", ColumnID: 5},
		Column{RowKey: "two", ColumnID: 3},
		Column{RowKey: "seven", ColumnID: 1},
	})
	field := index.Field("importfield-rowkey-colid", OptFieldKeys(true))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	err = client.ImportField(field, iterator)
	if err != nil {
		t.Fatal(err)
	}

	target := []uint64{3, 1, 5}
	bq := index.BatchQuery(
		field.Row("two"),
		field.Row("seven"),
		field.Row("ten"),
	)
	response, err := client.Query(bq)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results()) != 3 {
		t.Fatalf("Result count should be 3")
	}
	for i, result := range response.Results() {
		br := result.Row()
		if len(br.Columns) < 1 {
			t.Fatalf("1 or more columns should be returned")
		}
		if target[i] != br.Columns[0] {
			t.Fatalf("%d != %d", target[i], br.Columns[0])
		}
	}
}

func TestRowKeyColumnKeyImport(t *testing.T) {
	client := getClient()
	iterator := NewArrayRecordIterator([]Record{
		Column{RowKey: "ten", ColumnKey: "five"},
		Column{RowKey: "two", ColumnKey: "three"},
		Column{RowKey: "seven", ColumnKey: "one"},
	})
	field := keysIndex.Field("importfield-rowkey-colkey", OptFieldKeys(true))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	err = client.ImportField(field, iterator)
	if err != nil {
		t.Fatal(err)
	}

	target := []string{"three", "one", "five"}
	bq := keysIndex.BatchQuery(
		field.Row("two"),
		field.Row("seven"),
		field.Row("ten"),
	)
	response, err := client.Query(bq)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results()) != 3 {
		t.Fatalf("Result count should be 3")
	}
	for i, result := range response.Results() {
		br := result.Row()
		if len(br.Keys) < 1 {
			t.Fatalf("1 or more keys should be returned")
		}
		if target[i] != br.Keys[0] {
			t.Fatalf("%s != %s", target[i], br.Keys[0])
		}
	}
}

func TestValueFieldImport(t *testing.T) {
	client := getClient()
	iterator := NewArrayRecordIterator([]Record{
		FieldValue{ColumnID: 10, Value: 7},
		FieldValue{ColumnID: 7, Value: 1},
	})
	field := index.Field("importvaluefield", OptFieldTypeInt(0, 100))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	field2 := index.Field("importvaluefield-set")
	err = client.EnsureField(field2)
	if err != nil {
		t.Fatal(err)
	}
	bq := index.BatchQuery(
		field2.Set(1, 10),
		field2.Set(1, 7),
	)
	response, err := client.Query(bq)
	if err != nil {
		t.Fatal(err)
	}
	err = client.ImportField(field, iterator)
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.Query(field.Sum(field2.Row(1)))
	if err != nil {
		t.Fatal(err)
	}
	target := int64(8)
	if target != response.Result().Value() {
		t.Fatalf("%d != %#v", target, response.Result())
	}
}

func TestValueFieldWithKeysImport(t *testing.T) {
	client := getClient()
	iterator := NewArrayRecordIterator([]Record{
		FieldValue{ColumnKey: "ten", Value: 7},
		FieldValue{ColumnKey: "seven", Value: 1},
	})
	field := keysIndex.Field("importvaluefield", OptFieldTypeInt(0, 100))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	field2 := keysIndex.Field("importvaluefield-set")
	err = client.EnsureField(field2)
	if err != nil {
		t.Fatal(err)
	}
	bq := keysIndex.BatchQuery(
		field2.Set(1, "ten"),
		field2.Set(1, "seven"),
	)
	response, err := client.Query(bq)
	if err != nil {
		t.Fatal(err)
	}
	err = client.ImportField(field, iterator)
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.Query(field.Sum(field2.Row(1)))
	if err != nil {
		t.Fatal(err)
	}
	target := int64(8)
	if target != response.Result().Value() {
		t.Fatalf("%d != %#v", target, response.Result().Value())
	}
}

func TestExportRowIDColumnID(t *testing.T) {
	client := getClient()
	field := index.Field("exportfield-rowid-colid")
	client.EnsureField(field)
	_, err := client.Query(index.BatchQuery(
		field.Set(1, 1),
		field.Set(1, 10),
		field.Set(2, 1048577),
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.ExportField(field)
	if err != nil {
		t.Fatal(err)
	}
	s := consumeReader(t, r)
	target := "1,1\n1,10\n2,1048577\n"
	if target != s {
		t.Fatalf("%s != %s", target, s)
	}
}

func TestExportRowIDColumnKey(t *testing.T) {
	client := getClient()
	field := keysIndex.Field("exportfield-rowid-colkey")
	client.EnsureField(field)
	_, err := client.Query(keysIndex.BatchQuery(
		field.Set(1, "one"),
		field.Set(1, "ten"),
		field.Set(2, "big-number"),
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.ExportField(field)
	if err != nil {
		t.Fatal(err)
	}
	s := consumeReader(t, r)
	target := "1,one\n1,ten\n2,big-number\n"
	if target != s {
		t.Fatalf("%s != %s", target, s)
	}
}

func TestExportRowKeyColumnID(t *testing.T) {
	client := getClient()
	field := index.Field("exportfield-rowkey-colid", OptFieldKeys(true))
	client.EnsureField(field)
	_, err := client.Query(index.BatchQuery(
		field.Set("one", 1),
		field.Set("one", 10),
		field.Set("two", 1048577),
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.ExportField(field)
	if err != nil {
		t.Fatal(err)
	}
	s := consumeReader(t, r)
	target := "one,1\none,10\ntwo,1048577\n"
	if target != s {
		t.Fatalf("%s != %s", target, s)
	}
}

func TestExportRowKeyColumnKey(t *testing.T) {
	client := getClient()
	field := keysIndex.Field("exportfield-rowkey-colkey", OptFieldKeys(true))
	client.EnsureField(field)
	_, err := client.Query(keysIndex.BatchQuery(
		field.Set("one", "one"),
		field.Set("one", "ten"),
		field.Set("two", "big-number"),
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.ExportField(field)
	if err != nil {
		t.Fatal(err)
	}
	s := consumeReader(t, r)
	target := "one,one\none,ten\ntwo,big-number\n"
	if target != s {
		t.Fatalf("%s != %s", target, s)
	}
}

func TestCSVExportFailure(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	field := index.Field("exportfield")
	_, err := client.ExportField(field)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestImportColumnIteratorError(t *testing.T) {
	client := getClient()
	field := index.Field("not-important")
	iterator := &BrokenRecordIterator{}
	err := client.ImportField(field, iterator)
	if err == nil {
		t.Fatalf("import field should fail with broken reader")
	}
}

func TestErrorReturningImportOption(t *testing.T) {
	iterator := NewArrayRecordIterator([]Record{})
	field := index.Field("importfield")
	client := getClient()
	optionErr := errors.New("ERR")
	err := client.ImportField(field, iterator, ErrorImportOption(optionErr))
	if err != optionErr {
		t.Fatal("ImportField should fail if an import option fails")
	}
}

func getMockServer(statusCode int, response []byte, contentLength int) *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		if contentLength >= 0 {
			w.Header().Set("Content-Length", strconv.Itoa(contentLength))
		}
		w.WriteHeader(statusCode)
		if response != nil {
			io.Copy(w, bytes.NewReader(response))
		}
	})
	return httptest.NewServer(handler)
}

func fragmentNodesFromURL(url string) []fragmentNode {
	serverURI := URIFromAddress(url)
	nodes := []fragmentNode{
		{
			Scheme: serverURI.Scheme(),
			Host:   serverURI.Host(),
			Port:   serverURI.Port(),
		},
	}
	return nodes
}

type mockResponseItem struct {
	content       []byte
	contentLength int
	statusCode    int
}

func getMockPathServer(responses map[string]mockResponseItem) *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-protobuf")
		if item, ok := responses[r.RequestURI]; ok {
			if item.contentLength >= 0 {
				w.Header().Set("Content-Length", strconv.Itoa(item.contentLength))
			} else {
				w.Header().Set("Content-Length", strconv.Itoa(len(item.content)))
			}
			statusCode := item.statusCode
			if statusCode == 0 {
				statusCode = 200
			}
			w.WriteHeader(statusCode)
			if item.content != nil {
				io.Copy(w, bytes.NewReader(item.content))
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
		io.Copy(w, bytes.NewReader([]byte("not found")))
	})
	return httptest.NewServer(handler)
}

func getClient() *Client {
	var client *Client
	var err error
	uri, err := NewURIFromAddress(getPilosaBindAddress())
	if err != nil {
		panic(err)
	}
	client, err = NewClient(uri,
		OptClientTLSConfig(&tls.Config{InsecureSkipVerify: true}),
	)
	if err != nil {
		panic(err)
	}
	return client
}

func getPilosaBindAddress() string {
	for _, kvStr := range os.Environ() {
		kv := strings.SplitN(kvStr, "=", 2)
		if kv[0] == "PILOSA_BIND" {
			return kv[1]
		}
	}
	return "http://:10101"
}

type ArrayRecordIterator struct {
	records   []Record
	nextIndex int
}

func NewArrayRecordIterator(records []Record) *ArrayRecordIterator {
	return &ArrayRecordIterator{
		records: records,
	}
}

func (ar *ArrayRecordIterator) NextRecord() (Record, error) {
	if ar.nextIndex >= len(ar.records) {
		return nil, io.EOF
	}
	rec := ar.records[ar.nextIndex]
	ar.nextIndex += 1
	return rec, nil
}

type BrokenRecordIterator struct{}

func (it *BrokenRecordIterator) NextRecord() (Record, error) {
	return nil, errors.New("some error")
}

func ErrorImportOption(err error) ImportOption {
	return func(options *ImportOptions) error {
		return err
	}
}

func consumeReader(t *testing.T, r io.Reader) string {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
