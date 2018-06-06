// +build integration

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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	pbuf "github.com/pilosa/go-pilosa/gopilosa_pbuf"
	"github.com/pkg/errors"
)

var index *Index
var testField *Field

func TestMain(m *testing.M) {
	var err error
	index, err = NewIndex("go-testindex")
	if err != nil {
		panic(err)
	}
	testField, err = index.Field("test-field")
	if err != nil {
		panic(err)
	}

	Setup()
	r := m.Run()
	TearDown()
	os.Exit(r)
}

func Setup() {
	client := getClient()
	err := client.EnsureIndex(index)
	if err != nil {
		panic(err)
	}
	err = client.EnsureField(testField)
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
	response, err := client.Query(testField.Bitmap(1), nil)
	if err != nil {
		t.Fatalf("Error querying: %s", err)
	}
	if response == nil {
		t.Fatalf("Response should not be nil")
	}
}

func TestQueryWithSlices(t *testing.T) {
	Reset()
	const sliceWidth = 1048576
	client := getClient()
	if _, err := client.Query(testField.SetBit(1, 100)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(testField.SetBit(1, sliceWidth)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(testField.SetBit(1, sliceWidth*3)); err != nil {
		t.Fatal(err)
	}

	response, err := client.Query(testField.Bitmap(1), Slices(0, 3))
	if err != nil {
		t.Fatal(err)
	}
	if bits := response.Result().Bitmap().Bits; !reflect.DeepEqual(bits, []uint64{100, sliceWidth * 3}) {
		t.Fatalf("Unexpected results: %#v", bits)
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
	_, err := client.Query(testField.SetBit(1, 100), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(index.SetColumnAttrs(100, targetAttrs), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(response.Column(), ColumnItem{}) {
		t.Fatalf("No columns should be returned if it wasn't explicitly requested")
	}
	response, err = client.Query(testField.Bitmap(1), &QueryOptions{Columns: true})
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
	_, err := client.Query(testField.SetBit(1, 100), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(testField.SetRowAttrs(1, targetAttrs), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(testField.Bitmap(1), &QueryOptions{Columns: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(targetAttrs, response.Result().Bitmap().Attributes) {
		t.Fatalf("Bitmap attributes should be set")
	}
}

func TestOrmCount(t *testing.T) {
	client := getClient()
	countField, err := index.Field("count-test", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(countField)
	if err != nil {
		t.Fatal(err)
	}
	qry := index.BatchQuery(
		countField.SetBit(10, 20),
		countField.SetBit(10, 21),
		countField.SetBit(15, 25),
	)
	client.Query(qry, nil)
	response, err := client.Query(index.Count(countField.Bitmap(10)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.Result().Count() != 2 {
		t.Fatalf("Count should be 2")
	}
}

func TestIntersectReturns(t *testing.T) {
	client := getClient()
	field, err := index.Field("segments")
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	qry1 := index.BatchQuery(
		field.SetBit(2, 10),
		field.SetBit(2, 15),
		field.SetBit(3, 10),
		field.SetBit(3, 20),
	)
	client.Query(qry1, nil)
	qry2 := index.Intersect(field.Bitmap(2), field.Bitmap(3))
	response, err := client.Query(qry2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results()) != 1 {
		t.Fatal("There must be 1 result")
	}
	if !reflect.DeepEqual(response.Result().Bitmap().Bits, []uint64{10}) {
		t.Fatal("Returned bits must be: [10]")
	}
}

func TestTopNReturns(t *testing.T) {
	client := getClient()
	field, err := index.Field("topn_test", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	qry := index.BatchQuery(
		field.SetBit(10, 5),
		field.SetBit(10, 10),
		field.SetBit(10, 15),
		field.SetBit(20, 5),
		field.SetBit(30, 5),
	)
	client.Query(qry, nil)
	// XXX: The following is required to make this test pass. See: https://github.com/pilosa/pilosa/issues/625
	client.HttpRequest("POST", "/recalculate-caches", nil, nil)
	response, err := client.Query(field.TopN(2), nil)
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
}

func TestCreateDeleteIndexField(t *testing.T) {
	client := getClient()
	index1, err := NewIndex("to-be-deleted")
	if err != nil {
		panic(err)
	}
	field1, err := index1.Field("foo")
	err = client.CreateIndex(index1)
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
	field, err := index.Field("field-with-timequantum", OptFieldTime(TimeQuantumYear))
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateField(field)
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
	_, err := client.Query(index.RawQuery("won't run"), nil)
	if err != ErrEmptyCluster {
		t.Fatal(err)
	}
}

func TestMaxHostsFail(t *testing.T) {
	uri, _ := NewURIFromAddress("does-not-resolve.foo.bar")
	cluster := NewClusterWithHost(uri, uri, uri, uri)
	client, _ := NewClient(cluster)
	_, err := client.Query(index.RawQuery("foo"), nil)
	if err != ErrTriedMaxHosts {
		t.Fatalf("ErrTriedMaxHosts error should be returned")
	}
}

func TestQueryFailsIfAddressNotResolved(t *testing.T) {
	uri, _ := NewURIFromAddress("nonexisting.domain.pilosa.com:3456")
	client, _ := NewClient(uri)
	_, err := client.Query(index.RawQuery("bar"), nil)
	if err == nil {
		t.Fatal()
	}
}

func TestQueryFails(t *testing.T) {
	client := getClient()
	_, err := client.Query(index.RawQuery("Invalid query"), nil)
	if err == nil {
		t.Fatal()
	}
}

func TestInvalidHttpRequest(t *testing.T) {
	client := getClient()
	_, _, err := client.httpRequest("INVALID METHOD", "/foo", nil, nil)
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
	response, err := client.Query(testField.Bitmap(1), nil)
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
	response, err := client.Query(testField.Bitmap(1), nil)
	if err == nil {
		t.Fatalf("Got response: %v", response)
	}
}

func TestInvalidResponse(t *testing.T) {
	server := getMockServer(200, []byte("unmarshal this!"), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	response, err := client.Query(index.RawQuery("don't care"), nil)
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
	f, err := index.Field("schema-test-field",
		CacheTypeLRU,
		OptFieldCacheSize(9999),
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
	remoteIndex, _ := NewIndex("remote-index-1")
	err := client.EnsureIndex(remoteIndex)
	if err != nil {
		t.Fatal(err)
	}
	remoteField, _ := remoteIndex.Field("remote-field-1")
	err = client.EnsureField(remoteField)
	if err != nil {
		t.Fatal(err)
	}
	schema1 := NewSchema()
	index11, _ := schema1.Index("diff-index1")
	index11.Field("field1-1")
	index11.Field("field1-2")
	index12, _ := schema1.Index("diff-index2")
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

func TestInvalidSchemaInvalidIndex(t *testing.T) {
	data := []byte(`
		{
			"indexes": [{
				"Name": "**INVALID**"
			}]
		}
	`)
	server := getMockServer(200, data, len(data))
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

func TestInvalidSchemaInvalidField(t *testing.T) {
	data := []byte(`
		{
			"indexes": [{
				"name": "myindex",
				"frames": [{
					"name": "**INVALID**"
				}]
			}]
		}
	`)
	server := getMockServer(200, data, len(data))
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.Schema()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestCSVImport(t *testing.T) {
	client := getClient()
	text := `10,7
		10,5
		2,3
		7,1`
	iterator := NewCSVBitIterator(strings.NewReader(text))
	field, err := index.Field("importfield")
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	// TODO: This test doesn't actually use batch or time import strategy because it falls through to the
	// "remaining records" part of import_manager.recordImportWorker. These functional options are misleading.
	err = client.ImportField(field, iterator, OptImportBatchSize(10), OptImportThreadCount(1), OptImportTimeout(400*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	target := []uint64{3, 1, 5}
	bq := index.BatchQuery(
		field.Bitmap(2),
		field.Bitmap(7),
		field.Bitmap(10),
	)
	response, err := client.Query(bq)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results()) != 3 {
		t.Fatalf("Result count should be 3")
	}
	for i, result := range response.Results() {
		br := result.Bitmap()
		if target[i] != br.Bits[0] {
			t.Fatalf("%d != %d", target[i], br.Bits[0])
		}
	}
}

type BitGenerator struct {
	numRows    uint64
	numColumns uint64
	rowIndex   uint64
	colIndex   uint64
}

func (gen *BitGenerator) NextRecord() (Record, error) {
	bit := Bit{RowID: gen.rowIndex, ColumnID: gen.colIndex}
	if gen.rowIndex >= gen.numRows {
		return Bit{}, io.EOF
	}
	gen.colIndex += 1
	if gen.colIndex >= gen.numColumns {
		gen.colIndex = 0
		gen.rowIndex += 1
	}
	return bit, nil
}

// GivenBitGenerator iterates over the set of bits provided in New().
// This is being used because Goveralls would run out of memory
// when providing BitGenerator with a large number of columns (3 * sliceWidth).
type GivenBitGenerator struct {
	ii      int
	records []Record
}

func (gen *GivenBitGenerator) NextRecord() (Record, error) {
	if len(gen.records) > gen.ii {
		rec := gen.records[gen.ii]
		gen.ii++
		return rec, nil
	}
	return Bit{}, io.EOF
}

func NewGivenBitGenerator(recs []Record) *GivenBitGenerator {
	return &GivenBitGenerator{
		ii:      0,
		records: recs,
	}
}

func TestImportWithTimeout(t *testing.T) {
	client := getClient()
	iterator := &BitGenerator{numRows: 100, numColumns: 1000}
	field, err := index.Field("importfield-timeout")
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field)
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
	iterator := &BitGenerator{numRows: 10, numColumns: 1000}
	field, err := index.Field("importfield-batchsize")
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field)
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
// batch 1: slice[0,1]
// batch 2: slice[1,2]
// In other words, we want to ensure that batch 2 is not sending slice[0,1,2] where slice 0 contains 0 records.
func TestImportWithBatchSizeExpectingZero(t *testing.T) {
	const sliceWidth = 1048576
	client := getClient()

	iterator := NewGivenBitGenerator(
		[]Record{
			Bit{RowID: 1, ColumnID: 1},
			Bit{RowID: 1, ColumnID: 2},
			Bit{RowID: 1, ColumnID: 3},
			Bit{RowID: 1, ColumnID: sliceWidth + 1},
			Bit{RowID: 1, ColumnID: sliceWidth + 2},
			Bit{RowID: 1, ColumnID: sliceWidth + 3},

			Bit{RowID: 1, ColumnID: sliceWidth + 4},
			Bit{RowID: 1, ColumnID: sliceWidth + 5},
			Bit{RowID: 1, ColumnID: sliceWidth + 6},
			Bit{RowID: 1, ColumnID: 2*sliceWidth + 1},
			Bit{RowID: 1, ColumnID: 2*sliceWidth + 2},
			Bit{RowID: 1, ColumnID: 2*sliceWidth + 3},
		},
	)

	field, err := index.Field("importfield-batchsize-zero")
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	statusChan := make(chan ImportStatusUpdate, 10)
	err = client.ImportField(field, iterator, OptImportStatusChannel(statusChan), OptImportThreadCount(1), OptImportStrategy(BatchImport), OptImportBatchSize(6))
	if err != nil {
		t.Fatal(err)
	}
}

func failingImportBits(indexName string, fieldName string, slice uint64, bits []Record) error {
	if len(bits) > 0 {
		return errors.New("some error")
	}
	return nil
}

func TestImportWithTimeoutFails(t *testing.T) {
	client := getClient()
	iterator := &BitGenerator{numRows: 10, numColumns: 1000}
	field, err := index.Field("importfield-timeout")
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	statusChan := make(chan ImportStatusUpdate, 10)
	err = client.ImportField(field, iterator, OptImportStatusChannel(statusChan), OptImportThreadCount(1), OptImportStrategy(TimeoutImport), OptImportTimeout(1*time.Millisecond), importRecordsFunction(failingImportBits))
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestImportWithBatchSizeFails(t *testing.T) {
	client := getClient()
	iterator := &BitGenerator{numRows: 10, numColumns: 1000}
	field, err := index.Field("importfield-batchsize")
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	statusChan := make(chan ImportStatusUpdate, 10)
	err = client.ImportField(field, iterator, OptImportStatusChannel(statusChan), OptImportThreadCount(1), OptImportStrategy(BatchImport), OptImportBatchSize(1000), importRecordsFunction(failingImportBits))
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func ErrorImportOption(err error) ImportOption {
	return func(options *ImportOptions) error {
		return err
	}
}
func TestErrorReturningImportOption(t *testing.T) {
	text := `10,7
		10,5
		2,3
		7,1`
	iterator := NewCSVBitIterator(strings.NewReader(text))
	field, err := index.Field("importfield")
	if err != nil {
		t.Fatal(err)
	}
	client := getClient()
	optionErr := errors.New("ERR")
	err = client.ImportField(field, iterator, ErrorImportOption(optionErr))
	if err != optionErr {
		t.Fatal("ImportField should fail if an import option fails")
	}
}

func TestValueCSVImport(t *testing.T) {
	client := getClient()
	text := `10,7
		7,1`
	iterator := NewCSVValueIterator(strings.NewReader(text))
	field, err := index.Field("importvaluefield", OptFieldInt(0, 100))
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	bq := index.BatchQuery(
		field.SetBit(1, 10),
		field.SetBit(1, 7),
	)
	response, err := client.Query(bq)
	if err != nil {
		t.Fatal(err)
	}
	err = client.ImportIntField(field, iterator, OptImportBatchSize(10))
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.Query(field.Sum(field.Bitmap(1)))
	if err != nil {
		t.Fatal(err)
	}
	target := int64(8)
	if target != response.Result().Value() {
		t.Fatalf("%d != %d", target, response.Result().Value())
	}
}

func TestCSVExport(t *testing.T) {
	client := getClient()
	field, err := index.Field("exportfield")
	if err != nil {
		t.Fatal(err)
	}
	client.EnsureField(field)
	_, err = client.Query(index.BatchQuery(
		field.SetBit(1, 1),
		field.SetBit(1, 10),
		field.SetBit(2, 1048577),
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	target := []Bit{
		{RowID: 1, ColumnID: 1},
		{RowID: 1, ColumnID: 10},
		{RowID: 2, ColumnID: 1048577},
	}
	bits := []Record{}
	iterator, err := client.ExportField(field)
	if err != nil {
		t.Fatal(err)
	}
	for {
		bit, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		bits = append(bits, bit)
	}
	if len(bits) != len(target) {
		t.Fatalf("There should be %d bits", len(target))
	}
	for i := range target {
		if !reflect.DeepEqual(target[i], bits[i]) {
			t.Fatalf("%v != %v", target, bits)
		}
	}
}

func TestCSVExportFailure(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	field, err := index.Field("exportfield", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ExportField(field)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestExportReaderFailure(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	field, err := index.Field("exportfield", nil)
	if err != nil {
		t.Fatal(err)
	}
	sliceURIs := map[uint64]*URI{
		0: uri,
	}
	client, _ := NewClient(uri)
	reader := newExportReader(client, sliceURIs, field)
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
	field, err := index.Field("exportfield", nil)
	if err != nil {
		t.Fatal(err)
	}
	sliceURIs := map[uint64]*URI{0: uri}
	client, _ := NewClient(uri)
	reader := newExportReader(client, sliceURIs, field)
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

func TestRangeField(t *testing.T) {
	client := getClient()
	field, _ := index.Field("rangefield", OptFieldInt(10, 20))
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Query(index.BatchQuery(
		field.SetBit(1, 10),
		field.SetBit(1, 100),
		field.SetIntValue(10, 11),
		field.SetIntValue(100, 15),
	), nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Query(field.Sum(field.Bitmap(1)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result().Value() != 26 {
		t.Fatalf("Sum 26 != %d", resp.Result().Value())
	}
	if resp.Result().Count() != 2 {
		t.Fatalf("Count 2 != %d", resp.Result().Count())
	}

	resp, err = client.Query(field.Min(field.Bitmap(1)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result().Value() != 11 {
		t.Fatalf("Min 11 != %d", resp.Result().Value())
	}
	if resp.Result().Count() != 1 {
		t.Fatalf("Count 2 != %d", resp.Result().Count())
	}

	resp, err = client.Query(field.Max(field.Bitmap(1)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Result().Value() != 15 {
		t.Fatalf("Max 15 != %d", resp.Result().Value())
	}
	if resp.Result().Count() != 1 {
		t.Fatalf("Count 2 != %d", resp.Result().Count())
	}

	resp, err = client.Query(field.LT(15))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Result().Bitmap().Bits) != 1 {
		t.Fatalf("Count 1 != %d", len(resp.Result().Bitmap().Bits))
	}
	if resp.Result().Bitmap().Bits[0] != 10 {
		t.Fatalf("Bit 10 != %d", resp.Result().Bitmap().Bits[0])
	}
}

func TestExcludeAttrsBits(t *testing.T) {
	client := getClient()
	field, _ := index.Field("excludebitsattrsfield")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	attrs := map[string]interface{}{
		"foo": "bar",
	}
	_, err = client.Query(index.BatchQuery(
		field.SetBit(1, 100),
		field.SetRowAttrs(1, attrs),
	), nil)
	if err != nil {
		t.Fatal(err)
	}

	// test exclude bits.
	resp, err := client.Query(field.Bitmap(1), &QueryOptions{ExcludeBits: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Result().Bitmap().Bits) != 0 {
		t.Fatalf("bits should be excluded")
	}
	if len(resp.Result().Bitmap().Attributes) != 1 {
		t.Fatalf("attributes should be included")
	}

	// test exclude attributes.
	resp, err = client.Query(field.Bitmap(1), &QueryOptions{ExcludeAttrs: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Result().Bitmap().Bits) != 1 {
		t.Fatalf("bits should be included")
	}
	if len(resp.Result().Bitmap().Attributes) != 0 {
		t.Fatalf("attributes should be excluded")
	}
}

func TestImportBitIteratorError(t *testing.T) {
	client := getClient()
	field, err := index.Field("not-important", nil)
	if err != nil {
		t.Fatal(err)
	}
	iterator := NewCSVBitIterator(&BrokenReader{})
	err = client.ImportField(field, iterator)
	if err == nil {
		t.Fatalf("import field should fail with broken reader")
	}
}

func TestImportValueIteratorError(t *testing.T) {
	client := getClient()
	field, err := index.Field("not-important", nil)
	if err != nil {
		t.Fatal(err)
	}
	iterator := NewCSVValueIterator(&BrokenReader{})
	err = client.ImportIntField(field, iterator, OptImportBatchSize(100))
	if err == nil {
		t.Fatalf("import value field should fail with broken reader")
	}
}

func TestImportFailsOnImportBitsError(t *testing.T) {
	server := getMockServer(500, []byte{}, 0)
	defer server.Close()
	client, _ := NewClient(server.URL)
	err := client.importBits("foo", "bar", 0, []Record{})
	if err == nil {
		t.Fatalf("importBits should fail when fetch fragment nodes fails")
	}
}

func TestValueImportFailsOnImportValueError(t *testing.T) {
	server := getMockServer(500, []byte{}, 0)
	defer server.Close()
	client, _ := NewClient(server.URL)
	err := client.importValues("foo", "bar", 0, nil)
	if err == nil {
		t.Fatalf("importValues should fail when fetch fragment nodes fails")
	}
}

func TestImportFieldFailsIfImportBitsFails(t *testing.T) {
	data := []byte(`[{"host":"non-existing-domain:9999","internalHost":"10101"}]`)
	server := getMockServer(200, data, len(data))
	defer server.Close()
	client, _ := NewClient(server.URL)
	iterator := NewCSVBitIterator(strings.NewReader("10,7"))
	field, err := index.Field("importfield", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = client.ImportField(field, iterator)
	if err == nil {
		t.Fatalf("ImportField should fail if importBits fails")
	}
}

func TestImportIntFieldFailsIfImportValuesFails(t *testing.T) {
	data := []byte(`[{"host":"non-existing-domain:9999","internalHost":"10101"}]`)
	server := getMockServer(200, data, len(data))
	defer server.Close()
	client, _ := NewClient(server.URL)
	iterator := NewCSVValueIterator(strings.NewReader("10,7"))
	field, err := index.Field("importfield", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = client.ImportIntField(field, iterator, OptImportBatchSize(10))
	if err == nil {
		t.Fatalf("ImportIntField should fail if importValues fails")
	}
}

func TestImportBitsFailInvalidNodeAddress(t *testing.T) {
	data := []byte(`[{"host":"10101:","internalHost":"doesn'tmatter"}]`)
	server := getMockServer(200, data, len(data))
	defer server.Close()
	client, _ := NewClient(server.URL)
	err := client.importBits("foo", "bar", 0, []Record{})
	if err == nil {
		t.Fatalf("importBits should fail on invalid node host")
	}
}

func TestImportValuesFailInvalidNodeAddress(t *testing.T) {
	data := []byte(`[{"host":"10101:","internalHost":"doesn'tmatter"}]`)
	server := getMockServer(200, data, len(data))
	defer server.Close()
	client, _ := NewClient(server.URL)
	err := client.importValues("foo", "bar", 0, nil)
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
		Frame:      "bar",
		Slice:      0,
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
	_, err = client.Query(testField.Bitmap(1), nil)
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

func TestStatusToNodeSlicesForIndex(t *testing.T) {
	client := getClient()
	status := Status{
		Nodes: []StatusNode{
			{
				Scheme: "https",
				Host:   "localhost",
				Port:   10101,
			},
		},
		indexMaxSlice: map[string]uint64{
			index.Name(): 0,
		},
	}
	sliceMap, err := client.statusToNodeSlicesForIndex(status, index.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(sliceMap) != 1 {
		t.Fatalf("len(sliceMap) %d != %d", 1, len(sliceMap))
	}
	if _, ok := sliceMap[0]; !ok {
		t.Fatalf("slice map should have the correct slice")
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
	index, _ := schema.Index("foo")
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
		"/slices/max": {
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
	statusItem = paths["/slices/max"]
	statusItem.statusCode = 200
	paths["/slices/max"] = statusItem
	_, err = client.ExportField(testField)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestSlicesMaxDecodeFailure(t *testing.T) {
	server := getMockServer(200, []byte(`{`), 0)
	defer server.Close()
	client, _ := NewClient(server.URL)
	_, err := client.slicesMax()
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

func TestStatusToNodeSlicesForIndexFailure(t *testing.T) {
	server := getMockServer(200, []byte(`[]`), -1)
	defer server.Close()
	client, _ := NewClient(server.URL)
	// no slice
	status := Status{
		indexMaxSlice: map[string]uint64{},
	}
	_, err := client.statusToNodeSlicesForIndex(status, "foo")
	if err == nil {
		t.Fatal("should have failed")
	}

	// no fragment nodes
	status = Status{
		indexMaxSlice: map[string]uint64{
			"foo": 0,
		},
	}
	_, err = client.statusToNodeSlicesForIndex(status, "foo")
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
	client, err := NewClient(uri, TLSConfig(&tls.Config{InsecureSkipVerify: true}))
	if err != nil {
		panic(err)
	}
	f := func() {
		client.Query(testField.Bitmap(1))
	}
	for i := 0; i < 10; i++ {
		go f()
	}
}

func TestImportFieldWithoutImportFunFails(t *testing.T) {
	client := DefaultClient()
	err := client.ImportField(nil, nil, importRecordsFunction(nil))
	if err == nil {
		t.Fatalf("Should have failed")
	}
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

type BrokenReader struct{}

func (r BrokenReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("broken reader")
}
