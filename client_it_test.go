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
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pilosa/go-pilosa/internal"
)

var index *Index
var testFrame *Frame

func TestMain(m *testing.M) {
	var err error
	index, err = NewIndex("go-testindex", nil)
	if err != nil {
		panic(err)
	}
	testFrame, err = index.Frame("test-frame", nil)
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
	err = client.EnsureFrame(testFrame)
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
	client.CreateIndex(index)
	client.CreateFrame(testFrame)
}

func TestCreateDefaultClient(t *testing.T) {
	client := DefaultClient()
	if client == nil {
		t.Fatal()
	}
}

func TestClientReturnsResponse(t *testing.T) {
	client := getClient()
	response, err := client.Query(testFrame.Bitmap(1), nil)
	if err != nil {
		t.Fatalf("Error querying: %s", err)
	}
	if response == nil {
		t.Fatalf("Response should not be nil")
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
	_, err := client.Query(testFrame.SetBit(1, 100), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(index.SetColumnAttrs(100, targetAttrs), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.Column() != nil {
		t.Fatalf("No columns should be returned if it wasn't explicitly requested")
	}
	response, err = client.Query(testFrame.Bitmap(1), &QueryOptions{Columns: true})
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
	_, err := client.Query(testFrame.SetBit(1, 100), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(testFrame.SetRowAttrs(1, targetAttrs), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(testFrame.Bitmap(1), &QueryOptions{Columns: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(targetAttrs, response.Result().Bitmap.Attributes) {
		t.Fatalf("Bitmap attributes should be set")
	}
}

func TestOrmCount(t *testing.T) {
	client := getClient()
	countFrame, err := index.Frame("count-test", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureFrame(countFrame)
	if err != nil {
		t.Fatal(err)
	}
	qry := index.BatchQuery(
		countFrame.SetBit(10, 20),
		countFrame.SetBit(10, 21),
		countFrame.SetBit(15, 25),
	)
	client.Query(qry, nil)
	response, err := client.Query(index.Count(countFrame.Bitmap(10)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.Result().Count != 2 {
		t.Fatalf("Count should be 2")
	}
}

func TestTopNReturns(t *testing.T) {
	client := getClient()
	frame, err := index.Frame("topn_test", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	qry := index.BatchQuery(
		frame.SetBit(10, 5),
		frame.SetBit(10, 10),
		frame.SetBit(10, 15),
		frame.SetBit(20, 5),
		frame.SetBit(30, 5),
	)
	client.Query(qry, nil)
	// XXX: The following is required to make this test pass. See: https://github.com/pilosa/pilosa/issues/625
	time.Sleep(10 * time.Second)
	response, err := client.Query(frame.TopN(2), nil)
	if err != nil {
		t.Fatal(err)
	}
	items := response.Result().CountItems
	if len(items) != 2 {
		t.Fatalf("There should be 2 count items")
	}
	item := items[0]
	if item.ID != 10 {
		t.Fatalf("Item[0] ID should be 10")
	}
	if item.Count != 3 {
		t.Fatalf("Item[0] Count should be 3")
	}
}

func TestCreateDeleteIndexFrame(t *testing.T) {
	client := getClient()
	index1, err := NewIndex("to-be-deleted", nil)
	if err != nil {
		panic(err)
	}
	frame1, err := index1.Frame("foo", nil)
	err = client.CreateIndex(index1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateFrame(frame1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.DeleteFrame(frame1)
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

func TestCreateIndexWithTimeQuantum(t *testing.T) {
	client := getClient()
	options := &IndexOptions{TimeQuantum: TimeQuantumYear}
	index, err := NewIndex("index-with-timequantum", options)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateIndex(index)
	defer client.DeleteIndex(index)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnsureFrameExists(t *testing.T) {
	client := getClient()
	err := client.EnsureFrame(testFrame)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateFrameWithTimeQuantum(t *testing.T) {
	client := getClient()
	options := &FrameOptions{TimeQuantum: TimeQuantumYear}
	frame, err := index.Frame("frame-with-timequantum", options)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateFrame(frame)
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

func TestErrorCreatingFrame(t *testing.T) {
	client := getClient()
	err := client.CreateFrame(testFrame)
	if err == nil {
		t.Fatal()
	}
}

func TestIndexAlreadyExists(t *testing.T) {
	client := getClient()
	err := client.CreateIndex(index)
	if err != ErrorIndexExists {
		t.Fatal(err)
	}
}

func TestQueryWithEmptyClusterFails(t *testing.T) {
	client := NewClientWithCluster(DefaultCluster(), nil)
	_, err := client.Query(index.RawQuery("won't run"), nil)
	if err != ErrorEmptyCluster {
		t.Fatal(err)
	}
}

func TestQueryInverseBitmap(t *testing.T) {
	client := getClient()
	options := &FrameOptions{
		RowLabel:       "row_label",
		InverseEnabled: true,
	}
	f1, err := index.Frame("f1-inversable", options)
	if err != nil {
		t.Fatal(err)
	}
	err = client.EnsureFrame(f1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(
		index.BatchQuery(
			f1.SetBit(1000, 5000),
			f1.SetBit(1000, 6000),
			f1.SetBit(3000, 5000)), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(
		index.BatchQuery(
			f1.Bitmap(1000),
			f1.InverseBitmap(5000)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results()) != 2 {
		t.Fatalf("Response should contain 2 results")
	}
	bits1 := response.Results()[0].Bitmap.Bits
	targetBits1 := []uint64{5000, 6000}
	if !reflect.DeepEqual(targetBits1, bits1) {
		t.Fatalf("bits should be: %v, but it is: %v", targetBits1, bits1)
	}
	bits2 := response.Results()[1].Bitmap.Bits
	targetBits2 := []uint64{1000, 3000}
	if !reflect.DeepEqual(targetBits2, bits2) {
		t.Fatalf("bits should be: %v, but it is: %v", targetBits2, bits2)
	}
}

func TestQueryFailsIfAddressNotResolved(t *testing.T) {
	uri, _ := NewURIFromAddress("nonexisting.domain.pilosa.com:3456")
	client := NewClientWithURI(uri)
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
	_, _, err := client.httpRequest("INVALID METHOD", "/foo", nil, 0)
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
	client := NewClientWithURI(uri)
	response, err := client.Query(testFrame.Bitmap(1), nil)
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
	client := NewClientWithURI(uri)
	response, err := client.Query(testFrame.Bitmap(1), nil)
	if err == nil {
		t.Fatalf("Got response: %v", response)
	}
}

func TestInvalidResponse(t *testing.T) {
	server := getMockServer(200, []byte("unmarshal this!"), -1)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client := NewClientWithURI(uri)
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
	// go-testindex should be in the schema
	for _, index := range schema.Indexes {
		if index.Name == "go-testindex" {
			// test-frame should be in the schema
			for _, frame := range index.Frames {
				if frame.Name == "test-frame" {
					// OK!
					return
				}
			}
		}
	}
	t.Fatal("go-testindex or test-frame was not found")
}

func TestErrorRetrievingSchema(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client := NewClientWithURI(uri)
	_, err = client.Schema()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestInvalidSchema(t *testing.T) {
	server := getMockServer(200, []byte("unserialize this"), -1)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client := NewClientWithURI(uri)
	_, err = client.Schema()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestResponseWithInvalidType(t *testing.T) {
	qr := &internal.QueryResponse{
		Err: "",
		ColumnAttrSets: []*internal.ColumnAttrSet{
			{
				ID: 0,
				Attrs: []*internal.Attr{
					{
						Type:        9999,
						StringValue: "NOVAL",
					},
				},
			},
		},
		Results: []*internal.QueryResult{},
	}
	data, err := proto.Marshal(qr)
	if err != nil {
		t.Fatal(err)
	}
	server := getMockServer(200, data, -1)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClientWithURI(uri)
	_, err = client.Query(testFrame.Bitmap(1), nil)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func getClient() *Client {
	uri, err := NewURIFromAddress(":10101")
	if err != nil {
		panic(err)
	}
	return NewClientWithURI(uri)
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
