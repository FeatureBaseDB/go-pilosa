// +build !nointegration

package pilosa

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"testing/iotest"
)

func TestNewClientFromAddresses(t *testing.T) {
	cli, err := NewClient([]string{":10101", "node0.pilosa.com:10101", "node2.pilosa.com"})

	if err != nil {
		t.Fatalf("Creating client from addresses: %v", err)
	}
	expectedHosts := []URI{
		{scheme: "http", port: 10101, host: "localhost"},
		{scheme: "http", port: 10101, host: "node0.pilosa.com"},
		{scheme: "http", port: 10101, host: "node2.pilosa.com"},
	}
	actualHosts := cli.cluster.Hosts()
	if !reflect.DeepEqual(actualHosts, expectedHosts) {
		t.Fatalf("Unexpected hosts in client's cluster, got: %v, expected: %v", actualHosts, expectedHosts)
	}

	cli, err = NewClient([]string{"://"})
	if err == nil {
		t.Fatalf("Did not get expected error when creating client: %v", cli.cluster.Hosts())
	}

	cli, err = NewClient([]string{})
	if err != nil {
		t.Fatalf("Got error when creating empty client from addresses: %v", err)
	}

	cli, err = NewClient(nil)
	if err != nil {
		t.Fatalf("Got error when creating empty client from addresses: %v", err)
	}
}

func TestAnyError(t *testing.T) {
	err := anyError(
		&http.Response{StatusCode: 400,
			Body: ioutil.NopCloser(iotest.TimeoutReader(bytes.NewBuffer([]byte("asdf"))))},
		nil)
	if err == nil {
		t.Fatalf("should have gotten an error")
	}
}

func TestImportWithReplay(t *testing.T) {
	buf := &bytes.Buffer{}
	client := getClient(ExperimentalOptClientLogImports(buf))

	// the first iterator for creating the target
	iterator := &ColumnGenerator{numRows: 10, numColumns: 1000}
	target := map[uint64][]uint64{}
	for {
		rec, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if col, ok := rec.(Column); ok {
			target[col.RowID] = append(target[col.RowID], col.ColumnID)
		}
	}
	// the second iterator for the actual import
	iterator = &ColumnGenerator{numRows: 10, numColumns: 1000}
	field := index.Field("importfield-batchsize")
	err := client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	statusChan := make(chan ImportStatusUpdate, 10)
	err = client.ImportField(field, iterator, OptImportStatusChannel(statusChan), OptImportThreadCount(2), OptImportBatchSize(1000))
	if err != nil {
		t.Fatal(err)
	}

	// delete index and recreate
	Reset()
	field = index.Field("importfield-batchsize")
	err = client.EnsureField(field)
	if err != nil {
		t.Fatal(err)
	}
	client.logLock.Lock()
	defer client.logLock.Unlock()

	if client.importLogEncoder == nil {
		t.Fatalf("should have a log encoder")
	}

	err = client.ExperimentalReplayImport(buf)
	if err != nil {
		t.Fatalf("replaying import: %v", err)
	}

	for rowID, colIDs := range target {
		resp, err := client.Query(field.Row(rowID))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(colIDs, resp.Result().Row().Columns) {
			t.Fatalf("%v != %v", colIDs, resp.Result().Row().Columns)
		}
	}
}
