// +build integration

package pilosa

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
)

var dbname = "go-testdb"

func TestMain(m *testing.M) {
	Setup()
	r := m.Run()
	TearDown()
	os.Exit(r)
}

func Setup() {
	client := getClient()
	err := client.EnsureDatabaseExists(dbname)
	if err != nil {
		panic(err)
	}
	err = client.EnsureFrameExists(dbname, "test-frame")
	if err != nil {
		panic(err)
	}
}

func TearDown() {
	client := getClient()
	err := client.DeleteDatabase(dbname)
	if err != nil {
		panic(err)
	}
}

func TestCreateDefaultClient(t *testing.T) {
	client := NewClient()
	if client == nil {
		t.Fatal()
	}
}

func TestClientReturnsResponse(t *testing.T) {
	client := getClient()
	response, err := client.Query(dbname, "Bitmap(id=1, frame='test-frame')")
	if err != nil {
		t.Fatalf("Error querying: %s", err)
	}
	if response == nil {
		t.Fatalf("Response should not be nil")
	}
}

func TestQueryWithInvalidDatabaseNameFails(t *testing.T) {
	client := getClient()
	_, err := client.Query("", "Bitmap(id=1, frame='test-frame')")
	if err != ErrorInvalidDatabaseName {
		t.Fail()
	}
}

func TestCreateDeleteDatabaseFrame(t *testing.T) {
	client := getClient()
	const db1 = "to-be-deleted"
	const frame1 = "foo"
	err := client.CreateDatabase(db1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.CreateFrame(db1, frame1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.DeleteFrame(db1, frame1)
	if err != nil {
		t.Fatal(err)
	}
	err = client.DeleteDatabase(db1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestErrorCreatingDatabase(t *testing.T) {
	client := getClient()
	err := client.CreateDatabase(dbname)
	if err == nil {
		t.Fatal()
	}
}

func TestErrorCreatingFrame(t *testing.T) {
	client := getClient()
	err := client.CreateFrame(dbname, "test-frame")
	if err == nil {
		t.Fatal()
	}
}

func TestDatabaseAlreadyExists(t *testing.T) {
	client := getClient()
	err := client.CreateDatabase(dbname)
	if err != ErrorDatabaseExists {
		t.Fail()
	}
}

func TestCreatingDatabaseWithInvalidName(t *testing.T) {
	client := getClient()
	err := client.EnsureDatabaseExists("")
	if err == nil {
		t.Fatal()
	}
}

func TestCreatingFrameWithInvalidName(t *testing.T) {
	client := getClient()
	err := client.EnsureFrameExists("", "foo")
	if err != ErrorInvalidDatabaseName {
		t.Fatal()
	}
	err = client.EnsureFrameExists("foo", "")
	if err != ErrorInvalidFrameName {
		t.Fatal()
	}
}

func TestQueryWithEmptyClusterFails(t *testing.T) {
	client := NewClientWithCluster(NewCluster())
	_, err := client.Query("foo", "won't run")
	if err != ErrorEmptyCluster {
		t.Fatal()
	}
}

func TestQueryFailsIfAddressNotResovled(t *testing.T) {
	uri, _ := NewURIFromAddress("nonexisting.domain.pilosa.com:3456")
	client := NewClientWithAddress(uri)
	_, err := client.Query("foo", "bar")
	if err == nil {
		t.Fatal()
	}
}

func TestQueryFails(t *testing.T) {
	client := getClient()
	_, err := client.Query(dbname, "Invalid query")
	if err == nil {
		t.Fatal()
	}
}

func TestInvalidHttpRequest(t *testing.T) {
	client := getClient()
	_, err := client.httpRequest("INVALID METHOD", "/foo", nil, false)
	if err == nil {
		t.Fatal()
	}
}

func TestErrorResponseNotRead(t *testing.T) {
	server := getMockServer(500, []byte("Unknown error"), 512)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client := NewClientWithAddress(uri)
	response, err := client.Query(dbname, "Bitmap(id=1, frame='test-frame')")
	if err == nil {
		t.Fatalf("Got response: %s", response)
	}
}

func TestResponseNotRead(t *testing.T) {
	server := getMockServer(200, []byte("some content"), 512)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client := NewClientWithAddress(uri)
	response, err := client.Query(dbname, "Bitmap(id=1, frame='test-frame')")
	if err == nil {
		t.Fatalf("Got response: %s", response)
	}
}

func TestInvalidResponse(t *testing.T) {
	server := getMockServer(200, []byte("unmarshal this!"), -1)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client := NewClientWithAddress(uri)
	response, err := client.httpRequest("GET", "/foo", nil, true)
	if err == nil {
		t.Fatalf("Got response: %s", response)
	}
}

func getClient() *Client {
	uri, err := NewURIFromAddress("localhost:15000")
	if err != nil {
		panic(err)
	}
	return NewClientWithAddress(uri)
}

func getMockServer(statusCode int, response []byte, contentLength int) *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if contentLength >= 0 {
			w.Header().Set("Content-Length", strconv.Itoa(contentLength))
		}
		w.WriteHeader(statusCode)
		if response != nil {
			fmt.Fprintln(w, response)
		}
	})
	return httptest.NewServer(handler)
}
