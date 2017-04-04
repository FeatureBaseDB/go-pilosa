// +build integration

package pilosa

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"testing"
)

var db *Database
var testFrame *Frame

func TestMain(m *testing.M) {
	var err error
	db, err = NewDatabase("go-testdb", nil)
	if err != nil {
		panic(err)
	}
	testFrame, err = db.Frame("test-frame", nil)
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
	err := client.EnsureDatabaseExists(db)
	if err != nil {
		panic(err)
	}
	err = client.EnsureFrameExists(testFrame)
	if err != nil {
		panic(err)
	}
}

func TearDown() {
	client := getClient()
	err := client.DeleteDatabase(db)
	if err != nil {
		panic(err)
	}
}

func Reset() {
	client := getClient()
	client.DeleteDatabase(db)
	client.CreateDatabase(db)
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

func TestQueryWithProfiles(t *testing.T) {
	Reset()
	client := getClient()
	targetAttrs := map[string]interface{}{
		"name":       "some string",
		"age":        uint64(95),
		"registered": true,
		"height":     1.83,
	}
	_, err := client.Query(testFrame.SetBit(1, 100), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(db.RawQuery("SetProfileAttrs(id=100, name='some string', age=95, registered=true, height=1.83)"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.Profile() != nil {
		t.Fatalf("No profiles should be returned if it wasn't explicitly requested")
	}
	response, err = client.Query(testFrame.Bitmap(1), &QueryOptions{Profiles: true})
	if err != nil {
		t.Fatal(err)
	}
	profiles := response.Profiles()
	if len(profiles) != 1 {
		t.Fatalf("Profile count should be == 1")
	}
	if profiles[0].ID != 100 {
		t.Fatalf("Profile ID should be == 100")
	}
	if !reflect.DeepEqual(profiles[0].Attributes, targetAttrs) {
		t.Fatalf("Protile attrs does not match")
	}

	if !reflect.DeepEqual(response.Profile(), profiles[0]) {
		t.Fatalf("Profile() should be equivalent to first profile in the response")
	}
}

func TestCreateDeleteDatabaseFrame(t *testing.T) {
	client := getClient()
	db1, err := NewDatabase("to-be-deleted", nil)
	if err != nil {
		panic(err)
	}
	frame1, err := db1.Frame("foo", nil)
	err = client.CreateDatabase(db1)
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
	err = client.DeleteDatabase(db1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEnsureDatabaseExists(t *testing.T) {
	client := getClient()
	err := client.EnsureDatabaseExists(db)
	if err != nil {
		t.Fatal()
	}
}

func TestEnsureFrameExists(t *testing.T) {
	client := getClient()
	err := client.EnsureFrameExists(testFrame)
	if err != nil {
		t.Fatal()
	}
}

func TestErrorCreatingDatabase(t *testing.T) {
	client := getClient()
	err := client.CreateDatabase(db)
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

func TestDatabaseAlreadyExists(t *testing.T) {
	client := getClient()
	err := client.CreateDatabase(db)
	if err != ErrorDatabaseExists {
		t.Fail()
	}
}

func TestQueryWithEmptyClusterFails(t *testing.T) {
	client := NewClientWithCluster(DefaultCluster(), nil)
	_, err := client.Query(db.RawQuery("won't run"), nil)
	if err != ErrorEmptyCluster {
		t.Fatal()
	}
}

func TestQueryFailsIfAddressNotResovled(t *testing.T) {
	uri, _ := NewURIFromAddress("nonexisting.domain.pilosa.com:3456")
	client := NewClientWithAddress(uri)
	_, err := client.Query(db.RawQuery("bar"), nil)
	if err == nil {
		t.Fatal()
	}
}

func TestQueryFails(t *testing.T) {
	client := getClient()
	_, err := client.Query(db.RawQuery("Invalid query"), nil)
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
	response, err := client.Query(testFrame.Bitmap(1), nil)
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
	response, err := client.Query(testFrame.Bitmap(1), nil)
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
	response, err := client.Query(db.RawQuery("don't care"), nil)
	if err == nil {
		t.Fatalf("Got response: %s", response)
	}
}

func TestSchema(t *testing.T) {
	client := getClient()
	schema, err := client.Schema()
	if err != nil {
		t.Fatal(err)
	}
	// go-testdb should be in the schema
	for _, db := range schema.DBs {
		if db.Name == "go-testdb" {
			// test-frame should be in the schema
			for _, frame := range db.Frames {
				if frame.Name == "test-frame" {
					// OK!
					return
				}
			}
		}
	}
	t.Fatal("go-testdb or test-frame was not found")
}

func TestErrorRetrievingSchema(t *testing.T) {
	server := getMockServer(404, []byte("sorry, not found"), -1)
	defer server.Close()
	uri, err := NewURIFromAddress(server.URL)
	if err != nil {
		panic(err)
	}
	client := NewClientWithAddress(uri)
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
	client := NewClientWithAddress(uri)
	_, err = client.Schema()
	if err == nil {
		t.Fatal("should have failed")
	}
}

func getClient() *Client {
	uri, err := NewURIFromAddress(":10101")
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
