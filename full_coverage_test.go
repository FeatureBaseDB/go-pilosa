// +build fullcoverage

package pilosa

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// TestMultipleClientKeyQuery2 coverts else {host = c.coordinatorURI} line in Client.host function

func TestMultipleClientKeyQuery2(t *testing.T) {
	server := getMockStatusServer(200, []byte(`{"state":"NORMAL","nodes":[{"isCoordinator":true, "id":"0f5c2ffc-1244-47d0-a83d-f5a25abba9bc","uri":{"scheme":"http","host":"nonexistent","port":10101}}],"localID":"0f5c2ffc-1244-47d0-a83d-f5a25abba9bc"}`), -1)
	defer server.Close()
	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	schema := NewSchema()
	keysIndex := schema.Index("keys-index", OptIndexKeys(true))

	const goroutineCount = 10
	wg := &sync.WaitGroup{}
	wg.Add(goroutineCount)
	for i := 0; i < goroutineCount; i++ {
		go func(rowID uint64) {
			client.Status()
			client.Query(keysIndex.SetColumnAttrs("col", map[string]interface{}{"a": 1}))
			wg.Done()
		}(uint64(i))
	}
	wg.Wait()
}

func getMockStatusServer(statusCode int, response []byte, contentLength int) *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/status") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
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
