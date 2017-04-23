package pilosa

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pilosa/go-client-pilosa/internal"
)

// Pilosa HTTP Client

// Client queries the Pilosa server
type Client struct {
	cluster *Cluster
	client  *http.Client
}

// DefaultClient creates the default client
func DefaultClient() *Client {
	return NewClientWithURI(DefaultURI())
}

// NewClientWithURI creates a client with the given address
func NewClientWithURI(uri *URI) *Client {
	return NewClientWithCluster(NewClusterWithHost(uri), nil)
}

// NewClientWithCluster creates a client with the given cluster
func NewClientWithCluster(cluster *Cluster, options *ClientOptions) *Client {
	if options == nil {
		options = &ClientOptions{}
	}
	return &Client{
		cluster: cluster,
		client:  newHTTPClient(options.withDefaults()),
	}
}

// Query sends a query to the Pilosa server with the given options
func (c *Client) Query(query PQLQuery, options *QueryOptions) (*QueryResponse, error) {
	if err := query.Error(); err != nil {
		return nil, err
	}
	if options == nil {
		options = &QueryOptions{}
	}
	data := makeRequestData(query.serialize(), options)
	path := fmt.Sprintf("/db/%s/query", query.Database().name)
	_, buf, err := c.httpRequest("POST", path, data, rawResponse)
	if err != nil {
		return nil, err
	}
	iqr := &internal.QueryResponse{}
	err = proto.Unmarshal(buf, iqr)
	if err != nil {
		return nil, err
	}
	queryResponse, err := newQueryResponseFromInternal(iqr)
	if err != nil {
		return nil, err
	}
	if !queryResponse.Success {
		return nil, NewPilosaError(queryResponse.ErrorMessage)
	}
	return queryResponse, nil
}

// CreateDatabase creates a database with default options
func (c *Client) CreateDatabase(database *Database) error {
	data := []byte(database.options.String())
	path := fmt.Sprintf("/db/%s", database.name)
	_, _, err := c.httpRequest("POST", path,
		data, noResponse)
	if err != nil {
		return err
	}
	if database.options.TimeQuantum != TimeQuantumNone {
		err = c.patchDatabaseTimeQuantum(database)
	}
	return err

}

// CreateFrame creates a frame with default options
func (c *Client) CreateFrame(frame *Frame) error {
	data := []byte(frame.options.String())
	path := fmt.Sprintf("/db/%s/frame/%s", frame.database.name, frame.name)
	_, _, err := c.httpRequest("POST", path, data, noResponse)
	if err != nil {
		return err
	}
	if frame.options.TimeQuantum != TimeQuantumNone {
		err = c.patchFrameTimeQuantum(frame)
	}
	return err

}

// EnsureDatabase creates a database with default options if it doesn't already exist
func (c *Client) EnsureDatabase(database *Database) error {
	err := c.CreateDatabase(database)
	if err == ErrorDatabaseExists {
		return nil
	}
	return err
}

// EnsureFrame creates a frame with default options if it doesn't already exists
func (c *Client) EnsureFrame(frame *Frame) error {
	err := c.CreateFrame(frame)
	if err == ErrorFrameExists {
		return nil
	}
	return err
}

// DeleteDatabase deletes a database
func (c *Client) DeleteDatabase(database *Database) error {
	path := fmt.Sprintf("/db/%s", database.name)
	_, _, err := c.httpRequest("DELETE", path, nil, noResponse)
	return err

}

// DeleteFrame deletes a frame with default options
func (c *Client) DeleteFrame(frame *Frame) error {
	path := fmt.Sprintf("/db/%s/frame/%s", frame.database.name, frame.name)
	_, _, err := c.httpRequest("DELETE", path, nil, noResponse)
	return err
}

// Schema returns the databases and frames of the server
func (c *Client) Schema() (*Schema, error) {
	_, buf, err := c.httpRequest("GET", "/schema", nil, errorCheckedResponse)
	if err != nil {
		return nil, err
	}
	var schema *Schema
	err = json.NewDecoder(bytes.NewReader(buf)).Decode(&schema)
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func (c *Client) patchDatabaseTimeQuantum(database *Database) error {
	data := []byte(fmt.Sprintf(`{"time_quantum": "%s"}`, database.options.TimeQuantum))
	path := fmt.Sprintf("/db/%s/time-quantum", database.name)
	_, _, err := c.httpRequest("PATCH", path, data, noResponse)
	return err
}

func (c *Client) patchFrameTimeQuantum(frame *Frame) error {
	data := []byte(fmt.Sprintf(`{"db": "%s", "frame": "%s", "time_quantum": "%s"}`,
		frame.database.name, frame.name, frame.options.TimeQuantum))
	path := fmt.Sprintf("/db/%s/frame/%s/time-quantum", frame.database.name, frame.name)
	_, _, err := c.httpRequest("PATCH", path, data, noResponse)
	return err
}

func (c *Client) httpRequest(method string, path string, data []byte, returnResponse returnClientInfo) (*http.Response, []byte, error) {
	addr := c.cluster.Host()
	if addr == nil {
		return nil, nil, ErrorEmptyCluster
	}
	if data == nil {
		data = []byte{}
	}
	request, err := http.NewRequest(method, addr.Normalize()+path, bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	// both Content-Type and Accept headers must be set for protobuf content
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Accept", "application/x-protobuf")
	response, err := c.client.Do(request)
	if err != nil {
		c.cluster.RemoveHost(addr)
		return nil, nil, err
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
		return nil, nil, NewPilosaError(fmt.Sprintf("Server error (%d) %s: %s", response.StatusCode, response.Status, msg))
	}
	if returnResponse == noResponse {
		return nil, nil, nil
	}
	return response, buf, nil
}

func newHTTPClient(options *ClientOptions) *http.Client {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: options.ConnectTimeout,
		}).Dial,
		MaxIdleConnsPerHost: options.PoolSizePerRoute,
		MaxIdleConns:        options.TotalPoolSize,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   options.SocketTimeout,
	}
}

func makeRequestData(query string, options *QueryOptions) []byte {
	request := &internal.QueryRequest{
		Query:       query,
		ColumnAttrs: options.Columns,
	}
	r, _ := proto.Marshal(request)
	// request.Marshal never returns an error
	return r
}

func matchError(msg string) error {
	switch msg {
	case "database already exists\n":
		return ErrorDatabaseExists
	case "frame already exists\n":
		return ErrorFrameExists
	}
	return nil
}

// ClientOptions control the properties of client connection to the server
type ClientOptions struct {
	SocketTimeout    time.Duration
	ConnectTimeout   time.Duration
	PoolSizePerRoute int
	TotalPoolSize    int
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
		updated.ConnectTimeout = time.Second * 30
	}
	if updated.PoolSizePerRoute <= 0 {
		updated.PoolSizePerRoute = 10
	}
	if updated.TotalPoolSize <= 100 {
		updated.TotalPoolSize = 100
	}
	return
}

// QueryOptions contains options that can be sent with a query
type QueryOptions struct {
	Columns bool
}

// Schema contains the database and frame metadata
type Schema struct {
	DBs []*DBInfo `json:"dbs"`
}

// DBInfo represents schema information for a database.
type DBInfo struct {
	Name   string       `json:"name"`
	Frames []*FrameInfo `json:"frames"`
}

type returnClientInfo uint

const (
	noResponse returnClientInfo = iota
	rawResponse
	errorCheckedResponse
)
