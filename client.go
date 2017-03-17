package pilosa

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pilosa/go-client-pilosa/internal"
)

// Pilosa HTTP Client

// Client queries the Pilosa server
type Client struct {
	cluster *Cluster
}

// NewClient creates the default client
func NewClient() *Client {
	return &Client{
		cluster: NewClusterWithAddress(NewURI()),
	}
}

// NewClientWithAddress creates a client with the given address
func NewClientWithAddress(address *URI) *Client {
	return NewClientWithCluster(NewClusterWithAddress(address))
}

// NewClientWithCluster creates a client with the given cluster
func NewClientWithCluster(cluster *Cluster) *Client {
	return &Client{
		cluster: cluster,
	}
}

// Query sends a query to the Pilosa server with default options
func (c *Client) Query(databaseName string, query string) (*QueryResponse, error) {
	return c.QueryWithOptions(&QueryOptions{}, databaseName, query)
}

// QueryWithOptions sends a query to the Pilosa server with the given options
func (c *Client) QueryWithOptions(options *QueryOptions, databaseName string, query string) (*QueryResponse, error) {
	err := validateDatabaseName(databaseName)
	if err != nil {
		return nil, err
	}
	data := makeRequestData(databaseName, query, options)
	return c.httpRequest("POST", "/query", data, true)
}

// CreateDatabase creates a database with default options
func (c *Client) CreateDatabase(databaseName string) error {
	return c.createOrDeleteDatabase("POST", databaseName)
}

// CreateFrame creates a frame with default options
func (c *Client) CreateFrame(databaseName string, frameName string) error {
	return c.createOrDeleteFrame("POST", databaseName, frameName)
}

// EnsureDatabaseExists creates a database with default options if it doesn't already exist
func (c *Client) EnsureDatabaseExists(databaseName string) error {
	err := c.CreateDatabase(databaseName)
	if err == nil || err == ErrorDatabaseExists {
		return nil
	}
	return err
}

// EnsureFrameExists creates a frame with default options if it doesn't already exists
func (c *Client) EnsureFrameExists(databaseName string, frameName string) error {
	err := c.CreateFrame(databaseName, frameName)
	if err == nil || err == ErrorFrameExists {
		return nil
	}
	return err
}

// DeleteDatabase deletes a database
func (c *Client) DeleteDatabase(databaseName string) error {
	return c.createOrDeleteDatabase("DELETE", databaseName)
}

// DeleteFrame deletes a frame with default options
func (c *Client) DeleteFrame(databaseName string, frameName string) error {
	return c.createOrDeleteFrame("DELETE", databaseName, frameName)
}

func (c *Client) createOrDeleteDatabase(method string, databaseName string) error {
	err := validateDatabaseName(databaseName)
	if err != nil {
		return err
	}
	data := []byte(fmt.Sprintf(`{"db": "%s"}`, databaseName))
	_, err = c.httpRequest(method, "/db", data, false)
	return err
}

func (c *Client) createOrDeleteFrame(method string, databaseName string, frameName string) error {
	err := validateDatabaseName(databaseName)
	if err != nil {
		return err
	}
	err = validateFrameName(frameName)
	if err != nil {
		return err
	}
	data := []byte(fmt.Sprintf(`{"db": "%s", "frame": "%s"}`, databaseName, frameName))
	_, err = c.httpRequest(method, "/frame", data, false)
	return err
}

func (c *Client) httpRequest(method string, path string, data []byte, needsResponse bool) (*QueryResponse, error) {
	addr := c.cluster.GetAddress()
	if addr == nil {
		return nil, ErrorEmptyCluster
	}
	client := &http.Client{}
	request, err := http.NewRequest(method, addr.GetNormalizedAddress()+path, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	// both Content-Type and Accept headers must be set for protobuf content
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Accept", "application/x-protobuf")
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// TODO: Optimize buffer creation
		buf, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}
		msg := string(buf)
		switch msg {
		case "database already exists\n":
			return nil, ErrorDatabaseExists
		case "frame already exists\n":
			return nil, ErrorFrameExists
		}
		return nil, NewPilosaError(fmt.Sprintf("Server error (%d) %s: %s", response.StatusCode, response.Status, msg))
	}
	if needsResponse {
		// TODO: Optimize buffer creation
		buf, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}
		iqr := &internal.QueryResponse{}
		err = iqr.Unmarshal(buf)
		if err != nil {
			return nil, err
		}
		return newQueryResponseFromInternal(iqr)
	}
	return nil, nil
}

func makeRequestData(databaseName string, query string, options *QueryOptions) []byte {
	request := &internal.QueryRequest{
		DB:       databaseName,
		Query:    query,
		Profiles: options.GetProfiles,
	}
	r, _ := request.Marshal()
	// request.Marshal never returns an error
	return r
}

// QueryOptions contains options that can be sent with a query
type QueryOptions struct {
	GetProfiles bool
}
