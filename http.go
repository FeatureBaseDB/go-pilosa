package pilosa

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

const MAX_QUERIES = 1000

// Client is not safe for concurrent usage.
type Client struct {
	pilosaURL string
	queries   []string
}

func NewClient(pilosaURL string) *Client {
	return &Client{
		pilosaURL: pilosaURL,
		queries:   make([]string, 0),
	}
}

func (c *Client) AddQuery(query string) {
	c.queries = append(c.queries, query)
}

type Results struct {
	Results []interface{}
}

func (c *Client) ExecuteQueries(db int) (Results, error) {
	if len(c.queries) == 0 {
		return Results{}, nil
	}
	r := Results{}
	err := c.pilosaPost(bytes.NewBufferString(strings.Join(c.queries, "")), db, &r)
	return r, err
}

func (c *Client) ClearQueries() {
	c.queries = c.queries[:0]
}

type SetBitResponse struct {
	Results []bool
}

func (c *Client) SetBit(db int, bitmapID int, frame string, profileID int) (bool, error) {
	query := bytes.NewBufferString(fmt.Sprintf("SetBit(%d, '%s', %d)", bitmapID, frame, profileID))
	resp := SetBitResponse{}
	err := c.pilosaPost(query, db, &resp)
	if err != nil {
		return false, err
	}
	if len(resp.Results) != 1 {
		return false, fmt.Errorf("Unexpected response from SetBit: %v", resp)
	}
	return resp.Results[0], nil
}

type ClearBitResponse struct {
	Results []bool
}

func (c *Client) ClearBit(db int, bitmapID int, frame string, profileID int) (bool, error) {
	query := bytes.NewBufferString(fmt.Sprintf("ClearBit(%d, '%s', %d)", bitmapID, frame, profileID))
	resp := ClearBitResponse{}
	err := c.pilosaPost(query, db, &resp)
	if err != nil {
		return false, err
	}
	if len(resp.Results) != 1 {
		return false, fmt.Errorf("Unexpected response from ClearBit: %v", resp)
	}
	return resp.Results[0], nil
}

type CountBitResponse struct {
	Results []int64
}

func (c *Client) CountBit(db int, bitmapID int, frame string) (int64, error) {
	query := bytes.NewBufferString(fmt.Sprintf("Count(Bitmap(%d, '%s'))", bitmapID, frame))
	resp := CountBitResponse{}
	err := c.pilosaPost(query, db, &resp)
	if err != nil {
		return 0, err
	}
	if len(resp.Results) != 1 {
		return 0, fmt.Errorf("Unexpected response from CountBit: %v", resp)
	}
	return resp.Results[0], nil
}

func (c *Client) pilosaPostRaw(query io.Reader, db int) (string, error) {
	req, err := http.Post(fmt.Sprintf("%s/query?db=%d?", c.pilosaURL, db), "application/pql", query)
	if err != nil {
		return "", err
	}

	buf, err := ioutil.ReadAll(req.Body)
	return string(buf), err
}

func (c *Client) pilosaPost(query io.Reader, db int, v interface{}) error {
	req, err := http.Post(fmt.Sprintf("%s/query?db=%d?", c.pilosaURL, db), "application/pql", query)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(req.Body)

	err = dec.Decode(v)
	return err

}
