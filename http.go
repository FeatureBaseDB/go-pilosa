package pilosa

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Client struct {
	pilosaURL string
}

func NewClient(pilosaURL string) *Client {
	return &Client{
		pilosaURL: pilosaURL,
	}
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
		return false, fmt.Errorf("Unexpected response from setbit: %v", resp)
	}
	return resp.Results[0], nil
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
