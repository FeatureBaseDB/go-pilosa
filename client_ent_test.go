// +build enterprise

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
	"crypto/tls"
	"os"
	"strings"
	"testing"

	"github.com/pkg/errors"
)

var index *Index
var testFrame *Frame

func TestMain(m *testing.M) {
	var err error
	index, err = NewIndex("go-testindex", nil)
	if err != nil {
		panic(err)
	}
	testFrame, err = index.Frame("test-frame", &FrameOptions{RangeEnabled: true})
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
	err = client.CreateIntField(testFrame, "testfield", 0, 1000)
	if err != nil {
		panic(errors.Wrap(err, "creating int field"))
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

func TestCSVKeyImport(t *testing.T) {
	proxyClient := getProxyClient()
	text := `row1,col10
		row1,col20
		row2,col30
		row4,col80`
	iterator := NewCSVBitIteratorK(strings.NewReader(text))
	frame, err := index.Frame("importkframe", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = proxyClient.EnsureFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	err = proxyClient.ImportFrameK(frame, iterator, 10)
	if err != nil {
		t.Fatal(err)
	}

	target := []string{"col30", "col80", "col10"}
	bq := index.BatchQuery(
		frame.BitmapK("row2"),
		frame.BitmapK("row4"),
		frame.BitmapK("row1"),
	)

	response, err := proxyClient.Query(bq, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(response.Results()) != 3 {
		t.Fatalf("Result count should be 3")
	}
	for i, result := range response.Results() {
		br := result.Bitmap
		if target[i] != br.Keys[0] {
			t.Fatalf("%s != %s", target[i], br.Keys[0])
		}
	}
}

func TestValueCSVKeyImport(t *testing.T) {
	client := getClient()
	proxyClient := getProxyClient()
	text := `col10,7
		col7,1`
	iterator := NewCSVValueIteratorK(strings.NewReader(text))
	frameOptions := &FrameOptions{}
	frameOptions.AddIntField("foo", 0, 100)
	frame, err := index.Frame("importvaluekframe", frameOptions)
	if err != nil {
		t.Fatal(err)
	}
	err = proxyClient.EnsureFrame(frame)
	if err != nil {
		t.Fatal(err)
	}

	err = proxyClient.ImportValueFrameK(frame, "foo", iterator, 10)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Query(frame.Sum(nil, "foo"), nil)
	if err != nil {
		t.Fatal(err)
	}
	targetSum := int64(8)
	if targetSum != response.Result().Sum {
		t.Fatalf("sum %d != %d", targetSum, response.Result().Sum)
	}
	targetCount := uint64(2)
	if targetCount != response.Result().Count {
		t.Fatalf("count %d != %d", targetCount, response.Result().Count)
	}
}

func getClient() *Client {
	uri, err := NewURIFromAddress(getPilosaBindAddress())
	if err != nil {
		panic(err)
	}
	client, err := NewClient(uri, TLSConfig(&tls.Config{InsecureSkipVerify: true}))
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

func getProxyClient() *Client {
	uri, err := NewURIFromAddress(getProxyBindAddress())
	if err != nil {
		panic(err)
	}
	client, err := NewClient(uri, TLSConfig(&tls.Config{InsecureSkipVerify: true}))
	if err != nil {
		panic(err)
	}
	return client
}

func getProxyBindAddress() string {
	return "http://:20202"
}
