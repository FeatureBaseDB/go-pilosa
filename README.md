# Go Client for Pilosa

<a href="https://github.com/pilosa"><img src="https://img.shields.io/badge/pilosa-v0.7.0-blue.svg"></a>
<a href="https://godoc.org/github.com/pilosa/go-pilosa"><img src="https://godoc.org/github.com/pilosa/go-pilosa?status.svg" alt="GoDoc"></a>
<a href="https://travis-ci.com/pilosa/go-pilosa"><img src="https://api.travis-ci.com/pilosa/go-pilosa.svg?token=vqssvEWV3KAhu8oVFx9s&branch=master"></a>
<a href="https://goreportcard.com/report/github.com/pilosa/go-pilosa"><img src="https://goreportcard.com/badge/github.com/pilosa/go-pilosa?updated=1"></a>
<a href="https://coveralls.io/github/pilosa/go-pilosa"><img src="https://coveralls.io/repos/github/pilosa/go-pilosa/badge.svg?updated=1"></a>

<img src="https://www.pilosa.com/img/speed_sloth.svg" style="float: right" align="right" height="301">

Go client for Pilosa high performance distributed bitmap index.

## Change Log

* **v0.8.0** (2017-11-16)
    * IPv6 support.
    * **Deprecation** `Error*` constants. Use `Err*` constants instead.
    * **Deprecation** `NewClientWithURI`, `NewClientFromAddresses` and `NewClientWithCluster` functions. Use `NewClient` function which can be used with the same parameters.
    * **Deprecation** Passing a `*ClientOptions` struct to `NewClient` function. Pass 0 or more `ClientOption` structs to `NewClient` instead.  
    * **Deprecation** Passing a `*QueryOptions` struct to `client.Query` function. Pass 0 or more `QueryOption` structs instead. 
    * **Deprecation** Index options.
    * **Deprecation** Passing a `*FrameOptions` struct to `index.Frame` function. Pass 0 or more `FrameOption` structs instead.

* **v0.7.0** (2017-10-04):
    * Dropped support for Go 1.7.
    * Added support for creating range encoded frames.
    * Added `Xor` call.
    * Added support for excluding bits or attributes from bitmap calls. In order to exclude bits, pass `ExcludeBits: true` in your `QueryOptions`. In order to exclude attributes, pass `ExcludeAttrs: true`.
    * Added range field operations.
    * Customizable CSV timestamp format.
    * `HTTPS connections are supported.
    * **Deprecation** Row and column labels are deprecated, and will be removed in a future release of this library. Do not use `ColumnLabel` option for `IndexOptions` and `RowLabel` for `FrameOption` for new code. See: https://github.com/pilosa/pilosa/issues/752 for more info.

* **v0.5.0** (2017-08-03):
    * Supports imports and exports.
    * Introduced schemas. No need to re-define already existing indexes and frames.
    * `NewClientFromAddresses` convenience function added. Create a client for a
      cluster directly from a slice of strings.
    * Failover for connection errors.
    * *make* commands are supported on Windows.
    * **Deprecation** `NewIndex`. Use `schema.Index` instead.
    * **Deprecation** `CreateIndex`, `CreateFrame`, `EnsureIndex`, `EnsureFrame`. Use schemas and `client.SyncSchema` instead.

* **v0.4.0** (2017-06-09):
    * Supports Pilosa Server v0.4.0.
    * Updated the accepted values for index, frame names and labels to match with the Pilosa server.
    * `Union` query now accepts zero or more variadic arguments. `Intersect` and `Difference` queries now accept one or more variadic arguments.
    * Added `inverse TopN` and `inverse Range` calls.
    * Inverse enabled status of frames is not checked on the client side.

* **v0.3.1** (2017-05-01):
    * Initial version.
    * Supports Pilosa Server v0.3.1.

## Requirements

* Go 1.8 and higher

## Install

Download the library in your `GOPATH` using:
```
go get github.com/pilosa/go-pilosa
```

After that, you can import that in your code using:

```go
import "github.com/pilosa/go-pilosa"
```

## Usage

### Quick overview

Assuming [Pilosa](https://github.com/pilosa/pilosa) server is running at `localhost:10101` (the default):

```go
var err error

// Create the default client
client := pilosa.DefaultClient()

// Retrieve the schema
schema, err := client.Schema()

// Create an Index object
myindex, err := schema.Index("myindex")

// Create a Frame object
myframe, err := myindex.Frame("myframe")

// make sure the index and frame exists on the server
err = client.SyncSchema(schema)

// Send a SetBit query. PilosaException is thrown if execution of the query fails.
response, err := client.Query(myframe.SetBit(5, 42))

// Send a Bitmap query. PilosaException is thrown if execution of the query fails.
response, err = client.Query(myframe.Bitmap(5))

// Get the result
result := response.Result()
// Act on the result
if result != nil {
    bits := result.Bitmap.Bits
    fmt.Println("Got bits: %v", bits)
}

// You can batch queries to improve throughput
response, err = client.Query(myindex.BatchQuery(
    myframe.Bitmap(5),
    myframe.Bitmap(10)))
if err != nil {
    fmt.Println(err)
}

for _, result := range response.Results() {
    // Act on the result
    fmt.Println(result)
}

```

### Data Model and Queries

#### Indexes and Frames

*Index* and *frame*s are the main data models of Pilosa. You can check the [Pilosa documentation](https://www.pilosa.com/docs) for more detail about the data model.

`schema.Index` function is used to create an index object. Note that this does not create an index on the server; the index object simply defines the schema.

```go
schema := NewSchema()
repository, err := schema.Index("repository")
```

Frame definitions are created with a call to `Frame` function of an index:

```go
stargazer, err := repository.Frame("stargazer")
```

You can pass options to frames:

```go
stargazer, err := repository.Frame("stargazer", pilosa.InverseEnabled(true), pilosa.TimeQuantumYearMonthDay);
```

#### Queries

Once you have indexes and frame structs created, you can create queries for them. Some of the queries work on the columns; corresponding methods are attached to the index. Other queries work on rows with related methods attached to frames.

For instance, `Bitmap` queries work on rows; use a frame object to create those queries:

```go
bitmapQuery := stargazer.Bitmap(1, 100)  // corresponds to PQL: Bitmap(frame='stargazer', row=1)
```

`Union` queries work on columns; use the index object to create them:

```go
query := repository.Union(bitmapQuery1, bitmapQuery2)
```

In order to increase throughput, you may want to batch queries sent to the Pilosa server. The `index.BatchQuery` function is used for that purpose:

```go
query := repository.BatchQuery(
    stargazer.Bitmap(1, 100),
    repository.Union(stargazer.Bitmap(100, 200), stargazer.Bitmap(5, 100)))
```

The recommended way of creating query structs is, using dedicated methods attached to index and frame objects. But sometimes it would be desirable to send raw queries to Pilosa. You can use `index.RawQuery` method for that. Note that query string is not validated before sending to the server:

```go
query := repository.RawQuery("Bitmap(frame='stargazer', row=5)")
```

This client supports [Range encoded fields](https://www.pilosa.com/docs/latest/query-language/#range-bsi). Read [Range Encoded Bitmaps](https://www.pilosa.com/blog/range-encoded-bitmaps/) blog post for more information about the BSI implementation of range encoding in Pilosa.

In order to use range encoded fields, a frame should be created with one or more integer fields. Each field should have their minimums and maximums set. Here's how you would do that using this library:
```go
index, _ := schema.Index("animals")
frame, _ := index.Frame("traits", pilosa.IntField("captivity", 0, 956))
client.SyncSchema(schema)
``` 

If the frame with the necessary field already exists on the server, you don't need to create the field instance, `client.SyncSchema(schema)` would load that to `schema`. You can then add some data:
```go
// Add the captivity values to the field.
captivity := frame.Field("captivity")
data := []int{3, 392, 47, 956, 219, 14, 47, 504, 21, 0, 123, 318}
query := index.BatchQuery()
for i, x := range data {
	column := uint64(i + 1)
	query.Add(captivity.SetIntValue(column, x))
}
client.Query(query)
```

Let's write a range query:
```go
// Query for all animals with more than 100 specimens
response, _ := client.Query(captivity.GT(100))
fmt.Println(response.Result().Bitmap.Bits)

// Query for the total number of animals in captivity
response, _ = client.Query(captivity.Sum(nil))
fmt.Println(response.Result().Sum)
```

It's possible to pass a bitmap query to `Sum`, so only columns where a row is set are filtered in:
```go
// Let's run a few setbit queries first
client.Query(index.BatchQuery(
    frame.SetBit(42, 1),
    frame.SetBit(42, 6)))
// Query for the total number of animals in captivity where row 42 is set
response, _ = client.Query(captivity.Sum(frame.Bitmap(42)))
fmt.Println(response.Result().Sum)
``` 

See the *Field* functions further below for the list of functions that can be used with a `RangeField`.

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Index:

* `Union(bitmaps *PQLBitmapQuery...) *PQLBitmapQuery`
* `Intersect(bitmaps *PQLBitmapQuery...) *PQLBitmapQuery`
* `Difference(bitmaps *PQLBitmapQuery...) *PQLBitmapQuery`
* `Xor(bitmaps ...*PQLBitmapQuery) *PQLBitmapQuery`
* `Count(bitmap *PQLBitmapQuery) *PQLBaseQuery`
* `SetColumnAttrs(columnID uint64, attrs map[string]interface{}) *PQLBaseQuery`

Frame:

* `Bitmap(rowID uint64) *PQLBitmapQuery`
* `InverseBitmap(columnID uint64) *PQLBitmapQuery`
* `SetBit(rowID uint64, columnID uint64) *PQLBaseQuery`
* `SetBitTimestamp(rowID uint64, columnID uint64, timestamp time.Time) *PQLBaseQuery`
* `ClearBit(rowID uint64, columnID uint64) *PQLBaseQuery`
* `TopN(n uint64) *PQLBitmapQuery`
* `BitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery`
* `FilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery`
* `InverseTopN(n uint64) *PQLBitmapQuery`
* `InverseBitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery`
* `InverseFilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery`
* `Range(rowID uint64, start time.Time, end time.Time) *PQLBitmapQuery`
* `InverseRange(columnID uint64, start time.Time, end time.Time) *PQLBitmapQuery`
* `SetRowAttrs(rowID uint64, attrs map[string]interface{}) *PQLBaseQuery`
* (**deprecated**) `Sum(bitmap *PQLBitmapQuery, field string) *PQLBaseQuery`
* (**deprecated**) `SetIntFieldValue(columnID uint64, field string, value int) *PQLBaseQuery`

Field:

* `LT(n int) *PQLBitmapQuery`
* `LTE(n int) *PQLBitmapQuery`
* `GT(n int) *PQLBitmapQuery`
* `GTE(n int) *PQLBitmapQuery`
* `Between(a int, b int) *PQLBitmapQuery`
* `Sum(bitmap *PQLBitmapQuery) *PQLBaseQuery`
* `SetIntValue(columnID uint64, value int) *PQLBaseQuery`

### Pilosa URI

A Pilosa URI has the `${SCHEME}://${HOST}:${PORT}` format:
* **Scheme**: Protocol of the URI. Default: `http`.
* **Host**: Hostname or ipv4/ipv6 IP address. Default: localhost.
* **Port**: Port number. Default: `10101`.

All parts of the URI are optional, but at least one of them must be specified. The following are equivalent:

* `http://localhost:10101`
* `http://localhost`
* `http://:10101`
* `localhost:10101`
* `localhost`
* `:10101`

A Pilosa URI is represented by the `pilosa.URI` struct. Below are a few ways to create `URI` objects:

```go
// create the default URI: http://localhost:10101
uri1 := pilosa.DefaultURI()

// create a URI from string address
uri2, err := pilosa.NewURIFromAddress("index1.pilosa.com:20202");

// create a URI with the given host and port
uri3, err := pilosa.NewURIFromHostPort("index1.pilosa.com", 20202);
``` 

### Pilosa Client

In order to interact with a Pilosa server, an instance of `pilosa.Client` should be created. The client is thread-safe and uses a pool of connections to the server, so we recommend creating a single instance of the client and sharing it when necessary.

If the Pilosa server is running at the default address (`http://localhost:10101`) you can create the client with default options using:

```go
client := pilosa.DefaultClient()
```

To use a custom server address, use the `NewClient` function:

```go
uri, err := pilosa.NewURIFromAddress("http://index1.pilosa.com:15000")
if err != nil {
    // Act on the error
}
client, err := pilosa.NewClient(uri)
```

Equivalently:
```go
client, err := pilosa.NewClient("http://index1.pilosa.com:15000")
```

If you are running a cluster of Pilosa servers, you can create a `Cluster` struct that keeps addresses of those servers:

```go
uri1, err := pilosa.NewURIFromAddress(":10101")
uri2, err := pilosa.NewURIFromAddress(":10110")
uri3, err := pilosa.NewURIFromAddress(":10111")
cluster := pilosa.NewClusterWithHost(uri1, uri2, uri3)

// Create a client with the cluster
client, err := pilosa.NewClient(cluster)
```

That is equivalent to:
```go
client := pilosa.NewClient([]string{":10101", ":10110", ":10111"})

```

It is possible to customize the behaviour of the underlying HTTP client by passing `ClientOption` structs to the `NewClient` function:

```go
client := pilosa.NewClient(cluster,
	pilosa.ConnectTimeout(1000),  // if can't connect in  a second, close the connection 
    pilosa.SocketTimeout(10000),  // if no response received in 10 seconds, close the connection
    pilosa.PoolSizePerRoute(3),  // number of connections in the pool per host
    pilosa.TotalPoolSize(10))   // number of total connections in the pool
```

Once you create a client, you can create indexes, frames or start sending queries.

Here is how you would create a index and frame:

```go
// materialize repository index definition and stargazer frame definition initialized before
err := client.SyncSchema(schema)
```

You can send queries to a Pilosa server using the `Query` function of the `Client` struct:

```go
response, err := client.Query(frame.Bitmap(5));
```

`Query` accepts zero or more options:

```go
response := client.Query(frame.Bitmap(5), pilosa.ColumnAttrs(true), pilosa.ExcludeBits(true))
```

### Server Response

When a query is sent to a Pilosa server, the server either fulfills the query or sends an error message. In the case of an error, a `pilosa.Error` struct is returned, otherwise a `QueryResponse` struct is returned.

A `QueryResponse` struct may contain zero or more results of `QueryResult` type. You can access all results using the `Results` function of `QueryResponse` (which returns a list of `QueryResult` objects), or you can use the `Result` method (which returns either the first result or `nil` if there are no results):

```go
response, err := client.Query(frame.Bitmap(5))
if err != nil {
    // Act on the error
}

// check that there's a result and act on it
result := response.Result()
if result != nil {
    // Act on the result
}

// iterate over all results
for result := range response.Results() {
    // Act on the result
}
```

Similarly, a `QueryResponse` struct may include a number of columns (column objects) if `Columns` query option was set to `true`:

```go
var column *pilosa.ColumnItem

// check that there's a column and act on it
column = response.Column()
if (column != null) {
    // Act on the column
}

// iterate over all columns
for column = range response.Columns() {
    // Act on the column
}
```

`QueryResult` objects contain:

* `Bitmap` field to retrieve a bitmap result,
* `CountItems` fied to retrieve column count per row ID entries returned from `TopN` queries,
* `Count` field to retrieve the number of rows per the given row ID returned from `Count` queries.

```go
bitmap := result.Bitmap
bits := bitmap.Bits
attributes := bitmap.Attributes

countItems := result.CountItems

count := result.Count
```

## Importing and Exporting Data

### Importing Data

If you have large amounts of data, it is more efficient to import it into Pilosa instead of using multiple SetBit queries. This library supports importing bits into an existing frame.

Before starting the import, create an instance of a struct which implements `BitIterator` and pass it to the `client.ImportFrame` function. This library ships with the `CSVBitIterator` struct which supports importing bits in the CSV (comma separated values) format:
```
ROW_ID,COLUMN_ID
```

Optionally, a timestamp can be added. Note that Pilosa is not time zone aware:
```
ROW_ID,COLUMN_ID,TIMESTAMP
```

Note that each line corresponds to a single bit and ends with a new line (`\n` or `\r\n`).

Here's some sample code:
```go
text := `10,7
    10,5
    2,3
    7,1`
iterator := NewCSVBitIterator(strings.NewReader(text))
```

After creating the iterator you can pass it to `client.ImportFrame` together with the frame and batch size. The following sample sends batches with size 10000 bits:
```go
err = client.ImportFrame(frame, iterator, 10000)
if err != nil {
    panic(err)
}
```

You can define a custom `BitIterator` by including a function with the signature `NextBit() (Bit, error)` in your struct.
```go
type StaticBitIterator struct {
    NextBit() (Bit, error)
    bits []Bit
    index int
}

func NewStaticBitIterator() *StaticBitIterator {
    return &StaticBitIterator{
        bits: []Bit{
            Bit{RowID: 1, ColumnID: 1, Timestamp: 683793200},
            Bit{RowID: 5, ColumnID: 20, Timestamp: 683793300},
            Bit{RowID: 3, ColumnID: 41, Timestamp: 683793385},
        }
    }
}

func (it *StatiBitIterator) NextBit(Bit, error) {
    if it.index < len(it.bits) {
        return Bit{}, io.EOF
    }
    return it.bits[it.index++], nil
}
```

### Exporting Data

You can export a view of a frame from Pilosa using `client.ExportFrame` function which returns a `BitIterator`. Use the `NextBit` function of this iterator to receive all bits for the specified frame. When there are no more bits, `io.EOF` is returned.

The `PilosaClient` struct has the `Views` function which returns all of the views for a particular frame. You can use this function to retrieve view names:
```go
views, err := client.Views(frame)
```

Here's sample code which retrieves bits of the `standard` view:

```go
bits := []Bit{}
iterator, err := client.ExportFrame(frame, "standard")
if err != nil {
    t.Fatal(err)
}
for {
    bit, err := iterator.NextBit()
    if err == io.EOF {
        break
    }
    if err != nil {
        t.Fatal(err)
    }
    bits = append(bits, bit)
}
```

## Contribution

Please check our [Contributor's Guidelines](https://github.com/pilosa/pilosa/CONTRIBUTING.md).

1. Fork this repo and add it as upstream: `git remote add upstream git@github.com:pilosa/go-pilosa.git`.
2. Make sure all tests pass (use `make test-all`) and be sure that the tests cover all statements in your code (we aim for 100% test coverage).
3. Commit your code to a feature branch and send a pull request to the `master` branch of our repo.

### Running tests

You can run unit tests with:
```
make test
```

And both unit and integration tests with:
```
make test-all
```

Check the test coverage:
```
make cover
```

### Generating protobuf classes

Protobuf classes are already checked in to source control, so this step is only needed when the upstream `public.proto` changes.

Before running the following step, make sure you have the [Protobuf compiler](https://github.com/google/protobuf) and [Go protobuf support](https://github.com/golang/protobuf)  is installed:

```
make generate
```

## License

```
Copyright 2017 Pilosa Corp.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its
contributors may be used to endorse or promote products derived
from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
DAMAGE.
```
