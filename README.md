# Go Client for Pilosa

<a href="https://travis-ci.com/pilosa/go-pilosa"><img src="https://api.travis-ci.com/pilosa/go-client-pilosa.svg?token=vqssvEWV3KAhu8oVFx9s&branch=master"></a>

<img src="https://dc3kpxyuw05cb.cloudfront.net/img/ce.svg" style="float: right" align="right" height="301">

Go client for Pilosa high performance database.

## Changelog

* 2017-05-01: Initial version

## Requirements

* Go 1.7 and higher

## Install

Import the library in your code using:

```go
import pilosa "pilosa/go-pilosa"
```

Then, if you have [Glide](glide), you can run the following in the shell:

```
glide up
```

Otherwise:

```
go get -u
```

## Usage

### Quick overview

Assuming [Pilosa](https://github.com/pilosa/pilosa) server is running at `localhost:10101` (the default):

```go
var err error

// Create the default client
client := pilosa.DefaultClient()

// Create a Database object
mydb, err := pilosa.NewDatabase("mydb", nil)

// Make sure the database exists on the server
err = client.EnsureDatabase(mydb)

// Create a Frame object
myframe, err := mydb.Frame("myframe", nil)

// Make sure the frame exists on the server
err = client.EnsureFrame(myframe)

// Send a SetBit query. PilosaException is thrown if execution of the query fails.
err = client.Query(myframe.SetBit(5, 42), nil)

// Send a Bitmap query. PilosaException is thrown if execution of the query fails.
response, err = client.Query(myframe.Bitmap(5), nil)

// Get the result
result := response.Result()
// Deal with the result
if result != nil {
    bits := result.Bitmap.Bits
    fmt.Println("Got bits: %v", bits)
}

// You can batch queries to improve throughput
response, err = client.Query(
    mydb.BatchQuery(
        myframe.Bitmap(5),
        myframe.Bitmap(10),
    ), nil
)

for _, result := range reponse.Results() {
    // Deal with the result
}

```

### Data Model and Queries

#### Databases and Frames

*Database* and *frame*s are the main data models of Pilosa. You can check the [Pilosa documentation](https://www.pilosa.com/docs) for more detail about the data model.

`NewDatabase` function is used to create a database object. Note that, this does not create a database on the server, the database object just defines the schema.

```go
repository, err := NewDatabase("repository", nil)
```

Databases support changing the column label and time quantum (*resolution*). `DatabaseOptions` structs store that kind of data. In order to attach a `DatabaseOptions` struct to a `Database` struct, just pass it as the second argument to `NewDatabase`:

```go
options := &pilosa.DatabaseOptions{
    ColumnLabel: "repo_id",
    TimeQuantum: TimeQuantumYearMonth,
}

repository, err := pilosa.NewDatabase("repository", options);
```

Frames are created with a call to `Frame` function of a database:

```go
stargazer, err := repository.Frame("stargazer", nil)
```

Similar to database objects, you can pass custom options to frames:

```go
stargazerOptions, err := &pilosa.FrameOptions{
    RowLabel: "stargazer_id",
    TimeQuantum: TimeQuantumYearMonthDay,
}

stargazer, err := repository.Frame("stargazer", stargazerOptions);
```

#### Queries

Once you have database and frame structs created, you can create queries for those. Some of the queries work on the columns; corresponding methods are attached to the database. Other queries work on rows, with related methods attached to frames.

For instance, `Bitmap` queries work on rows; use a frame object to create those queries:

```go
bitmapQuery := stargazer.Bitmap(1, 100)  // corresponds to PQL: Bitmap(frame='stargazer', stargazer_id=1)
```

`Union` queries work on columns; use the database object to create them:

```go
query := repository.Union(bitmapQuery1, bitmapQuery2)
```

In order to increase througput, you may want to batch queries sent to the Pilosa server. `database.BatchQuery` function is used for that purpose:

```go
query := repository.BatchQuery(
    stargazer.Bitmap(1, 100),
    repository.Union(stargazer.Bitmap(100, 200), stargazer.Bitmap(5, 100))
)
```

The recommended way of creating query structs is, using dedicated methods attached to database and frame objects. But sometimes it would be desirable to send raw queries to Pilosa. You can use `database.RawQuery` method for that. Note that, query string is not validated before sending to the server:

```go
query := repository.RawQuery("Bitmap(frame='stargazer', stargazer_id=5)")
```

Please check [Pilosa documentation](https://www.pilosa.com/docs) for PQL details. Here is a list of methods corresponding to PQL calls:

Database:

* `Union(bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, ...) *PQLBitmapQuery`
* `Intersect(bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, ...) *PQLBitmapQuery`
* `Difference(bitmap1 *PQLBitmapQuery, bitmap2 *PQLBitmapQuery, ...) *PQLBitmapQuery`
* `Count(bitmap *PQLBitmapQuery) *PQLBaseQuery`
* `SetProfileAttrs(columnID uint64, attrs map[string]interface{}) *PQLBaseQuery`

Frame:

* `Bitmap(rowID uint64) *PQLBitmapQuery`
* `SetBit(rowID uint64, columnID uint64) *PQLBaseQuery`
* `ClearBit(rowID uint64, columnID uint64) *PQLBaseQuery`
* `TopN(n uint64) *PQLBitmapQuery`
* `BitmapTopN(n uint64, bitmap *PQLBitmapQuery) *PQLBitmapQuery`
* `FilterFieldTopN(n uint64, bitmap *PQLBitmapQuery, field string, values ...interface{}) *PQLBitmapQuery`
* `Range(rowID uint64, start time.Time, end time.Time) *PQLBitmapQuery`
* `SetBitmapAttrs(rowID uint64, attrs map[string]interface{}) *PQLBaseQuery`

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

A Pilosa URI is represented by `pilosa.URI` struct. Below is a few ways to create `URI` objects:

```go
// create the default URI: http://localhost:10101
uri1 := pilosa.DefaultURI()

// create a URI from string address
uri2, err := pilosa.NewURIFromAddress("db1.pilosa.com:20202");

// create a URI with the given host and port
uri3, err := pilosa.NewURIFromHostPort("db1.pilosa.com", 20202);
``` 

### Pilosa Client

In order to interact with a Pilosa server, an instance of `pilosa.Client` should be created. The client is thread-safe and uses a pool of connections to the server, so we recommend creating a single instance of the client and sharing it when necessary.

If the Pilosa server is running at the default address (`http://localhost:10101`) you can create the default client with default options using:

```go
client := pilosa.DefaultClient()
```

To use a a custom server address, you can use the `NewClientWithURI` function:

```go
uri, err := pilosa.NewURIFromAddress("http://db1.pilosa.com:15000")
if err != nil {
    // Deal with the error
}
client := pilosa.NewClientWithURI(uri)
```

If you are running a cluster of Pilosa servers, you can create a `Cluster` struct that keeps addresses of those servers for increased robustness:

```go
uri1, err := pilosa.NewURIFromAddress(":10101")
uri2, err := pilosa.NewURIFromAddress(":10110")
uri3, err := pilosa.NewURIFromAddress(":10111")
cluster := pilosa.NewClusterWithHost(uri1, uri2, uri3)

// Create a client with the cluster
client := pilosa.NewClientWithCluster(cluster, nil)
```

It is possible to customize the behaviour of the underlying HTTP client by passing a `ClientOptions` struct to the `NewClientWithCluster` function:

```go
options = &pilosa.ClientOptions{
    ConnectTimeout: 1000,  // if can't connect in  a second, close the connection
    setSocketTimeout: 10000,  // if no response received in 10 seconds, close the connection
    PoolSizePerRoute: 3,  // number of connections in the pool per host
    TotalPoolSize: 10,  // number of total connections in the pool  
}

client := pilosa.NewClientWithCluster(cluster, options)
```

Once you create a client, you can create databases, frames and start sending queries.

Here is how you would create a database and frame:

```go
// materialize repository database instance initialized before
err := client.CreateDatabase(repository)

// materialize stargazer frame instance initialized before
err :=client.CreateFrame(stargazer)
```

If the database or frame exists on the server, non-nil errors would be returned. You can use `EnsureDatabase` and `EnsureFrame` functions to ignore existing databases and frames.

You can send queries to a Pilosa server using the `Query` function of the `Client` struct:

```go
response, err := client.Query(frame.Bitmap(5), nil);
```

The second argument of `Query` function is of type `QueryOptions`:

```go
options = &pilosa.QueryOptions{
    Profiles: true,  // return column data in the response
}

response := client.Query(frame.Bitmap(5), options)
```

### Server Response

When a query is sent to a Pilosa server, the server fulfills the query or sends an error message. In the latter case, a `PilosaError` struct is returned, otherwise a `QueryResponse` struct is returned.

A `QueryResponse` struct may contain zero or more results of `QueryResult` type. You can access all results using `Results` function of `QueryResponse`, which returns a list of `QueryResult` objects. Or, using `Result` method, which returns the first result if there are any or `nil` otherwise:

```go
response, err := client.Query(frame.Bitmap(5). nil)
if err != nil {
    // deal with the error
}

// check that there's a result and act on it
result := response.Result()
if result != nil {
    // act on the result
}

// iterate on all results
for result := range response.Results() {
    // act on the result
}
```

Similarly, a `QueryResponse` struct may include a number of profiles (column objects), if `Profiles` query option was set to `true`:

```go
var profile *pilosa.ProfileItem

// check that there's a profile and act on it
profile = response.Profile()
if (profile != null) {
    // act on the profile
}

// iterate on all profiles
for profile = range response.Profiles() {
    // act on the profile
}
```

`QueryResult` objects contain

* `Bitmap` field to retrieve a bitmap result,
* `CountItems` fied to retrieve column count per row ID entries returned from `TopN` queries,
* `Count` field to retrieve the number of rows per the given row ID returned from `Count` queries.

```go
bitmap := response.Bitmap
bits := bitmap.Bits
attributes := bitmap.Attributes

countItems := response.CountItems

count := response.Count
```

## Contribution

Please check our [Contributor's Guidelines](https://github.com/pilosa/pilosa/CONTRIBUTING.md).

1. Sign the [Developer Agreement](https://wwww.pilosa.com/developer-agreement) so we can include your contibution in our codebase.
2. Fork this repo and add it as upstream: `git remote add upstream git@github.com:pilosa/go-pilosa.git`.
3. Make sure all tests pass (use `make test-all`) and be sure that the tests cover all statements in your code (we aim for 100% test coverage).
4. Commit your code to a feature branch and send a pull request to the `master` branch of our repo.

The sections below assume your platform has `make`. Otherwise you can view the corresponding steps of the `Makefile`.

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
make generate-proto
```

## License

**TODO**