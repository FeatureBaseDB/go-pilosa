# Change Log

* **Next**
    * Compatible with Pilosa master, **not compatible with Pilosa 0.8.x releases**.
    * Added `Equals`, `NotEquals` and `NotNull` field operations.
    * **Deprecation** `TimeQuantum` for `IndexOptions`. Use `TimeQuantum` of individual `FrameOptions` instead.
    * **Deprecation** `IndexOptions` struct is deprecated and will be removed in the future.
    * **Deprecation** Passing `IndexOptions` or `nil` to `schema.Index` function.
    * **Deprecation** `RangeEnabled` frame option. All frames have this option `true` on Pilosa 1.0.
    * **Deprecation** `InverseEnabled` frame option and `Frame.InverseBitmap`, `Frame.InverseTopN`, `Frame.InverseBitmapTopN`, `Frame.InverseFilterFieldTopN`, `Frame.InverseRange` functions. Inverse frames will be removed from Pilosa 1.0.
    
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
