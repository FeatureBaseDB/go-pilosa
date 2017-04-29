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
	"errors"

	"github.com/pilosa/go-pilosa/internal"
)

// QueryResponse represents the response from a Pilosa query
type QueryResponse struct {
	results      []*QueryResult
	columns      []*ColumnItem
	ErrorMessage string
	Success      bool
}

func newQueryResponseFromInternal(response *internal.QueryResponse) (*QueryResponse, error) {
	if response.Err != "" {
		return &QueryResponse{
			ErrorMessage: response.Err,
			Success:      false,
		}, nil
	}
	results := make([]*QueryResult, 0, len(response.Results))
	for _, r := range response.Results {
		result, err := newQueryResultFromInternal(r)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	columns := make([]*ColumnItem, 0, len(response.ColumnAttrSets))
	for _, p := range response.ColumnAttrSets {
		columnItem, err := newColumnItemFromInternal(p)
		if err != nil {
			return nil, err
		}
		columns = append(columns, columnItem)
	}

	return &QueryResponse{
		results: results,
		columns: columns,
		Success: true,
	}, nil
}

// Results returns all results in the response
func (qr *QueryResponse) Results() []*QueryResult {
	return qr.results
}

// Result returns the first result or nil
func (qr *QueryResponse) Result() *QueryResult {
	if len(qr.results) == 0 {
		return nil
	}
	return qr.results[0]
}

// Columns returns all columns in the response
func (qr *QueryResponse) Columns() []*ColumnItem {
	return qr.columns
}

// Column returns the first column or nil
func (qr *QueryResponse) Column() *ColumnItem {
	if len(qr.columns) == 0 {
		return nil
	}
	return qr.columns[0]
}

// QueryResult represent one of the results in the response
type QueryResult struct {
	Bitmap     *BitmapResult
	CountItems []*CountResultItem
	Count      uint64
}

func newQueryResultFromInternal(result *internal.QueryResult) (*QueryResult, error) {
	var bitmapResult *BitmapResult
	var err error
	if result.Bitmap != nil {
		bitmapResult, err = newBitmapResultFromInternal(result.Bitmap)
		if err != nil {
			return nil, err
		}
	}
	return &QueryResult{
		Bitmap:     bitmapResult,
		CountItems: countItemsFromInternal(result.Pairs),
		Count:      result.N,
	}, nil
}

// CountResultItem represents a result from TopN call
type CountResultItem struct {
	ID    uint64
	Count uint64
}

func countItemsFromInternal(items []*internal.Pair) []*CountResultItem {
	result := make([]*CountResultItem, 0, len(items))
	for _, v := range items {
		result = append(result, &CountResultItem{ID: v.Key, Count: v.Count})
	}
	return result
}

// BitmapResult represents a result from Bitmap, Union, Intersect, Difference and Range PQL calls
type BitmapResult struct {
	Attributes map[string]interface{}
	Bits       []uint64
}

func newBitmapResultFromInternal(bitmap *internal.Bitmap) (*BitmapResult, error) {
	attrs, err := convertInternalAttrsToMap(bitmap.Attrs)
	if err != nil {
		return nil, err
	}
	result := &BitmapResult{
		Attributes: attrs,
		Bits:       bitmap.Bits,
	}
	return result, nil
}

const (
	stringType = 1
	intType    = 2
	boolType   = 3
	floatType  = 4
)

func convertInternalAttrsToMap(attrs []*internal.Attr) (attrsMap map[string]interface{}, err error) {
	attrsMap = make(map[string]interface{}, len(attrs))
	for _, attr := range attrs {
		switch attr.Type {
		case stringType:
			attrsMap[attr.Key] = attr.StringValue
		case intType:
			attrsMap[attr.Key] = attr.IntValue
		case boolType:
			attrsMap[attr.Key] = attr.BoolValue
		case floatType:
			attrsMap[attr.Key] = attr.FloatValue
		default:
			return nil, errors.New("Unknown attribute type")
		}
	}

	return attrsMap, nil
}

// ColumnItem representes a column in the index
type ColumnItem struct {
	ID         uint64
	Attributes map[string]interface{}
}

func newColumnItemFromInternal(column *internal.ColumnAttrSet) (*ColumnItem, error) {
	attrs, err := convertInternalAttrsToMap(column.Attrs)
	if err != nil {
		return nil, err
	}
	return &ColumnItem{
		ID:         column.ID,
		Attributes: attrs,
	}, nil
}
