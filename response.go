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
	"encoding/json"
	"errors"
	"fmt"

	pbuf "github.com/pilosa/go-pilosa/gopilosa_pbuf"
)

// QueryResponse types.
const (
	QueryResultTypeNil uint32 = iota
	QueryResultTypeBitmap
	QueryResultTypePairs
	QueryResultTypeSumCount
	QueryResultTypeUint64
	QueryResultTypeBool
)

// QueryResponse represents the response from a Pilosa query.
type QueryResponse struct {
	ResultList   []QueryResult `json:"results,omitempty"`
	ColumnList   []*ColumnItem `json:"columns,omitempty"`
	ErrorMessage string        `json:"error-message,omitempty"`
	Success      bool          `json:"success,omitempty"`
}

func newQueryResponseFromInternal(response *pbuf.QueryResponse) (*QueryResponse, error) {
	if response.Err != "" {
		return &QueryResponse{
			ErrorMessage: response.Err,
			Success:      false,
		}, nil
	}
	results := make([]QueryResult, 0, len(response.Results))
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
		ResultList: results,
		ColumnList: columns,
		Success:    true,
	}, nil
}

// Results returns all results in the response.
func (qr *QueryResponse) Results() []QueryResult {
	return qr.ResultList
}

// Result returns the first result or nil.
func (qr *QueryResponse) Result() QueryResult {
	if len(qr.ResultList) == 0 {
		return nil
	}
	return qr.ResultList[0]
}

// Columns returns all columns in the response.
func (qr *QueryResponse) Columns() []*ColumnItem {
	return qr.ColumnList
}

// Column returns the first column or nil.
func (qr *QueryResponse) Column() *ColumnItem {
	if len(qr.ColumnList) == 0 {
		return nil
	}
	return qr.ColumnList[0]
}

// QueryResult represents one of the results in the response.
type QueryResult interface {
	Type() uint32
	Bitmap() BitmapResult
	CountItems() []*CountResultItem
	Count() int64
	Sum() int64
	Changed() bool
}

func newQueryResultFromInternal(result *pbuf.QueryResult) (QueryResult, error) {
	switch result.Type {
	case QueryResultTypeNil:
		return NilResult(false), nil
	case QueryResultTypeBitmap:
		return newBitmapResultFromInternal(result.Bitmap)
	case QueryResultTypePairs:
		return countItemsFromInternal(result.Pairs), nil
	case QueryResultTypeSumCount:
		return &SumCountResult{
			sum:   result.SumCount.Sum,
			count: result.SumCount.Count,
		}, nil
	case QueryResultTypeUint64:
		return IntResult(result.N), nil
	case QueryResultTypeBool:
		return BoolResult(result.Changed), nil
	}
	return nil, ErrUnknownType
}

// CountResultItem represents a result from TopN call.
type CountResultItem struct {
	ID    uint64 `json:"id"`
	Key   string `json:"key,omitempty"`
	Count uint64 `json:"count"`
}

func (c *CountResultItem) String() string {
	if c.Key != "" {
		return fmt.Sprintf("%s:%d", c.Key, c.Count)
	}
	return fmt.Sprintf("%d:%d", c.ID, c.Count)
}

func countItemsFromInternal(items []*pbuf.Pair) TopNResult {
	result := make([]*CountResultItem, 0, len(items))
	for _, v := range items {
		result = append(result, &CountResultItem{ID: v.ID, Key: v.Key, Count: v.Count})
	}
	return TopNResult(result)
}

type TopNResult []*CountResultItem

func (TopNResult) Type() uint32 {
	return QueryResultTypePairs
}

func (TopNResult) Bitmap() BitmapResult {
	return defaultBitmapResult()
}

func (t TopNResult) CountItems() []*CountResultItem {
	return t
}

func (TopNResult) Count() int64 {
	return 0
}

func (TopNResult) Sum() int64 {
	return 0
}

func (TopNResult) Changed() bool {
	return false
}

// BitmapResult represents a result from Bitmap, Union, Intersect, Difference and Range PQL calls.
type BitmapResult struct {
	Attributes map[string]interface{} `json:"attrs"`
	Bits       []uint64               `json:"bits"`
	Keys       []string               `json:"keys"`
}

func newBitmapResultFromInternal(bitmap *pbuf.Bitmap) (*BitmapResult, error) {
	attrs, err := convertInternalAttrsToMap(bitmap.Attrs)
	if err != nil {
		return nil, err
	}
	bits := bitmap.Bits
	if bits == nil {
		bits = []uint64{}
	}
	keys := bitmap.Keys
	if keys == nil {
		keys = []string{}
	}
	result := &BitmapResult{
		Attributes: attrs,
		Bits:       bits,
		Keys:       keys,
	}
	return result, nil
}

func defaultBitmapResult() BitmapResult {
	return BitmapResult{
		Attributes: map[string]interface{}{},
		Bits:       []uint64{},
		Keys:       []string{},
	}
}

func (BitmapResult) Type() uint32 {
	return QueryResultTypeBitmap
}

func (b BitmapResult) Bitmap() BitmapResult {
	return b
}

func (BitmapResult) CountItems() []*CountResultItem {
	return []*CountResultItem{}
}

func (BitmapResult) Count() int64 {
	return 0
}

func (BitmapResult) Sum() int64 {
	return 0
}

func (BitmapResult) Changed() bool {
	return false
}

type SumCountResult struct {
	sum   int64
	count int64
}

func (SumCountResult) Type() uint32 {
	return QueryResultTypeSumCount
}

func (SumCountResult) Bitmap() BitmapResult {
	return defaultBitmapResult()
}

func (SumCountResult) CountItems() []*CountResultItem {
	return []*CountResultItem{}
}

func (c SumCountResult) Count() int64 {
	return c.count
}

func (c SumCountResult) Sum() int64 {
	return c.sum
}

func (SumCountResult) Changed() bool {
	return false
}

func (c SumCountResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Sum   int64 `json:"sum"`
		Count int64 `json:"count"`
	}{
		Sum:   c.sum,
		Count: c.count,
	})
}

type IntResult int64

func (IntResult) Type() uint32 {
	return QueryResultTypeUint64
}

func (IntResult) Bitmap() BitmapResult {
	return defaultBitmapResult()
}

func (IntResult) CountItems() []*CountResultItem {
	return []*CountResultItem{}
}

func (i IntResult) Count() int64 {
	return int64(i)
}

func (IntResult) Sum() int64 {
	return 0
}

func (IntResult) Changed() bool {
	return false
}

type BoolResult bool

func (BoolResult) Type() uint32 {
	return QueryResultTypeBool
}

func (BoolResult) Bitmap() BitmapResult {
	return defaultBitmapResult()
}

func (BoolResult) CountItems() []*CountResultItem {
	return []*CountResultItem{}
}

func (BoolResult) Count() int64 {
	return 0
}

func (BoolResult) Sum() int64 {
	return 0
}

func (b BoolResult) Changed() bool {
	return bool(b)
}

func (b BoolResult) MarshalJSON() ([]byte, error) {
	if b {
		return []byte("true"), nil
	}
	return []byte("false"), nil
}

type NilResult bool

func (NilResult) Type() uint32 {
	return QueryResultTypeNil
}

func (NilResult) Bitmap() BitmapResult {
	return defaultBitmapResult()
}

func (NilResult) CountItems() []*CountResultItem {
	return []*CountResultItem{}
}

func (NilResult) Count() int64 {
	return 0
}

func (NilResult) Sum() int64 {
	return 0
}

func (NilResult) Changed() bool {
	return false
}

func (NilResult) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

const (
	stringType = 1
	intType    = 2
	boolType   = 3
	floatType  = 4
)

func convertInternalAttrsToMap(attrs []*pbuf.Attr) (attrsMap map[string]interface{}, err error) {
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

// ColumnItem represents data about a column.
// Column data is only returned if QueryOptions.Columns was set to true.
type ColumnItem struct {
	ID         uint64                 `json:"id,omitempty"`
	Key        string                 `json:"key,omitempty"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

func newColumnItemFromInternal(column *pbuf.ColumnAttrSet) (*ColumnItem, error) {
	attrs, err := convertInternalAttrsToMap(column.Attrs)
	if err != nil {
		return nil, err
	}
	return &ColumnItem{
		ID:         column.ID,
		Key:        column.Key,
		Attributes: attrs,
	}, nil
}
