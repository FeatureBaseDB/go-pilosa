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
	QueryResultTypeRow
	QueryResultTypePairs
	QueryResultTypeValCount
	QueryResultTypeUint64
	QueryResultTypeBool
	QueryResultTypeRowIDs // this is not used by the client
	QueryResultTypeGroupCounts
	QueryResultTypeRowIdentifiers
)

// QueryResponse represents the response from a Pilosa query.
type QueryResponse struct {
	ResultList   []QueryResult `json:"results,omitempty"`
	ColumnList   []ColumnItem  `json:"columns,omitempty"`
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
	columns := make([]ColumnItem, 0, len(response.ColumnAttrSets))
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
func (qr *QueryResponse) Columns() []ColumnItem {
	return qr.ColumnList
}

// Column returns the first column.
func (qr *QueryResponse) Column() ColumnItem {
	if len(qr.ColumnList) == 0 {
		return ColumnItem{}
	}
	return qr.ColumnList[0]
}

// QueryResult represents one of the results in the response.
type QueryResult interface {
	Type() uint32
	Row() RowResult
	CountItems() []CountResultItem
	Count() int64
	Value() int64
	Changed() bool
	RowIdentifiers() RowIdentifiersResult
	GroupCounts() []GroupCount
}

func newQueryResultFromInternal(result *pbuf.QueryResult) (QueryResult, error) {
	switch result.Type {
	case QueryResultTypeNil:
		return NilResult{}, nil
	case QueryResultTypeRow:
		return newRowResultFromInternal(result.Row)
	case QueryResultTypePairs:
		return countItemsFromInternal(result.Pairs), nil
	case QueryResultTypeValCount:
		return &ValCountResult{
			Val: result.ValCount.Val,
			Cnt: result.ValCount.Count,
		}, nil
	case QueryResultTypeUint64:
		return IntResult(result.N), nil
	case QueryResultTypeBool:
		return BoolResult(result.Changed), nil
	case QueryResultTypeRowIdentifiers:
		return &RowIdentifiersResult{
			IDs:  result.RowIdentifiers.Rows,
			Keys: result.RowIdentifiers.Keys,
		}, nil
	case QueryResultTypeGroupCounts:
		return groupCountsFromInternal(result.GroupCounts), nil

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
	result := make([]CountResultItem, 0, len(items))
	for _, v := range items {
		result = append(result, CountResultItem{ID: v.ID, Key: v.Key, Count: v.Count})
	}
	return TopNResult(result)
}

type TopNResult []CountResultItem

func (TopNResult) Type() uint32                         { return QueryResultTypePairs }
func (TopNResult) Row() RowResult                       { return RowResult{} }
func (t TopNResult) CountItems() []CountResultItem      { return t }
func (TopNResult) Count() int64                         { return 0 }
func (TopNResult) Value() int64                         { return 0 }
func (TopNResult) Changed() bool                        { return false }
func (TopNResult) GroupCounts() []GroupCount            { return nil }
func (TopNResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

// RowResult represents a result from Row, Union, Intersect, Difference and Range PQL calls.
type RowResult struct {
	Attributes map[string]interface{} `json:"attrs"`
	Columns    []uint64               `json:"columns"`
	Keys       []string               `json:"keys"`
}

func newRowResultFromInternal(row *pbuf.Row) (*RowResult, error) {
	attrs, err := convertInternalAttrsToMap(row.Attrs)
	if err != nil {
		return nil, err
	}
	result := &RowResult{
		Attributes: attrs,
		Columns:    row.Columns,
		Keys:       row.Keys,
	}
	return result, nil
}

func (RowResult) Type() uint32                         { return QueryResultTypeRow }
func (b RowResult) Row() RowResult                     { return b }
func (RowResult) CountItems() []CountResultItem        { return nil }
func (RowResult) Count() int64                         { return 0 }
func (RowResult) Value() int64                         { return 0 }
func (RowResult) Changed() bool                        { return false }
func (RowResult) GroupCounts() []GroupCount            { return nil }
func (RowResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

func (b RowResult) MarshalJSON() ([]byte, error) {
	columns := b.Columns
	if columns == nil {
		columns = []uint64{}
	}
	keys := b.Keys
	if keys == nil {
		keys = []string{}
	}
	return json.Marshal(struct {
		Attributes map[string]interface{} `json:"attrs"`
		Columns    []uint64               `json:"columns"`
		Keys       []string               `json:"keys"`
	}{
		Attributes: b.Attributes,
		Columns:    columns,
		Keys:       keys,
	})
}

type ValCountResult struct {
	Val int64 `json:"val"`
	Cnt int64 `json:"count"`
}

func (ValCountResult) Type() uint32                         { return QueryResultTypeValCount }
func (ValCountResult) Row() RowResult                       { return RowResult{} }
func (ValCountResult) CountItems() []CountResultItem        { return nil }
func (c ValCountResult) Count() int64                       { return c.Cnt }
func (c ValCountResult) Value() int64                       { return c.Val }
func (ValCountResult) Changed() bool                        { return false }
func (ValCountResult) GroupCounts() []GroupCount            { return nil }
func (ValCountResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

type IntResult int64

func (IntResult) Type() uint32                         { return QueryResultTypeUint64 }
func (IntResult) Row() RowResult                       { return RowResult{} }
func (IntResult) CountItems() []CountResultItem        { return nil }
func (i IntResult) Count() int64                       { return int64(i) }
func (IntResult) Value() int64                         { return 0 }
func (IntResult) Changed() bool                        { return false }
func (IntResult) GroupCounts() []GroupCount            { return nil }
func (IntResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

type BoolResult bool

func (BoolResult) Type() uint32                         { return QueryResultTypeBool }
func (BoolResult) Row() RowResult                       { return RowResult{} }
func (BoolResult) CountItems() []CountResultItem        { return nil }
func (BoolResult) Count() int64                         { return 0 }
func (BoolResult) Value() int64                         { return 0 }
func (b BoolResult) Changed() bool                      { return bool(b) }
func (BoolResult) GroupCounts() []GroupCount            { return nil }
func (BoolResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

type NilResult struct{}

func (NilResult) Type() uint32                         { return QueryResultTypeNil }
func (NilResult) Row() RowResult                       { return RowResult{} }
func (NilResult) CountItems() []CountResultItem        { return nil }
func (NilResult) Count() int64                         { return 0 }
func (NilResult) Value() int64                         { return 0 }
func (NilResult) Changed() bool                        { return false }
func (NilResult) GroupCounts() []GroupCount            { return nil }
func (NilResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

type FieldRow struct {
	FieldName string `json:"field"`
	RowID     uint64 `json:"rowID"`
	RowKey    string `json:"rowKey"`
}

type GroupCount struct {
	Groups []FieldRow `json:"groups"`
	Count  int64      `json:"count"`
}

type GroupCountResult []GroupCount

func (GroupCountResult) Type() uint32                         { return QueryResultTypeRowIdentifiers }
func (GroupCountResult) Row() RowResult                       { return RowResult{} }
func (GroupCountResult) CountItems() []CountResultItem        { return nil }
func (GroupCountResult) Count() int64                         { return 0 }
func (GroupCountResult) Value() int64                         { return 0 }
func (GroupCountResult) Changed() bool                        { return false }
func (r GroupCountResult) GroupCounts() []GroupCount          { return r }
func (GroupCountResult) RowIdentifiers() RowIdentifiersResult { return RowIdentifiersResult{} }

type RowIdentifiersResult struct {
	IDs  []uint64 `json:"ids"`
	Keys []string `json:"keys,omitempty"`
}

func (RowIdentifiersResult) Type() uint32                           { return QueryResultTypeRowIdentifiers }
func (RowIdentifiersResult) Row() RowResult                         { return RowResult{} }
func (RowIdentifiersResult) CountItems() []CountResultItem          { return nil }
func (RowIdentifiersResult) Count() int64                           { return 0 }
func (RowIdentifiersResult) Value() int64                           { return 0 }
func (RowIdentifiersResult) Changed() bool                          { return false }
func (RowIdentifiersResult) GroupCounts() []GroupCount              { return nil }
func (r RowIdentifiersResult) RowIdentifiers() RowIdentifiersResult { return r }

func groupCountsFromInternal(items []*pbuf.GroupCount) GroupCountResult {
	result := make([]GroupCount, 0, len(items))
	for _, g := range items {
		groups := make([]FieldRow, 0, len(g.Group))
		for _, f := range g.Group {
			groups = append(groups, FieldRow{
				FieldName: f.Field,
				RowID:     f.RowID,
				RowKey:    f.RowKey,
			})
		}
		result = append(result, GroupCount{
			Groups: groups,
			Count:  int64(g.Count),
		})
	}
	return GroupCountResult(result)
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

func newColumnItemFromInternal(column *pbuf.ColumnAttrSet) (ColumnItem, error) {
	attrs, err := convertInternalAttrsToMap(column.Attrs)
	if err != nil {
		return ColumnItem{}, err
	}
	return ColumnItem{
		ID:         column.ID,
		Key:        column.Key,
		Attributes: attrs,
	}, nil
}
