package pilosa

import "github.com/pilosa/gopilosa/internal"

// QueryResponse represents the response from a Pilosa query
type QueryResponse struct {
	Results      []*QueryResult
	ErrorMessage string
	IsSuccess    bool
}

func newQueryResponseFromInternal(response *internal.QueryResponse) (*QueryResponse, error) {
	if response.Err != "" {
		return &QueryResponse{
			ErrorMessage: response.Err,
			IsSuccess:    false,
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
	return &QueryResponse{
		Results:   results,
		IsSuccess: true,
	}, nil
}

// GetResult returns the first result or nil
func (qr *QueryResponse) GetResult() *QueryResult {
	if qr.Results == nil || len(qr.Results) == 0 {
		return nil
	}
	return qr.Results[0]
}

// QueryResult represent one of the results in the response
type QueryResult struct {
	BitmapResult *BitmapResult
	CountItems   []*CountResultItem
	Count        uint64
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
		BitmapResult: bitmapResult,
		CountItems:   countItemsFromInternal(result.Pairs),
		Count:        result.N,
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
