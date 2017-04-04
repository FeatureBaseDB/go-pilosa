package pilosa

import (
	"errors"

	"github.com/pilosa/go-client-pilosa/internal"
)

// QueryResponse represents the response from a Pilosa query
type QueryResponse struct {
	results      []*QueryResult
	profiles     []*ProfileItem
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
	profiles := make([]*ProfileItem, 0, len(response.Profiles))
	for _, p := range response.Profiles {
		profileItem, err := newProfileItemFromInternal(p)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, profileItem)
	}

	return &QueryResponse{
		results:  results,
		profiles: profiles,
		Success:  true,
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

// Profiles returns all profiles in the response
func (qr *QueryResponse) Profiles() []*ProfileItem {
	return qr.profiles
}

// Profile returns the first profile or nil
func (qr *QueryResponse) Profile() *ProfileItem {
	if len(qr.profiles) == 0 {
		return nil
	}
	return qr.profiles[0]
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

const (
	stringType = 1
	uintType   = 2
	boolType   = 3
	floatType  = 4
)

func convertInternalAttrsToMap(attrs []*internal.Attr) (attrsMap map[string]interface{}, err error) {
	attrsMap = make(map[string]interface{}, len(attrs))
	for _, attr := range attrs {
		switch attr.Type {
		case stringType:
			attrsMap[attr.Key] = attr.StringValue
		case uintType:
			attrsMap[attr.Key] = attr.UintValue
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

// ProfileItem representes a column in the database
type ProfileItem struct {
	ID         uint64
	Attributes map[string]interface{}
}

func newProfileItemFromInternal(profile *internal.Profile) (*ProfileItem, error) {
	attrs, err := convertInternalAttrsToMap(profile.Attrs)
	if err != nil {
		return nil, err
	}
	return &ProfileItem{
		ID:         profile.ID,
		Attributes: attrs,
	}, nil
}
