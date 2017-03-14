package pilosa

import (
	"reflect"
	"testing"

	"github.com/pilosa/gopilosa/internal"
)

func TestNewBitmapResultFromInternal(t *testing.T) {
	targetAttrs := map[string]interface{}{
		"name":       "some string",
		"age":        uint64(95),
		"registered": true,
		"height":     1.83,
	}
	targetBits := []uint64{5, 10}
	attrs := []*internal.Attr{
		&internal.Attr{Key: "name", StringValue: "some string", Type: 1},
		&internal.Attr{Key: "age", UintValue: 95, Type: 2},
		&internal.Attr{Key: "registered", BoolValue: true, Type: 3},
		&internal.Attr{Key: "height", FloatValue: 1.83, Type: 4},
	}
	bitmap := &internal.Bitmap{
		Attrs: attrs,
		Bits:  []uint64{5, 10},
	}
	result, err := newBitmapResultFromInternal(bitmap)
	if err != nil {
		t.Fatalf("Failed with error: %s", err)
	}
	assertMapEquals(t, targetAttrs, result.GetAttributes())
	if !reflect.DeepEqual(targetBits, result.GetBits()) {
		t.Fatalf("Not equal")
	}
}

func TestNewBitmapResultFromInternalFailure(t *testing.T) {
	attrs := []*internal.Attr{
		&internal.Attr{Key: "name", StringValue: "some string", Type: 99},
	}
	bitmap := &internal.Bitmap{
		Attrs: attrs,
	}
	result, err := newBitmapResultFromInternal(bitmap)
	if result != nil && err == nil {
		t.Fatalf("Should have failed")
	}
}

func assertMapEquals(t *testing.T, map1 map[string]interface{}, map2 map[string]interface{}) {
	if map1 == nil && map2 == nil {
		return
	}
	if map1 == nil && map2 != nil {
		t.Fatalf("map1 is nil, map2 is not nil")
	}
	if map1 != nil && map2 == nil {
		t.Fatalf("map1 is not nil, map is nil")
	}
	if len(map1) != len(map2) {
		t.Fatalf("len(map1) != len(map2)")
	}
	for k := range map1 {
		if map1[k] != map2[k] {
			t.Fatalf("Element %s (map1) != %s (map2)", map1[k], map2[k])
		}
	}
}
