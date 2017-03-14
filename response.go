package pilosa

import "github.com/pilosa/gopilosa/internal"

// BitmapResult represents bitmap data in the response
type BitmapResult struct {
	attributes map[string]interface{}
	bits       []uint64
}

func newBitmapResultFromInternal(bitmap *internal.Bitmap) (*BitmapResult, error) {
	attrs, err := convertInternalAttrsToMap(bitmap.Attrs)
	if err != nil {
		return nil, err
	}
	result := &BitmapResult{
		attributes: attrs,
		bits:       bitmap.Bits,
	}
	return result, nil
}

// GetAttributes returns the attributes of this bitmap result
func (b *BitmapResult) GetAttributes() map[string]interface{} {
	return b.attributes
}

// GetBits returns the bits of this bitmap result
func (b *BitmapResult) GetBits() []uint64 {
	return b.bits
}
