package pilosa

import (
	"encoding/gob"
	"io"
)

type importLog struct {
	Index string
	Path  string
	Shard uint64
	Data  []byte
}

type Encoder interface {
	Encode(thing interface{}) error
}

func NewImportLogEncoder(w io.Writer) Encoder {
	return gob.NewEncoder(w)
}

type Decoder interface {
	Decode(thing interface{}) error
}

func NewImportLogDecoder(r io.Reader) Decoder {
	return gob.NewDecoder(r)
}
