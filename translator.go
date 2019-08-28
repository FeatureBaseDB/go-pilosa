package pilosa

type Translator interface {
	GetCol(index, key string) (uint64, bool, error)
	GetRow(index, field, key string) (uint64, bool, error)
	AddCols(index string, keys []string, values []uint64) error
	AddRows(index, field string, keys []string, values []uint64) error
}

// MapTranslator implements Translator using in-memory maps. It is not
// threadsafe.
type MapTranslator struct {
	indexes map[string]map[string]uint64
	fields  map[indexfield]map[string]uint64
}

func NewMapTranslator() *MapTranslator {
	return &MapTranslator{
		indexes: make(map[string]map[string]uint64),
		fields:  make(map[indexfield]map[string]uint64),
	}
}

type indexfield struct {
	index string
	field string
}

func (t *MapTranslator) GetCol(index, key string) (uint64, bool, error) {
	if idx, ok := t.indexes[index]; ok {
		if val, ok := idx[key]; ok {
			return val, true, nil
		}
	}
	return 0, false, nil
}

func (t *MapTranslator) AddCols(index string, keys []string, values []uint64) error {
	for i := range keys {
		key, value := keys[i], values[i]
		idxMap, ok := t.indexes[index]
		if !ok {
			idxMap = make(map[string]uint64)
		}
		idxMap[key] = value
		t.indexes[index] = idxMap
	}
	return nil
}

func (t *MapTranslator) GetRow(index, field, key string) (uint64, bool, error) {
	if fld, ok := t.fields[indexfield{index: index, field: field}]; ok {
		if val, ok := fld[key]; ok {
			return val, true, nil
		}
	}
	return 0, false, nil
}

func (t *MapTranslator) AddRows(index, field string, keys []string, values []uint64) error {
	for i := range keys {
		key, value := keys[i], values[i]
		keyMap, ok := t.fields[indexfield{index: index, field: field}]
		if !ok {
			keyMap = make(map[string]uint64)
		}
		keyMap[key] = value
		t.fields[indexfield{index: index, field: field}] = keyMap
	}
	return nil
}
