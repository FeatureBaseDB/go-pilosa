package pilosa

type Translator struct {
	indexes map[string]map[string]uint64
	fields  map[indexfield]map[string]uint64
}

func NewTranslator() *Translator {
	return &Translator{
		indexes: make(map[string]map[string]uint64),
		fields:  make(map[indexfield]map[string]uint64),
	}
}

type indexfield struct {
	index string
	field string
}

func (t *Translator) GetCol(index, key string) (uint64, bool) {
	if idx, ok := t.indexes[index]; ok {
		if val, ok := idx[key]; ok {
			return val, true
		}
	}
	return 0, false
}

func (t *Translator) AddCol(index, key string, value uint64) {
	idx, ok := t.indexes[index]
	if !ok {
		idx = make(map[string]uint64)
	}
	idx[key] = value
	t.indexes[index] = idx
}

func (t *Translator) GetRow(index, field, key string) (uint64, bool) {
	if fld, ok := t.fields[indexfield{index: index, field: field}]; ok {
		if val, ok := fld[key]; ok {
			return val, true
		}
	}
	return 0, false
}

func (t *Translator) AddRow(index, field, key string, value uint64) {
	keys, ok := t.fields[indexfield{index: index, field: field}]
	if !ok {
		keys = make(map[string]uint64)
	}
	keys[key] = value
	t.fields[indexfield{index: index, field: field}] = keys
}
