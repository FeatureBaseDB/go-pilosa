package pilosa

import (
	"sync"

	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
)

type ExpClient struct {
	*Client
	importers map[string]*threadsafeImporter
	mu        sync.RWMutex
}

func NewExpClient(c *Client) *ExpClient {
	return &ExpClient{
		Client:    c,
		importers: make(map[string]*threadsafeImporter),
	}
}

func (e *ExpClient) Flush() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for index, tsi := range e.importers {
		err := e.importIndex(index, tsi)
		if err != nil {
			return errors.Wrapf(err, "import index '%s'", index)
		}
	}
	return nil
}

func (e *ExpClient) importIndex(index string, tsi *threadsafeImporter) error {
	tsi.mu.RLock()
	defer tsi.mu.RUnlock()
	for fs, safeBM := range tsi.fragments {
		err := e.importFragment(index, fs, safeBM)
		if err != nil {
			return errors.Wrapf(err, "importing fragment %v", fs)
		}
	}
	return nil
}

func (e *ExpClient) importFragment(index string, fs fieldShard, safeBM *safeBitmap) error {
	nodes, err := e.Client.FetchFragmentNodes(index, fs.shard)
	if err != nil {
		return errors.Wrap(err, "fetching frag nodes")
	}
	safeBM.mu.Lock()
	defer safeBM.mu.Unlock()
	err = e.Client.ImportRoaringBitmap(nodes[0].URI(),
		&Field{name: fs.field, index: &Index{name: index}},
		fs.shard, ViewImports{"": safeBM.Bitmap}, &ImportOptions{},
	)
	if err != nil {
		return errors.Wrap(err, "importing")
	}
	safeBM.Bitmap = roaring.NewBitmap() // TODO somehow reset to avoid allocating
	return nil
}

// ExpImporter is a thread-unsafe importer.
type ExpImporter struct {
	index      string
	fragments  map[fieldShard]*roaring.Bitmap
	ShardWidth uint64
	tsImporter *threadsafeImporter
}

type fieldShard struct {
	field string
	shard uint64
}

func (c *ExpClient) NewImporter(index string) *ExpImporter {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.importers[index]; !ok {
		c.importers[index] = newThreadsafeImporter(index)
	}
	return &ExpImporter{
		index:      index,
		fragments:  make(map[fieldShard]*roaring.Bitmap),
		ShardWidth: 1048576,
		tsImporter: c.importers[index],
	}
}

func (i *ExpImporter) Add(col uint64, field string, row uint64) {
	fs := fieldShard{field: field, shard: col / i.ShardWidth}
	bm, ok := i.fragments[fs]
	if !ok {
		bm = roaring.NewBitmap()
		i.fragments[fs] = bm
	}
	bm.DirectAdd(row*i.ShardWidth + col%i.ShardWidth)
}

func (i *ExpImporter) Flush() {
	i.tsImporter.mu.RLock() // TODO consider moving this into the loop
	for fs, bm := range i.fragments {
		safeBM, ok := i.tsImporter.fragments[fs]
		if !ok {
			i.tsImporter.mu.RUnlock()
			i.tsImporter.mu.Lock()
			safeBM, ok = i.tsImporter.fragments[fs]
			if !ok {
				safeBM = newSafeBitmap()
				i.tsImporter.fragments[fs] = safeBM
			}
			i.tsImporter.mu.Unlock()
			i.tsImporter.mu.RLock()
		}
		safeBM.UnionInPlace(bm)
	}
	i.tsImporter.mu.RUnlock()
	for fs := range i.fragments {
		delete(i.fragments, fs)
	}
}

type threadsafeImporter struct {
	index     string
	fragments map[fieldShard]*safeBitmap
	mu        sync.RWMutex
}

func newThreadsafeImporter(index string) *threadsafeImporter {
	return &threadsafeImporter{
		index:     index,
		fragments: make(map[fieldShard]*safeBitmap),
	}
}

type safeBitmap struct {
	*roaring.Bitmap
	mu sync.Mutex
}

func newSafeBitmap() *safeBitmap {
	return &safeBitmap{
		Bitmap: roaring.NewBitmap(),
	}
}

func (b *safeBitmap) UnionInPlace(others ...*roaring.Bitmap) {
	b.mu.Lock()
	b.Bitmap.UnionInPlace(others...)
	b.mu.Unlock()
}
