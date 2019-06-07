package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"

	"github.com/jaffee/commandeer/cobrafy"
	"github.com/pilosa/go-pilosa"
	pbuf "github.com/pilosa/go-pilosa/gopilosa_pbuf"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
)

type Main struct {
	Dir      string `help:"Directory to walk looking for fragment data."`
	Index    string
	Field    string
	Workers  int `help:"Number of worker goroutines to run."`
	Pilosa   []string
	Shards   uint64        `help:"Number of shards into which to ingest"`
	Duration time.Duration `help:"How long to run the import"`

	shardNodes map[uint64][]pilosa.URI
}

func NewMain() *Main {
	return &Main{
		Dir:     "frags",
		Index:   "fragtest",
		Field:   "field",
		Workers: 8,
		Pilosa:  []string{"localhost:10101"},
		Shards:  10,
	}
}

func (m *Main) Run() error {
	fragments := make([]*roaring.Bitmap, 0)

	// walk all files in directory structure and load the ones which are roaring bitmaps.
	err := filepath.Walk(m.Dir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return errors.Wrap(err, "walk func")
			}
			if info.IsDir() {
				return nil
			}
			f, err := os.Open(path)
			data, err := ioutil.ReadAll(f)
			if err != nil {
				return errors.Wrap(err, "reading all")
			}
			bm := roaring.NewFileBitmap()
			err = bm.UnmarshalBinary(data)
			if err != nil {
				log.Printf("%s was not a valid roaring bitmap: %v", path, err)
			}
			fragments = append(fragments, bm)
			return nil
		})
	if err != nil {
		return errors.Wrap(err, "walking file path")
	}

	if len(fragments) == 0 {
		return errors.New("no valid bitmaps found.")
	}
	fmt.Printf("found %d bitmap files\n", len(fragments))

	client, err := pilosa.NewClient(m.Pilosa)
	if err != nil {
		return errors.Wrapf(err, "getting client for %v", m.Pilosa)
	}
	sch, err := client.Schema()
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}
	idx := sch.Index(m.Index)
	idx.Field(m.Field)
	err = client.SyncSchema(sch)
	if err != nil {
		return errors.Wrap(err, "syncing schema")
	}

	m.shardNodes, err = client.ShardNodes(m.Index, m.Shards)
	if err != nil {
		return errors.Wrap(err, "getting shard nodes")
	}

	eg := errgroup.Group{}
	done := make(chan struct{})
	for i := 0; i < m.Workers; i++ {
		i := i
		eg.Go(func() error {
			return m.importWorker(i, client, fragments, done)
		})
	}
	if m.Duration > 0 {
		fmt.Println("sleeping")
		time.Sleep(m.Duration)
		close(done)
	}

	return eg.Wait()
}

func main() {
	err := cobrafy.Execute(NewMain())
	if err != nil {
		log.Fatal(err)
	}
}

func (m *Main) importWorker(num int, client *pilosa.Client, fragments []*roaring.Bitmap, done chan struct{}) error {
	idx := num % len(fragments)
	path := fmt.Sprintf("/index/%s/field/%s/import-roaring/", m.Index, m.Field)
	headers := map[string]string{
		"Content-Type": "application/x-protobuf",
		"Accept":       "application/x-protobuf",
		"PQL-Version":  pilosa.PQLVersion,
	}
	for {
		shard := rand.Uint64() % m.Shards
		hosts, ok := m.shardNodes[shard]
		if !ok {
			panic("tried to get unknown shard")
		}

		bitmap := fragments[idx]
		data := &bytes.Buffer{}
		bitmap.WriteTo(data)
		req := &pbuf.ImportRoaringRequest{
			Views: []*pbuf.ImportRoaringRequestView{{Data: data.Bytes()}},
		}
		r, err := proto.Marshal(req)
		if err != nil {
			return errors.Wrap(err, "marshaling request to protobuf")
		}

		resp, err := client.ExperimentalDoRequest(&hosts[0], "POST", path+strconv.Itoa(int(shard)), headers, r)
		if err != nil {
			return errors.Wrap(err, "error doing request")
		}
		fmt.Println(resp)

		idx = (idx + 1) % len(fragments)
		select {
		case <-done:
			return nil
		}
	}
}
