package main_test

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/pilosa/go-pilosa"
	picsv "github.com/pilosa/go-pilosa/cmd/picsv"
)

func BenchmarkImportCSV(b *testing.B) {
	m := picsv.NewMain()
	m.BatchSize = 1 << 20
	m.Index = "picsvbench"
	m.File = "marketing-200k.csv"
	getRawData(b, m.File)
	client, err := pilosa.NewClient(m.Pilosa)
	if err != nil {
		b.Fatalf("getting client: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := m.Run()
		if err != nil {
			b.Fatalf("running import: %v", err)
		}
		b.StopTimer()
		err = client.DeleteIndexByName(m.Index)
		if err != nil {
			b.Fatalf("deleting index: %v", err)
		}
		b.StartTimer()
	}

}

func getRawData(t testing.TB, file string) {
	if _, err := os.Open(file); err == nil {
		return
	} else if !os.IsNotExist(err) {
		t.Fatalf("opening %s: %v", file, err)
	}
	// if the file doesn't exist
	f, err := os.Create(file)
	if err != nil {
		t.Fatalf("creating file: %v", err)
	}
	resp, err := http.Get(fmt.Sprintf("https://molecula-sample-data.s3.amazonaws.com/%s", file))
	if err != nil {
		t.Fatalf("getting data: %v", err)
	}
	if resp.StatusCode > 299 {
		t.Fatalf("getting data failed: %v", resp.Status)
	}
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		t.Fatalf("copying data into file: %v", err)
	}

	err = f.Close()
	if err != nil {
		t.Fatalf("closing file: %v", err)
	}

}

func TestImportCSV(t *testing.T) {
	m := picsv.NewMain()
	m.BatchSize = 1 << 20
	m.Index = "testpicsv"
	m.File = "marketing-200k.csv"
	m.Config.SourceFields["age"] = picsv.SourceField{TargetField: "age", Type: "float"}
	m.Config.PilosaFields["age"] = picsv.Field{Type: "int"}
	getRawData(t, m.File)
	client, err := pilosa.NewClient(m.Pilosa)
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}

	defer func() {
		err = client.DeleteIndexByName(m.Index)
		if err != nil {
			t.Fatalf("deleting index: %v", err)
		}
	}()
	err = m.Run()
	if err != nil {
		t.Fatalf("running ingest: %v", err)
	}

	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}

	index := schema.Index(m.Index)
	marital := index.Field("marital")
	converted := index.Field("converted")
	age := index.Field("age")

	tests := []struct {
		query *pilosa.PQLRowQuery
		bash  string
		exp   int64
	}{
		{
			query: marital.Row("married"),
			bash:  `awk -F, '/married/ {print $1,$4}' marketing-200k.csv | sort | uniq | wc`,
			exp:   125514,
		},
		{
			query: converted.Row("no"),
			bash:  `awk -F, '{print $1,$17}' marketing-200k.csv | grep "no" |sort | uniq | wc`,
			exp:   199999,
		},
		{
			query: age.Equals(55),
			bash:  `awk -F, '{print $1,$2}' marketing-200k.csv | grep " 55.0" |sort | uniq | wc`,
			exp:   3282,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			q := index.Count(test.query)
			resp, err := client.Query(q)
			if err != nil {
				t.Fatalf("running query '%s': %v", q.Serialize(), err)
			}
			if resp.Result().Count() != test.exp {
				t.Fatalf("Got unexpected result %d instead of %d for\nquery: %s\nbash: %s", resp.Result().Count(), test.exp, q.Serialize(), test.bash)
			}
		})
	}
}
