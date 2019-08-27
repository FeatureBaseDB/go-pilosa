package main_test

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/pilosa/go-pilosa"
	picsv "github.com/pilosa/go-pilosa/cmd/picsv"
	"github.com/pkg/errors"
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
	m.BatchSize = 100000
	m.Index = "testpicsv"
	m.File = "marketing-200k.csv"
	m.Config.SourceFields["age"] = picsv.SourceField{TargetField: "age", Type: "float"}
	m.Config.PilosaFields["age"] = picsv.Field{Type: "int"}
	m.Config.IDField = "id"
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

func TestSmallImport(t *testing.T) {
	m := picsv.NewMain()
	m.BatchSize = 1 << 20
	m.Index = "testsample"
	m.File = "testdata/sample.csv"
	m.ConfigFile = "config.json"
	client, err := pilosa.NewClient(m.Pilosa)
	if err != nil {
		t.Fatalf("getting client: %v", err)
	}
	defer func() {
		err = client.DeleteIndexByName(m.Index)
		if err != nil {
			t.Logf("deleting index: %v", err)
		}
	}()
	config := `{
"pilosa-fields": {"size": {"type": "set", "keys": true, "cache-type": "ranked", "cache-size": 100000},
                  "age": {"type": "int"},
                  "color": {"type": "set", "keys": true},
                  "result": {"type": "int"},
                  "dayofweek": {"type": "set", "keys": false, "cache-type": "ranked", "cache-size": 7}
    },
"id-field": "ID",
"id-type": "string",
"source-fields": {
        "Size": {"target-field": "size", "type": "string"},
        "Color": {"target-field": "color", "type": "string"},
        "Age": {"target-field": "age", "type": "int"},
        "Result": {"target-field": "result", "type": "float", "multiplier": 100000000},
        "dayofweek": {"target-field": "dayofweek", "type": "uint64"}
    }
}
`
	data := `
ID,Size,Color,Age,Result,dayofweek
ABDJ,small,green,42,1.13106317,1
HFZP,large,red,99,30.23959735,2
HFZP,small,green,99,NA,3
EJSK,medium,purple,22,20.23959735,1
EJSK,large,green,35,25.13106317,
FEFF,,,,,6
`
	writeFile(t, m.ConfigFile, config)
	writeFile(t, m.File, data)

	err = m.Run()
	if err != nil {
		t.Fatalf("running ingest: %v", err)
	}

	schema, err := client.Schema()
	if err != nil {
		t.Fatalf("getting schema: %v", err)
	}

	index := schema.Index(m.Index)
	size := index.Field("size")
	color := index.Field("color")
	age := index.Field("age")
	result := index.Field("result")
	day := index.Field("dayofweek")

	tests := []struct {
		query   pilosa.PQLQuery
		resType string
		exp     interface{}
	}{
		{
			query:   index.Count(size.Row("small")),
			resType: "count",
			exp:     int64(2),
		},
		{
			query:   size.Row("small"),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "HFZP"},
		},
		{
			query:   color.Row("green"),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "HFZP", "EJSK"},
		},
		{
			query:   age.Equals(99),
			resType: "rowKeys",
			exp:     []string{"HFZP"},
		},
		{
			query:   age.GT(0),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "HFZP", "EJSK"},
		},
		{
			query:   result.GT(0),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "EJSK"},
		},
		{
			query:   result.GT(100000),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "EJSK"},
		},
		{
			query:   day.Row(1),
			resType: "rowKeys",
			exp:     []string{"ABDJ", "EJSK"},
		},
		{
			query:   day.Row(6),
			resType: "rowKeys",
			exp:     []string{"FEFF"},
		},
		{
			query:   index.Count(day.Row(3)),
			resType: "count",
			exp:     int64(1),
		},
		{
			query:   index.Count(day.Row(2)),
			resType: "count",
			exp:     int64(1), // not mutually exclusive!
		},
		{
			query:   size.Row(`""`), // TODO... go-pilosa should probably serialize keys into PQL using quotes.
			resType: "rowKeys",
			exp:     []string{}, // empty strings are ignored rather than ingested
		},
		{
			query:   color.Row(`""`),
			resType: "rowKeys",
			exp:     []string{}, // empty strings are ignored rather than ingested
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			resp, err := client.Query(test.query)
			if err != nil {
				t.Fatalf("running query: %v", err)
			}
			res := resp.Result()
			switch test.resType {
			case "count":
				if res.Count() != test.exp.(int64) {
					t.Fatalf("unexpected count %d is not %d", res.Count(), test.exp.(int64))
				}
			case "rowKeys":
				got := res.Row().Keys
				exp := test.exp.([]string)
				if err := isPermutationOf(got, exp); err != nil {
					t.Fatalf("unequal rows %v expected/got:\n%v\n%v", err, exp, got)
				}
			}
		})
	}

}

func writeFile(t testing.TB, name, contents string) {
	cf, err := os.Create(name)
	if err != nil {
		t.Fatalf("creating config file: %v", err)
	}
	_, err = cf.Write([]byte(contents))
	if err != nil {
		t.Fatalf("writing config file: %v", err)
	}
}

func isPermutationOf(one, two []string) error {
	if len(one) != len(two) {
		return errors.Errorf("different lengths %d and %d", len(one), len(two))
	}
outer:
	for _, vOne := range one {
		for j, vTwo := range two {
			if vOne == vTwo {
				two = append(two[:j], two[j+1:]...)
				continue outer
			}
		}
		return errors.Errorf("%s in one but not two", vOne)
	}
	if len(two) != 0 {
		return errors.Errorf("vals in two but not one: %v", two)
	}
	return nil
}
