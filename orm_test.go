package pilosa

import (
	"testing"
	"time"
)

var sampleDb = mustNewDatabase("sample-db", "")
var sampleFrame = mustNewFrame(sampleDb, "sample-frame", "")
var projectDb = mustNewDatabase("project-db", "user")
var collabFrame = mustNewFrame(projectDb, "collaboration", "project")
var b1 = sampleFrame.Bitmap(10)
var b2 = sampleFrame.Bitmap(20)
var b3 = sampleFrame.Bitmap(42)
var b4 = collabFrame.Bitmap(2)

func TestNewDatabase(t *testing.T) {
	db, err := NewDatabase("db-name")
	if err != nil {
		t.Fatal(err)
	}
	if db.Name() != "db-name" {
		t.Fatalf("database name was not set")
	}
}

func TestNewDatabaseWithInvalidColumnLabel(t *testing.T) {
	_, err := NewDatabaseWithColumnLabel("foo", "$$INVALID$$")
	if err == nil {
		t.Fatal()
	}
}

func TestNewDatabaseWithInvalidName(t *testing.T) {
	_, err := NewDatabase("$FOO")
	if err == nil {
		t.Fatal()
	}
}

func TestNewFrameWithInvalidName(t *testing.T) {
	db, err := NewDatabase("foo")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.FrameWithRowLabel("$$INVALIDFRAME$$", "label")
	if err == nil {
		t.Fatal(err)
	}
}

func TestBitmap(t *testing.T) {
	comparePQL(t,
		"Bitmap(id=5, frame='sample-frame')",
		sampleFrame.Bitmap(5))
	comparePQL(t,
		"Bitmap(project=10, frame='collaboration')",
		collabFrame.Bitmap(10))
}

func TestSetBit(t *testing.T) {
	comparePQL(t,
		"SetBit(id=5, frame='sample-frame', profileID=10)",
		sampleFrame.SetBit(5, 10))
	comparePQL(t,
		"SetBit(project=10, frame='collaboration', user=20)",
		collabFrame.SetBit(10, 20))
}

func TestClearBit(t *testing.T) {
	comparePQL(t,
		"ClearBit(id=5, frame='sample-frame', profileID=10)",
		sampleFrame.ClearBit(5, 10))
	comparePQL(t,
		"ClearBit(project=10, frame='collaboration', user=20)",
		collabFrame.ClearBit(10, 20))
}

func TestUnion(t *testing.T) {
	comparePQL(t,
		"Union(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))",
		sampleDb.Union(b1, b2))
	comparePQL(t,
		"Union(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))",
		sampleDb.Union(b1, b2, b3))
	comparePQL(t,
		"Union(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
		sampleDb.Union(b1, b4))
}

func TestIntersect(t *testing.T) {
	comparePQL(t,
		"Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))",
		sampleDb.Intersect(b1, b2))
	comparePQL(t,
		"Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))",
		sampleDb.Intersect(b1, b2, b3))
	comparePQL(t,
		"Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
		sampleDb.Intersect(b1, b4))
}

func TestDifference(t *testing.T) {
	comparePQL(t,
		"Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))",
		sampleDb.Difference(b1, b2))
	comparePQL(t,
		"Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))",
		sampleDb.Difference(b1, b2, b3))
	comparePQL(t,
		"Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))",
		sampleDb.Difference(b1, b4))
}

func TestTopN(t *testing.T) {
	comparePQL(t,
		"TopN(frame='sample-frame', n=27)",
		sampleFrame.TopN(27))
	comparePQL(t,
		"TopN(Bitmap(project=3, frame='collaboration'), frame='sample-frame', n=10)",
		sampleFrame.BitmapTopN(10, collabFrame.Bitmap(3)))
	comparePQL(t,
		"TopN(Bitmap(project=7, frame='collaboration'), frame='sample-frame', n=12, field='category', [80,81])",
		sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, 81))
}

func TestFilterFieldTopNInvalidField(t *testing.T) {
	q := sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "$invalid$", 80, 81)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestFilterFieldTopNInvalidValue(t *testing.T) {
	q := sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, func() {})
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestBitmapOperationInvalidArg(t *testing.T) {
	invalid := sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "$invalid$", 80, 81)
	// invalid argument in pos 1
	q := sampleDb.Union(invalid, b1)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// invalid argument in pos 2
	q = sampleDb.Intersect(b1, invalid)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
	// invalid argument in pos 3
	q = sampleDb.Intersect(b1, b2, invalid)
	if q.Error() == nil {
		t.Fatalf("should have failed")
	}
}

func TestSetProfileAttrsTest(t *testing.T) {
	attrs := map[string]interface{}{
		"quote": "\"Don't worry, be happy\"",
		"happy": true,
	}
	comparePQL(t,
		"SetProfileAttrs(user=5, happy=true, quote=\"\\\"Don't worry, be happy\\\"\")",
		projectDb.SetProfileAttrs(5, attrs))
}

func TestSetProfileAttrsInvalidAttr(t *testing.T) {
	attrs := map[string]interface{}{
		"color":     "blue",
		"$invalid$": true,
	}
	if projectDb.SetProfileAttrs(5, attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}
}

func TestSetBitmapAttrsTest(t *testing.T) {
	attrs := map[string]interface{}{
		"quote":  "\"Don't worry, be happy\"",
		"active": true,
	}

	comparePQL(t,
		"SetBitmapAttrs(project=5, frame='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")",
		collabFrame.SetBitmapAttrs(5, attrs))
}

func TestSetBitmapAttrsInvalidAttr(t *testing.T) {
	attrs := map[string]interface{}{
		"color":     "blue",
		"$invalid$": true,
	}
	if collabFrame.SetBitmapAttrs(5, attrs).Error() == nil {
		t.Fatalf("Should have failed")
	}
}

func TestCount(t *testing.T) {
	q := projectDb.Count(collabFrame.Bitmap(42))
	comparePQL(t, "Count(Bitmap(project=42, frame='collaboration'))", q)
}

func TestRange(t *testing.T) {
	start := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2000, time.February, 2, 3, 4, 0, 0, time.UTC)
	comparePQL(t,
		"Range(project=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')",
		collabFrame.Range(10, start, end))
}

func comparePQL(t *testing.T, target string, q PQLQuery) {
	pql := q.String()
	if pql != target {
		t.Fatalf("%s != %s", pql, target)
	}
}

func mustNewDatabase(name string, columnLabel string) (db *Database) {
	var err error
	if columnLabel != "" {
		db, err = NewDatabaseWithColumnLabel(name, columnLabel)
	} else {
		db, err = NewDatabase(name)
	}
	if err != nil {
		panic(err)
	}
	return
}

func mustNewFrame(db *Database, name string, rowLabel string) (frame *Frame) {
	var err error
	if rowLabel != "" {
		frame, err = db.FrameWithRowLabel(name, rowLabel)
	} else {
		frame, err = db.Frame(name)
	}
	if err != nil {
		panic(err)
	}
	return
}
