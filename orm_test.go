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
	comparePql(t,
		sampleFrame.Bitmap(5),
		"Bitmap(id=5, frame='sample-frame')")
	comparePql(t,
		collabFrame.Bitmap(10),
		"Bitmap(project=10, frame='collaboration')")
}

func TestSetBit(t *testing.T) {
	comparePql(t,
		sampleFrame.SetBit(5, 10),
		"SetBit(id=5, frame='sample-frame', profileID=10)")
	comparePql(t,
		collabFrame.SetBit(10, 20),
		"SetBit(project=10, frame='collaboration', user=20)")
}

func TestClearBit(t *testing.T) {
	comparePql(t,
		sampleFrame.ClearBit(5, 10),
		"ClearBit(id=5, frame='sample-frame', profileID=10)")
	comparePql(t,
		collabFrame.ClearBit(10, 20),
		"ClearBit(project=10, frame='collaboration', user=20)")
}

func TestUnion(t *testing.T) {
	comparePql(t,
		sampleDb.Union(b1, b2),
		"Union(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))")
	comparePql(t,
		sampleDb.Union(b1, b2, b3),
		"Union(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))")
	comparePql(t,
		sampleDb.Union(b1, b4),
		"Union(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))")
}

func TestIntersect(t *testing.T) {
	comparePql(t,
		sampleDb.Intersect(b1, b2),
		"Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))")
	comparePql(t,
		sampleDb.Intersect(b1, b2, b3),
		"Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))")
	comparePql(t,
		sampleDb.Intersect(b1, b4),
		"Intersect(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))")
}

func TestDifference(t *testing.T) {
	comparePql(t,
		sampleDb.Difference(b1, b2),
		"Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'))")
	comparePql(t,
		sampleDb.Difference(b1, b2, b3),
		"Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(id=20, frame='sample-frame'), Bitmap(id=42, frame='sample-frame'))")
	comparePql(t,
		sampleDb.Difference(b1, b4),
		"Difference(Bitmap(id=10, frame='sample-frame'), Bitmap(project=2, frame='collaboration'))")
}

func TestTopN(t *testing.T) {
	comparePql(t,
		sampleFrame.TopN(27),
		"TopN(frame='sample-frame', n=27)")
	comparePql(t,
		sampleFrame.BitmapTopN(10, collabFrame.Bitmap(3)),
		"TopN(Bitmap(project=3, frame='collaboration'), frame='sample-frame', n=10)")
	comparePql(t,
		sampleFrame.FilterFieldTopN(12, collabFrame.Bitmap(7), "category", 80, 81),
		"TopN(Bitmap(project=7, frame='collaboration'), frame='sample-frame', n=12, field='category', [80,81])")
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
	comparePql(t,
		projectDb.SetProfileAttrs(5, attrs),
		"SetProfileAttrs(user=5, happy=true, quote=\"\\\"Don't worry, be happy\\\"\")")
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

	comparePql(t,
		collabFrame.SetBitmapAttrs(5, attrs),
		"SetBitmapAttrs(project=5, frame='collaboration', active=true, quote=\"\\\"Don't worry, be happy\\\"\")")
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
	comparePql(t, q, "Count(Bitmap(project=42, frame='collaboration'))")
}

func TestRange(t *testing.T) {
	start := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2000, time.February, 2, 3, 4, 0, 0, time.UTC)
	comparePql(t,
		collabFrame.Range(10, start, end),
		"Range(project=10, frame='collaboration', start='1970-01-01T00:00', end='2000-02-02T03:04')")
}

func comparePql(t *testing.T, q PQLQuery, target string) {
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
