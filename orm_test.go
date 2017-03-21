package pilosa

import (
	"testing"
)

var sampleDb = mustNewDatabase("sample-db", "")
var sampleFrame = mustNewFrame(sampleDb, "sample-frame", "")
var projectDb = mustNewDatabase("project-db", "user")
var collabFrame = mustNewFrame(projectDb, "collaboration", "project")
var b1 = sampleFrame.Bitmap(10)
var b2 = sampleFrame.Bitmap(20)
var b3 = sampleFrame.Bitmap(42)
var b4 = collabFrame.Bitmap(2)

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
}

func TestCount(t *testing.T) {
	q := projectDb.Count(collabFrame.Bitmap(42))
	comparePql(t, q, "Count(Bitmap(project=42, frame='collaboration'))")
}

func comparePql(t *testing.T, q IPqlQuery, target string) {
	pql := q.ToString()
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
