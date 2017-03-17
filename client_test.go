package pilosa

import "testing"

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
