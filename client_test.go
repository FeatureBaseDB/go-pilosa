package pilosa

import "testing"

func TestQueryWithQueryWithError(t *testing.T) {
	var err error
	client := NewClient()
	db, err := NewDatabase("foo")
	if err != nil {
		t.Fatal(err)
	}
	frame, err := db.Frame("foo")
	if err != nil {
		t.Fatal(err)
	}
	invalid := frame.FilterFieldTopN(12, frame.Bitmap(7), "$invalid$", 80, 81)
	_, err = client.Query(invalid)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}
