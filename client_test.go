package pilosa

import "testing"

func TestQueryWithError(t *testing.T) {
	var err error
	client := DefaultClient()
	db, err := NewDatabase("foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := db.Frame("foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	invalid := frame.FilterFieldTopN(12, frame.Bitmap(7), "$invalid$", 80, 81)
	_, err = client.Query(invalid, nil)
	if err == nil {
		t.Fatalf("Should have failed")
	}
}
