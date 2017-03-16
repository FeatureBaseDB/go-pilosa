package pilosa

import (
	"fmt"
	"testing"
)

func TestError(t *testing.T) {
	err := NewPilosaError("some error")
	if err.Error() != "PilosaError: some error" {
		fmt.Println(err.Error())
		t.Fatal()
	}
}
