package pilosa_test

import (
	"testing"

	"time"

	pilosa "github.com/pilosa/go-client-pilosa"
)

func TestQueryWithError(t *testing.T) {
	var err error
	client := pilosa.DefaultClient()
	db, err := pilosa.NewDatabase("foo", nil)
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

func TestDefaultClientOptions(t *testing.T) {
	options := pilosa.DefaultClientOptions()
	if options.SocketTimeout != time.Second*300 {
		t.Fatalf("ClientOptions default SocketTimeout %d", options.SocketTimeout)
	}
	if options.ConnectTimeout != time.Second*30 {
		t.Fatalf("ClientOptions default ConnectTimeout %d", options.ConnectTimeout)
	}
	if options.PoolSizePerRoute != 10 {
		t.Fatalf("ClientOptions default PoolSizePerRoute %d", options.PoolSizePerRoute)
	}
	if options.TotalPoolSize != 100 {
		t.Fatalf("ClientOptions default TotalPoolSize %d", options.TotalPoolSize)
	}
}
