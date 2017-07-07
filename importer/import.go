package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	pilosa "github.com/pilosa/go-pilosa"
)

func main() {
	client := pilosa.DefaultClient()
	index, _ := pilosa.NewIndex("i", nil)
	frame, _ := index.Frame("f", nil)
	file, err := os.Open("/home/yuce/ramdisk/testdata/index-frame1_100x1050000.csv")
	if err != nil {
		panic(err)
	}
	err = client.EnsureIndex(index)
	if err != nil {
		panic(err)
	}
	err = client.EnsureFrame(frame)
	if err != nil {
		panic(err)
	}
	iterator := pilosa.NewCSVBitIterator(bufio.NewReader(file))
	tic := time.Now()
	err = client.ImportFrame(frame, iterator, 1000000)
	if err != nil {
		panic(err)
	}
	tac := time.Since(tic)
	fmt.Println("TAC:", tac)
}
