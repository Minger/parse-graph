package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
)

// Edge joins two nodes
type Edge struct {
	Source       uint64
	Destination  uint64
	Relationship string
}

// Node represents a package
type Node struct {
	VertexID uint64
	Name     string
}

// this func lifted from http://stackoverflow.com/questions/32027590/efficient-read-and-write-csv-in-go
func processCSV(rc io.Reader) (ch chan []string) {
	ch = make(chan []string, 10)
	go func() {
		r := csv.NewReader(rc)
		if _, err := r.Read(); err != nil { //read header
			log.Fatal(err)
		}
		defer close(ch)
		for {
			rec, err := r.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)

			}
			ch <- rec
		}
	}()
	return
}

func processRow(in <-chan []string) <-chan interface{} {
	out := make(chan interface{})
	go func() {
		for {
			row := <-in
			source := row[1]
			if len(source) > 0 {
				sourceHash := HashString(source)
				edge := make(Edge)
			}
		}
		close(out)
	}()
	return out
}

func main() {
	file, err := os.Open("requirements.csv")
	if err != nil {
		log.Fatal(err)
	}

	fileCh := processCSV(file)

	graphCh := make(chan interface{})

}
