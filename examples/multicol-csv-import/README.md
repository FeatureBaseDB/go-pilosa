# Pilosa Multi-field CSV Import

## Prerequisites

* Go 1.12 or better

## Install

Clone the Go client:

    $  git clone https://github.com/pilosa/go-pilosa.git

Edit `go-pilosa/examples/multicol-csv-import/main.go` so it matches the contents of the CSV file.

Open a terminal in the `go-pilosa` directory and build the importer:
    $ go build ./examples/multicol-csv-import

## Usage

* Run `multicol-csv-import` with the Pilosa address (by default: `localhost:10101`) and name of the CSV file:

    $ ./multicol-csv-import :10101 sample.csv
