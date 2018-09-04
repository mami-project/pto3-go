// ptonorm is a command-line utility to run a specified normalizer with a
// specified raw data file, passing the observation data and metadata produced
// by the normalizer to standard output.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	pto3 "github.com/mami-project/pto3-go"
)

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "", "path to PTO configuration `file`")
var outFlag = flag.String("out", "", "path to output `file` [stdout if omitted]")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: run a normalizer over a given raw data file\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s <flags> normalizer-command campaign filename\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if *helpFlag {
		flag.Usage()
		os.Exit(1)
	}

	config, err := pto3.NewConfigWithDefault(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	var outfile *os.File
	if *outFlag == "" {
		outfile = os.Stdout
	} else {
		outfile, err = os.Create(*outFlag)
		if err != nil {
			log.Fatal(err)
		}
		defer outfile.Close()
	}

	args := flag.Args()

	if len(args) < 3 {
		flag.Usage()
		os.Exit(1)
	}

	if err = pto3.RunNormalizer(config, outfile, args[0], args[1], args[2]); err != nil {
		log.Fatal(err)
	}

}
