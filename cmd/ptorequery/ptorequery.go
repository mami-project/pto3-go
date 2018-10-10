// ptorequery is a command-line utility to run a query based on
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	pto3 "github.com/mami-project/pto3-go"
)

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "", "path to PTO configuration file with DB connection information")
var forceFlag = flag.Bool("force", false, "rerun query regardless of query state")

func main() {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: update or rebuild a PTO query cache\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s <flags> querylist.json ...\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	args := flag.Args()

	if *helpFlag || len(args) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	config, err := pto3.NewConfigWithDefault(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	qc, err := pto3.NewQueryCache(config)
	if err != nil {
		log.Fatal(err)
	}

	donechans := make([]chan struct{}, 0)

	for _, filename := range args {
		r, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}

		in := bufio.NewScanner(r)
		for in.Scan() {
			encoded := in.Text()

			oq, err := qc.ParseQueryFromURLEncoded(encoded)
			if err != nil {
				log.Printf("error parsing query %s: %v", encoded)
			}

			var doPurge bool
			if *forceFlag {
				doPurge = true
			}

			if doPurge {
				if err := oq.Purge(); err != nil && !os.IsNotExist(err) {
					log.Printf("error purging query %s: %v", encoded)
				}
			}

			donechan := make(chan struct{})
			donechans = append(donechans, donechan)
			nq, isNew, err := qc.ExecuteQueryFromURLEncoded(encoded, donechan)

			if isNew {
				log.Printf("executing query %s with ID %s", nq.URLEncoded(), nq.Identifier)
			}
		}
	}

	log.Printf("waiting for queries to finish executing...")

}
