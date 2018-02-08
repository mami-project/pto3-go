// ptoload loads an observation file in the form produced by normalizers and
// derived analyzers into the database.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/go-pg/pg"
	pto3 "github.com/mami-project/pto3-go"
)

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "ptoconfig.json", "path to PTO configuration `file` with DB connection information")
var initdbFlag = flag.Bool("initdb", false, "Create database tables on startup")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: load observations from a file into a PTO database\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s <flags> input-files\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if *helpFlag {
		flag.Usage()
		os.Exit(1)
	}

	args := flag.Args()

	if len(args) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	config, err := pto3.NewConfigFromFile(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	db := pg.Connect(&config.ObsDatabase)
	if *initdbFlag {
		if err := pto3.CreateTables(db); err != nil {
			log.Fatal(err)
		}
	}

	// share pid and condition caches across all files
	cidCache, err := pto3.LoadConditionCache(db)
	if err != nil {
		log.Fatal(err)
	}

	pidCache := make(pto3.PathCache)

	for _, filename := range args {
		var set *pto3.ObservationSet
		set, err = pto3.CopySetFromObsFile(filename, db, cidCache, pidCache)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("created observation set %x:", set.ID)
		b, _ := json.MarshalIndent(set, "  ", "  ")
		os.Stderr.Write(b)
		log.Println("")
	}
}
