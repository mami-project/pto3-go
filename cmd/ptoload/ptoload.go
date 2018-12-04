// ptoload loads an observation file in the form produced by normalizers and
// derived analyzers into the database.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/go-pg/pg"
	pto3 "github.com/mami-project/pto3-go"
)

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "", "path to PTO configuration `file` with DB connection information")
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

	config, err := pto3.NewConfigWithDefault(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	db := pg.Connect(&config.ObsDatabase)
	if *initdbFlag {
		if err := pto3.CreateTables(db); err != nil {
			log.Fatal("creating database tables: ", err)
		}
	}

	// share pid and condition caches across all files
	cidCache, err := pto3.LoadConditionCache(db)
	if err != nil {
		log.Fatal("loading condition cache: ", err)
	}

	pidCache := make(pto3.PathCache)

	for i, filename := range args {
		var set *pto3.ObservationSet
		set, err = pto3.CopySetFromObsFile(filename, db, cidCache, pidCache)
		if err != nil {
			log.Fatal("copying set from obs file: ", err)
		}

		set.LinkVia(config)

		log.Printf("%d/%d (%5.2f%%) done, created observation set 0x%x",
			i+1, len(args), 100.0*float64(i+1)/float64(len(args)), set.ID)
		/* Previous debugging output:
		 * b, _ := json.MarshalIndent(set, "  ", "  ")
		 * os.Stderr.Write(b)
		 * log.Println("")
		 */
	}
}
