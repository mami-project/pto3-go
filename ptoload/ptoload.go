// ptoload is a command-line utility to load an observation set file into a PTO observation database.

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/go-pg/pg"

	pto3 "github.com/mami-project/pto3-go"
)

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "ptoconfig.json", "path to PTO configuration `file` with DB connection information")
var initdbFlag = flag.Bool("initdb", false, "Create database tables on startup")

func extractMetadata(filename string, r io.Reader) (*pto3.ObservationSet, error) {
	// create an observation set to hold metadata
	set := pto3.ObservationSet{}

	// now scan the file for metadata
	var lineno = 0
	in := bufio.NewScanner(r)
	for in.Scan() {
		lineno++
		line := strings.TrimSpace(in.Text())
		if line[0] == '{' {
			if err := set.UnmarshalJSON([]byte(line)); err != nil {
				return nil, fmt.Errorf("error in metadata at %s line %d: %s", filename, lineno, err.Error())
			}
		}
	}

	// done
	return &set, nil
}

func loadObservations(filename string, db *pg.DB, set *pto3.ObservationSet, r io.Reader) error {

	var lineno = 0

	in := bufio.NewScanner(r)

	// the rest of this function is all or nothing
	err := db.RunInTransaction(func(t *pg.Tx) error {

		// first insert the set
		if err := set.Insert(t, true); err != nil {
			return err
		}

		// now scan standard input for observations
		for in.Scan() {
			lineno++
			line := strings.TrimSpace(in.Text())
			if line[0] == '[' {
				// create an observation object
				var obs pto3.Observation
				if err := obs.UnmarshalJSON([]byte(line)); err != nil {
					return err
				}
				// and nsert it in the set
				if err := obs.InsertInSet(t, set); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error in observation at %s line %d: %s", filename, lineno, err)
	}

	return nil
}

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

	for _, filename := range args {
		func() {
			obsfile, err := os.Open(filename)
			if err != nil {
				log.Fatal(err)
			}
			defer obsfile.Close()

			set, err := extractMetadata(filename, obsfile)
			if err != nil {
				log.Fatal(err)
			}

			if _, err := obsfile.Seek(0, 0); err != nil {
				log.Fatal(err)
			}

			if err := loadObservations(filename, db, set, obsfile); err != nil {
				log.Fatal(err)
			}

			log.Printf("created observation set %x:", set.ID)
			b, _ := json.MarshalIndent(set, "  ", "  ")
			os.Stderr.Write(b)
		}()
	}

}
