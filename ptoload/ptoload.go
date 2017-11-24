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

func PtoLoad(config *pto3.PTOServerConfig, db *pg.DB, r io.Reader) *pto3.ObservationSet {

	var lineno = 0
	var setid int
	var set pto3.ObservationSet

	in := bufio.NewScanner(r)

	// the rest of this function is all or nothing
	err := db.RunInTransaction(func(t *pg.Tx) error {

		// start by creating and inserting a dummy set to get a Set ID
		set = pto3.ObservationSet{}

		if err := set.Insert(t, true); err != nil {
			return err
		}

		// save the set ID since UnmarshalJSON will overwrite it
		setid = set.ID

		// now scan standard input
		for in.Scan() {
			lineno++
			line := strings.TrimSpace(in.Text())
			switch line[0] {
			case '{':
				// metadata object, update
				if err := set.UnmarshalJSON([]byte(line)); err != nil {
					return err
				}
				set.ID = setid
				if err := set.Update(t); err != nil {
					return err
				}
				log.Printf("updated metadata: %v", set)
			case '[':
				// observation object
				var obs pto3.Observation
				if err := obs.UnmarshalJSON([]byte(line)); err != nil {
					return err
				}
				if err := obs.InsertInSet(t, &set); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("error at input line %d: %v", lineno, err)
	}

	return &set
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: load observations into a PTO database\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s <flags>\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if *helpFlag {
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

	set := PtoLoad(config, db, os.Stdin)

	log.Printf("created observation set %x:", set.ID)
	b, _ := json.MarshalIndent(set, "  ", "  ")
	os.Stderr.Write(b)
}
