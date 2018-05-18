// ptocat is a command-line utility to retrieve observations from one or more
// observation sets and write them to standard output in the appropriate form
// for a derived analyzer.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	pto3 "github.com/mami-project/pto3-go"
)

func CatMetadata(db orm.DB, set *pto3.ObservationSet, out io.Writer) error {
	b, err := json.Marshal(&set)
	if err != nil {
		return err
	}

	os.Stdout.Write(b)
	fmt.Fprint(os.Stdout, "\n")
	return nil
}

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "ptoconfig.json", "path to PTO configuration file with DB connection information")

func main() {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: dump observations from a PTO database", os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s <flags> (Set ID)+\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Note that set IDs are given in hexadecimal")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *helpFlag {
		flag.Usage()
		os.Exit(1)
	}

	setIDs := make([]int, 0)
	for _, arg := range flag.Args() {
		idarg, err := strconv.ParseUint(arg, 16, 64)
		if err != nil {
			log.Printf("cannot parse Set ID %s", arg)
			flag.Usage()
			os.Exit(1)
		}
		setIDs = append(setIDs, int(idarg))
	}

	config, err := pto3.NewConfigFromFile(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	db := pg.Connect(&config.ObsDatabase)

	for _, setID := range setIDs {
		set := pto3.ObservationSet{ID: setID}
		if err := set.SelectByID(db); err != nil {
			log.Fatal(err)
		}

		if err := CatMetadata(db, &set, os.Stdout); err != nil {
			log.Fatal(err)
		}

		if err := set.CopyDataToStream(db, os.Stdout); err != nil {
			log.Fatal(err)
		}

	}

}
