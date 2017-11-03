package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	pto3 "github.com/mami-project/pto3-go"
)

func CatMetadata(db orm.DB, setID int) error {
	set := pto3.ObservationSet{ID: setID}
	if err := set.SelectByID(db); err != nil {
		return err
	}

	b, err := json.Marshal(&set)
	if err != nil {
		return err
	}

	os.Stdout.Write(b)
	fmt.Fprint(os.Stdout, "\n")
	return nil
}

func CatObservations(db orm.DB, setID int) error {
	obsdata, err := pto3.ObservationsBySetID(db, setID)
	if err != nil {
		return err
	}

	return pto3.WriteObservations(obsdata, os.Stdout)
}

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "ptoconfig.json", "path to PTO configuration file with DB connection information")

func main() {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: dump observations from a PTO database")
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

		if err := CatMetadata(db, setID); err != nil {
			log.Fatal(err)
		}

		if err := CatObservations(db, setID); err != nil {
			log.Fatal(err)
		}

	}

}
