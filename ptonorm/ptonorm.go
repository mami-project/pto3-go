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

func PtoNorm(config *pto3.PTOServerConfig, normalizerCommand string, campaign string, filename string) error {

	// create a raw data store (no need for an authorizer)
	rds, err := pto3.NewRawDataStore(config, &pto3.NullAuthorizer{})
	if err != nil {
		return err
	}

	// retrieve the campaign
	cam, err := rds.CampaignForName(campaign)
	if err != nil {
		return err
	}

	// get metadata for the file
	md, err := cam.GetFileMetadata(filename)
	if err != nil {
		return err
	}

	// open raw data file
	// FIXME need an method on cam to do this

	// create pipes and start subprocess

	// start a goroutine to fill the metadata pipe

	// start a goroutine to fill the data pipe

	// wait for everything to finish

	return nil
}

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "ptoconfig.json", "path to PTO configuration `file` with raw data store information")

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

	config, err := pto3.NewConfigFromFile(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	args := flag.Args()

	if len(args) < 3 {
		flag.Usage()
		os.Exit(1)
	}

	if err := PtoNorm(config, args[0], args[1], args[2]); err != nil {
		log.Fatal(err)
	}

}
