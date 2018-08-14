package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/go-pg/pg"
	pto3 "github.com/mami-project/pto3-go"
)

type autonormConfig struct {
	Campaigns   []string
	Normalizers map[string]string
}

func newAutonormConfig(filename string) (*autonormConfig, error) {
	var out autonormConfig

	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	json.Unmarshal(b, &out)

	return &out, nil
}

var configFlag = flag.String("config", "", "path to PTO/autonorm configuration `file`")

func main() {

	// get PTO configuration
	pconfig, err := pto3.NewConfigWithDefault(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	// get autonormalizer configuration
	aconfig, err := newAutonormConfig(pconfig.ConfigFilePath)
	if err != nil {
		log.Fatal(err)
	}

	// create a raw data store (no need for an authorizer)
	rds, err := pto3.NewRawDataStore(pconfig)
	if rds == nil {
		log.Fatal("autonorm needs a raw data store to work")
	}

	if err != nil {
		log.Fatal(err)
	}

	// create a database connection
	db := pg.Connect(&pconfig.ObsDatabase)

	// share pid and condition caches across all files in a single autonorm run
	cidCache, err := pto3.LoadConditionCache(db)
	if err != nil {
		log.Fatal(err)
	}

	pidCache := make(pto3.PathCache)

	// for each campaign directory
	for _, camname := range aconfig.Campaigns {

		// retrieve campaign and metadata
		cam, err := rds.CampaignForName(camname)
		if err != nil {
			log.Fatal(err)
		}

		// iterate over files
		filenames, err := cam.FileNames()
		if err != nil {
			log.Fatal(err)
		}

		for _, filename := range filenames {

			// skip if deprecated
			filemd, err := cam.GetFileMetadata(filename)
			if err != nil {
				log.Fatal(err)
			}

			if filemd.Get("_deprecated", true) != "" {
				continue
			}

			// generate a link to this file for source
			filelink, err := pconfig.LinkTo(fmt.Sprintf("/raw/%s/%s", camname, filename))
			if err != nil {
				log.Fatal(err)
			}

			// find observation sets claiming this file as a source
			osids, err := pto3.ObservationSetIDsWithSource(db, filelink)
			if err != nil {
				log.Fatal(err)
			}

			if len(osids) == 0 {
				// we have a winner! get a filetype to find a normalizer
				filetype := cam.GetFiletype(filename)

				normalizer := aconfig.Normalizers[filetype.Filetype]

				if normalizer != "" {
					log.Fatalf("no configured normalizer for file %s in campaign %s with filetype %s", filename, camname, filetype.Filetype)
				}

				// and now we have a normalizer. create a temporary output file.
				obsfile, err := ioutil.TempFile("", "autonorm_obs")
				if err != nil {
					log.Fatal(err)
				}

				// run the normalizer into it
				if err := pto3.RunNormalizer(pconfig, obsfile, normalizer, camname, filename); err != nil {
					log.Fatal(err)
				}

				// load it
				set, err := pto3.CopySetFromObsFile(obsfile.Name(), db, cidCache, pidCache)
				if err != nil {
					log.Fatal(err)
				}

				set.LinkVia(pconfig)

				log.Printf("created observation set %x:", set.ID)
				b, _ := json.MarshalIndent(set, "  ", "  ")
				os.Stderr.Write(b)
				log.Println("")
			}
		}
	}
}
