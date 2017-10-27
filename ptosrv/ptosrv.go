// Path Transparency Observatory Server

package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

var configPath = flag.String("config", "ptoconfig.json", "Path to PTO configuration file")

func main() {
	flag.Parse()

	config, err := pto3.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err.Error())
	}

	// create an API key authorizer
	azr, err := pto3.LoadAPIKeys(config.APIKeyFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	// now hook up routes
	r := mux.NewRouter()
	r.HandleFunc("/", config.HandleRoot)

	// create a RawDataStore around the RDS path if given
	if config.RawRoot != "" {
		rds, err := pto3.NewRawDataStore(config, azr)
		if err != nil {
			log.Fatal(err.Error())
		}
		rds.AddRoutes(r)
	}

	if config.ObsDatabase.Database != "" {
		osr, err := pto3.NewObservationStore(config, azr)
		if err != nil {
			log.Fatal(err.Error())
		}
		osr.AddRoutes(r)
	}

	log.Fatal(http.ListenAndServe(config.BindTo, r))
}
