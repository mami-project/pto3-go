// Path Transparency Observatory Server

package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

func main() {
	// load configuration file
	config, err := pto3.LoadConfig("ptoconfig.json")
	if err != nil {
		log.Fatal(err.Error())
	}

	// now hook up routes
	r := mux.NewRouter()
	r.HandleFunc("/", config.HandleRoot)

	// create a RawDataStore around the RDS path if given
	if config.RawRoot != "" {
		rds, err := pto3.NewRawDataStore(config)
		if err != nil {
			log.Fatal(err.Error())
		}
		rds.AddRoutes(r)
	}

	log.Fatal(http.ListenAndServe(":8000", r))
}
