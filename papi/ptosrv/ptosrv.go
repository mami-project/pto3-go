// Path Transparency Observatory Server

package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
	"github.com/mami-project/pto3-go/papi"
)

var configPath = flag.String("config", "ptoconfig.json", "Path to PTO `config file`")
var initdb = flag.Bool("initdb", false, "Create database tables on startup")
var help = flag.Bool("help", false, "show usage message")

func main() {
	flag.Parse()

	if *help {
		flag.PrintDefaults()
		return
	}

	// load configuration file
	config, err := pto3.NewConfigFromFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("ptosrv starting with configuration at %s...", *configPath)

	// create an API key authorizer
	azr, err := papi.LoadAPIKeys(config.APIKeyFile)
	if err != nil {
		log.Fatal(err)
	}

	// now hook up routes
	r := mux.NewRouter()

	papi.NewRootAPI(config, azr, r)

	rawapi, err := papi.NewRawAPI(config, azr, r)
	if err != nil {
		log.Fatal(err)
	}
	if rawapi != nil {
		log.Printf("...will serve /raw from %s", config.RawRoot)
	}

	obsapi := papi.NewObsAPI(config, azr, r)
	if obsapi != nil {
		log.Printf("...will serve /obs from postgresql://%s@%s/%s",
			config.ObsDatabase.User, config.ObsDatabase.Addr, config.ObsDatabase.Database)
		if *initdb {
			if err := obsapi.CreateTables(); err != nil {
				log.Fatal(err)
			}
			log.Printf("...created observation tables")
		}
	}

	bindto := config.BindTo

	// if certificate and key are present, listen and serve over TLS.
	// otherwise, go insecure.

	if config.CertificateFile != "" && config.PrivateKeyFile != "" {
		if bindto == "" {
			bindto = ":443"
		}
		log.Printf("...listening on %s", bindto)
		log.Fatal(http.ListenAndServeTLS(bindto, config.CertificateFile, config.PrivateKeyFile, r))
	} else {
		if bindto == "" {
			bindto = ":80"
		}
		log.Printf("...listening INSECURELY on %s", bindto)
		log.Fatal(http.ListenAndServe(bindto, r))
	}
}
