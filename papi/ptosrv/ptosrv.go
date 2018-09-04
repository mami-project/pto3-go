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

var configPath = flag.String("config", "", "Path to PTO `config file`")
var initdb = flag.Bool("initdb", false, "Create database tables on startup")
var help = flag.Bool("help", false, "show usage message")

func main() {
	flag.Parse()

	if *help {
		flag.PrintDefaults()
		return
	}

	// load configuration file
	config, err := pto3.NewConfigWithDefault(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("ptosrv starting with configuration at %s...", *configPath)

	// initialize database and exit if -initdb given
	if *initdb {
		azr := &papi.NullAuthorizer{}
		r := mux.NewRouter()
		obsapi := papi.NewObsAPI(config, azr, r)
		if obsapi == nil {
			log.Fatalf("-initdb given but no observation API configuration available in %s", *configPath)
		}

		if err := obsapi.CreateTables(); err != nil {
			log.Fatal(err)
		}

		log.Printf("Observation database initialized; exiting...")
		return
	}

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
			log.Printf("...initialized observation database")
		}
	}

	qapi, err := papi.NewQueryAPI(config, azr, r)
	if err != nil {
		log.Fatal(err)
	}
	if qapi != nil {
		log.Printf("...will serve /query from cache at %s", config.QueryCacheRoot)
	}

	bindto := config.BindTo

	// tell CORS to go away, and that API keys are OK
	// c := cors.New(cors.Options{
	// 	AllowedMethods:   []string{"GET", "POST", "PUT", "OPTIONS"},
	// 	AllowCredentials: true,
	// })

	// if certificate and key are present, listen and serve over TLS.
	// otherwise, go insecure.

	if config.CertificateFile != "" && config.PrivateKeyFile != "" {
		if bindto == "" {
			bindto = ":443"
		}
		log.Printf("...listening on %s", bindto)
		log.Fatal(http.ListenAndServeTLS(bindto,
			config.CertificateFile, config.PrivateKeyFile, r))
	} else {
		if bindto == "" {
			bindto = ":80"
		}
		log.Printf("...listening INSECURELY on %s", bindto)
		log.Fatal(http.ListenAndServe(bindto, r))
	}
}
