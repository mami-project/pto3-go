package pto3_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/go-pg/pg"
	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

func setupRDS(config *pto3.PTOServerConfig, azr *pto3.Authorizer) *pto3.RawDataStore {
	// create temporary RDS directory
	var err error
	config.RawRoot, err = ioutil.TempDir("", "pto3-test")
	if err != nil {
		log.Fatal(err.Error())
	}

	// create an RDS
	rds, err := pto3.NewRawDataStore(config, azr)
	if err != nil {
		log.Fatal(err.Error())
	}

	return rds
}

func teardownRDS(rds *pto3.RawDataStore) {
	if err := rds.RemoveDirectories(); err != nil {
		log.Fatal(err.Error())
	}
}

func setupOSR(config *pto3.PTOServerConfig, azr *pto3.Authorizer) *pto3.ObservationStore {
	// create an RDS
	osr, err := pto3.NewObservationStore(config, azr)
	if err != nil {
		log.Fatal(err.Error())
	}

	// create tables
	if err := osr.CreateTables(); err != nil {
		log.Fatal(err.Error())
	}

	return osr
}

func teardownOSR(osr *pto3.ObservationStore) {
	// create tables
	err := osr.DropTables()
	if err != nil {
		log.Fatal(err.Error())
	}
}

const GoodAPIKey = "07e57ab18e70"

func setupAZR() *pto3.Authorizer {
	return &pto3.Authorizer{
		APIKeys: map[string]map[string]bool{
			GoodAPIKey: map[string]bool{
				"list_raw":       true,
				"read_raw:test":  true,
				"write_raw:test": true,
				"read_obs":       true,
				"write_obs":      true,
			},
		},
	}
}

var TestConfig pto3.PTOServerConfig

func TestMain(m *testing.M) {
	// define a configuration
	baseurl, _ := url.Parse("http://ptotest.mami-project.eu")
	TestConfig = pto3.PTOServerConfig{
		BaseURL: *baseurl,
		ContentTypes: map[string]string{
			"test": "application/json",
			"osf":  "applicaton/vnd.mami.ndjson",
		},
		ObsDatabase: pg.Options{
			Addr:     "localhost:5432",
			User:     "ptotest",
			Database: "ptotest",
			Password: "helpful guide sheep train",
		},
	}

	// inner anon function ensures that os.Exit doesn't keep deferred teardown from running
	os.Exit(func() int {
		// get an authorizer
		azr := setupAZR()

		// build a raw data store around it (and prepare to clean up after it)
		rds := setupRDS(&TestConfig, azr)
		defer teardownRDS(rds)

		// build an observation store around it (and prepare to clean up after it)
		osr := setupOSR(&TestConfig, azr)
		defer teardownOSR(osr)

		// debugging: log every query
		osr.EnableQueryLogging()

		// set up routes
		r = mux.NewRouter()
		r.HandleFunc("/", TestConfig.HandleRoot)
		rds.AddRoutes(r)
		osr.AddRoutes(r)

		return m.Run()
	}())
}

func executeRequest(r *mux.Router, t *testing.T, method string, url string, body io.Reader, bodytype string, apikey string, expectstatus int) *httptest.ResponseRecorder {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")

	if bodytype != "" {
		req.Header.Set("Content-Type", bodytype)
	}

	if apikey != "" {
		req.Header.Set("Authorization", "APIKEY "+apikey)
	}

	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)

	if res.Code != expectstatus {
		errstr := fmt.Sprintf("%s %s expected status %d but got %d", method, url, expectstatus, res.Code)
		if res.Code >= 400 {
			errstr += ":\n" + string(res.Body.Bytes())
		}
		t.Fatal(errstr)
	}

	return res
}

func executePutJSON(r *mux.Router, t *testing.T, url string, content interface{}, bodytype string, apikey string) *httptest.ResponseRecorder {
	b, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}

	return executeRequest(r, t, "PUT", url, bytes.NewBuffer(b), bodytype, apikey, http.StatusCreated)
}

func executePutFile(r *mux.Router, t *testing.T, url string, filepath string, bodytype string, apikey string) *httptest.ResponseRecorder {
	f, err := os.Open(filepath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	return executeRequest(r, t, "PUT", url, f, bodytype, apikey, http.StatusCreated)
}
