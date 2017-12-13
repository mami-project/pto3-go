package papi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
	"github.com/mami-project/pto3-go/papi"
)

// set to true to allow inspection of tables after testing
const SuppressDropTables = false

func setupRaw(config *pto3.PTOConfiguration, azr papi.Authorizer, r *mux.Router) *papi.RawAPI {
	// create temporary RDS directory
	var err error
	config.RawRoot, err = ioutil.TempDir("", "pto3-test-raw")
	if err != nil {
		log.Fatal(err)
	}

	// create an RDS
	obsapi, err := papi.NewRawAPI(config, azr, r)
	if err != nil {
		log.Fatal(err)
	}

	return obsapi
}

func teardownRaw(config *pto3.PTOConfiguration) {
	if err := os.RemoveAll(config.RawRoot); err != nil {
		log.Fatal(err)
	}
}

func setupObs(config *pto3.PTOConfiguration, azr papi.Authorizer, r *mux.Router) *papi.ObsAPI {
	// create an observation API
	obsapi := papi.NewObsAPI(config, azr, r)

	// log everything
	obsapi.EnableQueryLogging()

	// create tables
	if err := obsapi.CreateTables(); err != nil {
		log.Fatal(err)
	}

	return obsapi
}

func teardownObs(obsapi *papi.ObsAPI) {
	// (don't) delete tables
	if !SuppressDropTables && TestRC == 0 {
		if err := obsapi.DropTables(); err != nil {
			log.Fatal(err)
		}
	}
}

const GoodAPIKey = "07e57ab18e70"

func setupAZR() papi.Authorizer {
	return &papi.APIKeyAuthorizer{
		APIKeys: map[string]map[string]bool{
			GoodAPIKey: map[string]bool{
				"list_raw":       true,
				"read_raw:test":  true,
				"write_raw:test": true,
				"read_obs":       true,
				"write_obs":      true,
				// "submit_query":   true,
				// "read_query":     true,
				// "update_query":   true,
			},
		},
	}
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

func executeWithJSON(r *mux.Router, t *testing.T,
	method string, url string,
	content interface{},
	apikey string, expectstatus int) *httptest.ResponseRecorder {

	b, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}

	return executeRequest(r, t, method, url, bytes.NewBuffer(b), "application/json", apikey, expectstatus)
}

func executeWithFile(r *mux.Router, t *testing.T,
	method string, url string,
	filepath string, bodytype string,
	apikey string, expectstatus int) *httptest.ResponseRecorder {

	f, err := os.Open(filepath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	return executeRequest(r, t, method, url, f, bodytype, apikey, http.StatusCreated)
}

const TestBaseURL = "https://ptotest.mami-project.eu"

var TestConfig *pto3.PTOConfiguration
var TestRouter *mux.Router

var TestRC int

func TestMain(m *testing.M) {
	// define a configuration
	testConfigJSON := []byte(`
{ 	
	"BaseURL" : "https://ptotest.mami-project.eu",
	"ContentTypes" : {
		"test" : "application/json",
		"osf" :  "applicaton/vnd.mami.ndjson"
	},
	"ObsDatabase" : {
		"Addr":     "localhost:5432",
		"User":     "ptotest",
		"Database": "ptotest",
		"Password": "helpful guide sheep train"
	}
}`)

	var err error
	TestConfig, err = pto3.NewConfigFromJSON(testConfigJSON)
	if err != nil {
		log.Fatal(err)
	}

	// create a router
	TestRouter = mux.NewRouter()

	// inner anon function ensures that os.Exit doesn't keep deferred teardown from running
	os.Exit(func() int {
		// get an authorizer
		azr := setupAZR()

		papi.NewRootAPI(TestConfig, azr, TestRouter)

		// build a raw data store  (and prepare to clean up after it)
		setupRaw(TestConfig, azr, TestRouter)
		defer teardownRaw(TestConfig)

		// build an observation store (and prepare to clean up after it)
		obsapi := setupObs(TestConfig, azr, TestRouter)
		defer teardownObs(obsapi)

		TestRC = m.Run()
		return TestRC
	}())
}

func TestListRoot(t *testing.T) {
	res := executeRequest(TestRouter, t, "GET", TestBaseURL+"/", nil, "", "", http.StatusOK)

	checkContentType(t, res)

	var links map[string]string

	if err := json.Unmarshal(res.Body.Bytes(), &links); err != nil {
		t.Fatal(err)
	}

	rawlink := links["raw"]
	if rawlink != TestBaseURL+"/raw" {
		t.Fatalf("raw link is %s", rawlink)
	}
}
