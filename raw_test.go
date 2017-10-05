package pto3_test

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

var rds *pto3.RawDataStore
var r *mux.Router

func TestMain(m *testing.M) {

	// create temporary RDS directory
	rawroot, err := ioutil.TempDir("", "pto3_test")
	if err != nil {
		log.Fatal(err.Error())
	}
	defer os.RemoveAll(rawroot)

	// create configuration, RDS, and router (as package vars?)
	baseurl, _ := url.Parse("http://ptotest.corvid.ch")
	config := pto3.PTOServerConfig{
		BaseURL:      *baseurl,
		RawRoot:      rawroot,
		ContentTypes: map[string]string{"test": "application/json"},
	}

	authorizer := pto3.Authorizer{
		APIKeys: map[string]map[string]bool{
			"07e57ab18e70": map[string]bool{
				"list_raw":       true,
				"read_raw:test":  true,
				"write_raw:test": true,
			},
		},
	}

	rds, err = pto3.NewRawDataStore(&config, &authorizer)
	if err != nil {
		log.Fatal(err.Error())
	}

	r = mux.NewRouter()
	r.HandleFunc("/", config.HandleRoot)
	rds.AddRoutes(r)

	// go!
	os.Exit(m.Run())

}

func TestListRoot(t *testing.T) {
	req, err := http.NewRequest("GET", "http://ptotest.corvid.ch/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "application/json")

	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)

	var links map[string]string

	if res.Code != 200 {
		t.Fatalf("GET / got status %d", res.Code)
	}

	if res.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("GET / unexpected content type %s", res.Header().Get("Content-Type"))
	}

	if err = json.Unmarshal(res.Body.Bytes(), &links); err != nil {
		t.Fatal(err)
	}

	rawlink := links["raw"]
	if rawlink != "http://ptotest.corvid.ch/raw" {
		t.Fatalf("raw link is %s", rawlink)
	}
}

func TestListCampaigns(t *testing.T) {

	// list campaigns, this should be empty
	req, err := http.NewRequest("GET", "http://ptotest.corvid.ch/raw", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "APIKEY 07e57ab18e70")
	req.Header.Set("Accept", "application/json")

	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)

	var camlist map[string]interface{}

	if res.Code != 200 {
		t.Fatalf("GET /raw got status %d", res.Code)
	}

	if res.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected content type %s", res.Header().Get("Content-Type"))
	}

	if err = json.Unmarshal(res.Body.Bytes(), &camlist); err != nil {
		t.Fatal(err)
	}

	if _, ok := camlist["campaigns"]; !ok {
		t.Fatal("GET /raw missing campaign key")
	}
}

func TestBadAuth(t *testing.T) {

	// list campaigns with an unknown api key
	req, err := http.NewRequest("GET", "http://ptotest.corvid.ch/raw", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "APIKEY deadbeef")
	req.Header.Set("Accept", "application/json")

	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)

	if res.Code != 403 {
		t.Fatalf("GET /raw with bad APIKEY got status %d", res.Code)
	}
}
