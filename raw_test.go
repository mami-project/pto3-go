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

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

const GoodAPIKey = "07e57ab18e70"

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
	baseurl, _ := url.Parse("http://ptotest.mami-project.eu")
	config := pto3.PTOServerConfig{
		BaseURL:      *baseurl,
		RawRoot:      rawroot,
		ContentTypes: map[string]string{"test": "application/json"},
	}

	authorizer := pto3.Authorizer{
		APIKeys: map[string]map[string]bool{
			GoodAPIKey: map[string]bool{
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

func checkContentType(t *testing.T, res *httptest.ResponseRecorder) {
	if res.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected content type %s", res.Header().Get("Content-Type"))
	}
}
func TestListRoot(t *testing.T) {
	res := executeRequest(r, t, "GET", "http://ptotest.mami-project.eu/", nil, "", "", http.StatusOK)

	checkContentType(t, res)

	var links map[string]string

	if err := json.Unmarshal(res.Body.Bytes(), &links); err != nil {
		t.Fatal(err)
	}

	rawlink := links["raw"]
	if rawlink != "http://ptotest.mami-project.eu/raw" {
		t.Fatalf("raw link is %s", rawlink)
	}
}

func TestListCampaigns(t *testing.T) {

	// list campaigns, this should be empty
	res := executeRequest(r, t, "GET", "http://ptotest.mami-project.eu/raw", nil, "", GoodAPIKey, http.StatusOK)
	checkContentType(t, res)

	var camlist map[string]interface{}

	if err := json.Unmarshal(res.Body.Bytes(), &camlist); err != nil {
		t.Fatal(err)
	}

	if _, ok := camlist["campaigns"]; !ok {
		t.Fatal("GET /raw missing campaign key")
	}
}

func TestBadAuth(t *testing.T) {
	executeRequest(r, t, "GET", "http://ptotest.mami-project.eu/raw", nil, "", "abadc0de", http.StatusForbidden)
}

type testRawMetadata struct {
	FileType    string `json:"_file_type"`
	Owner       string `json:"_owner"`
	Description string `json:"description"`
	TimeStart   string `json:"_time_start"`
	TimeEnd     string `json:"_time_end"`
	DataSize    int    `json:"__data_size"`
	DataURL     string `json:"__data"`
}

func TestRoundTrip(t *testing.T) {

	// create a new campaign
	md := pto3.RDSMetadata{
		"_file_type":  "test",
		"_owner":      "ptotest@mami-project.eu",
		"description": "a campaign filled with uninteresting test data",
	}
	t.Log("attempting to create http://ptotest.mami-project.eu/raw/test")

	res := executePutJSON(r, t, "http://ptotest.mami-project.eu/raw/test", md, "application/json", GoodAPIKey)

	// create a file within the campaign
	md = pto3.RDSMetadata{
		"_time_start": "2010-01-01T00:00:00",
		"_time_end":   "2010-01-02T00:00:00",
	}
	t.Log("attempting to create http://ptotest.mami-project.eu/raw/test/file001.json")
	res = executePutJSON(r, t, "http://ptotest.mami-project.eu/raw/test/file001.json", md, "application/json", GoodAPIKey)

	// find the data link
	var trm testRawMetadata
	err := json.Unmarshal(res.Body.Bytes(), &trm)
	if err != nil {
		t.Fatal(err)
	}

	if trm.DataURL == "" {
		t.Fatal("missing __data virtual after metadata upload")
	}

	t.Logf("attempting to upload file to %s", trm.DataURL)

	// now upload the data
	data := []string{"this", "is", "a", "list", "of", "words"}
	res = executePutJSON(r, t, trm.DataURL, data, "application/json", GoodAPIKey)

	// retrieve file metadata and check file size
	res = executeRequest(r, t, "GET", "http://ptotest.mami-project.eu/raw/test/file001.json", nil, "", GoodAPIKey, http.StatusOK)
	checkContentType(t, res)

	err = json.Unmarshal(res.Body.Bytes(), &trm)
	if err != nil {
		t.Fatal(err)
	}

	b, _ := json.Marshal(data)
	if len(b) != trm.DataSize {
		t.Fatalf("file upload size mismatch: sent %d got %d", len(b), trm.DataSize)
	}
}
