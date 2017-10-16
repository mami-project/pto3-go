package pto3_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	pto3 "github.com/mami-project/pto3-go"
)

var rds *pto3.RawDataStore

func checkContentType(t *testing.T, res *httptest.ResponseRecorder) {
	if res.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected content type %s", res.Header().Get("Content-Type"))
	}
}

func TestListCampaigns(t *testing.T) {
	// list campaigns, this should be empty
	res := executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw", nil, "", GoodAPIKey, http.StatusOK)
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
	executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw", nil, "", "abadc0de", http.StatusForbidden)
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

func TestRawRoundtrip(t *testing.T) {
	// create a new campaign
	md := testRawMetadata{
		FileType:    "test",
		Owner:       "ptotest@mami-project.eu",
		Description: "a campaign filled with uninteresting test data",
	}
	t.Log("attempting to create http://ptotest.mami-project.eu/raw/test")

	res := executeWithJSON(TestRouter, t, "PUT", TestBaseURL+"/raw/test", md, GoodAPIKey, http.StatusCreated)

	// check campaign metadata download
	res = executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw/test", nil, "", GoodAPIKey, 200)
	err := json.Unmarshal(res.Body.Bytes(), &md)
	if err != nil {
		t.Fatal(err)
	}
	if md.Description != "a campaign filled with uninteresting test data" {
		t.Fatalf("campaign metadata retrieval failed; got description %s", md.Description)
	}

	// create a file within the campaign
	md = testRawMetadata{
		TimeStart: "2010-01-01T00:00:00Z",
		TimeEnd:   "2010-01-02T00:00:00Z",
	}
	t.Log("attempting to create http://ptotest.mami-project.eu/raw/test/file001.json")
	res = executeWithJSON(TestRouter, t, "PUT", TestBaseURL+"/raw/test/file001.json", md, GoodAPIKey, http.StatusCreated)

	// find the data link
	var trm testRawMetadata
	err = json.Unmarshal(res.Body.Bytes(), &trm)
	if err != nil {
		t.Fatal(err)
	}

	if trm.DataURL == "" {
		t.Fatal("missing __data virtual after metadata upload")
	}

	t.Logf("attempting to upload file to %s", trm.DataURL)

	// now upload the data
	data := []string{"this", "is", "a", "list", "of", "words"}
	res = executeWithJSON(TestRouter, t, "PUT", trm.DataURL, data, GoodAPIKey, http.StatusCreated)

	// retrieve file metadata and check file size
	res = executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw/test/file001.json", nil, "", GoodAPIKey, http.StatusOK)
	checkContentType(t, res)

	err = json.Unmarshal(res.Body.Bytes(), &trm)
	if err != nil {
		t.Fatal(err)
	}

	bytesup, _ := json.Marshal(data)
	if len(bytesup) != trm.DataSize {
		t.Fatalf("file upload size mismatch: sent %d got %d", len(bytesup), trm.DataSize)
	}

	// now download the file
	res = executeRequest(TestRouter, t, "GET", trm.DataURL, nil, "", GoodAPIKey, 200)

	bytesdown := res.Body.Bytes()
	if !bytes.Equal(bytesup, bytesdown) {
		t.Fatalf("file download content mismatch: sent %s got %s", bytesup, bytesdown)
	}
}
