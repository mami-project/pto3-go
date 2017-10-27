package pto3_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	pto3 "github.com/mami-project/pto3-go"
)

var rds *pto3.RawDataStore

func checkContentType(t *testing.T, res *httptest.ResponseRecorder) {
	if res.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected content type %s", res.Header().Get("Content-Type"))
	}
}

type testCampaignMetadata struct {
	FileType    string `json:"_file_type"`
	Owner       string `json:"_owner"`
	Description string `json:"description"`
}

type testFileMetadata struct {
	TimeStart string `json:"_time_start"`
	TimeEnd   string `json:"_time_end"`
	DataSize  int    `json:"__data_size"`
	DataURL   string `json:"__data"`
}

type testRawMetadata struct {
	testCampaignMetadata
	testFileMetadata
}

func TestScanCampaigns(t *testing.T) {
	// create a test directory
	if err := os.Mkdir(filepath.Join(TestConfig.RawRoot, "scantest"), 0755); err != nil {
		t.Fatal(err)
	}

	// create a metadata file in it
	mdmap := testCampaignMetadata{
		FileType:    "test",
		Owner:       "ptotest@mami-project.eu",
		Description: "An empty campaign designed to force a scanCampaigns() to run",
	}

	mdfile, err := os.Create(filepath.Join(TestConfig.RawRoot, "scantest", pto3.CampaignMetadataFilename))
	if err != nil {
		t.Fatal(err)
	}
	defer mdfile.Close()
	if err != nil {
		t.Fatal(err)
	}

	b, err := json.Marshal(mdmap)

	if _, err := mdfile.Write(b); err != nil {
		t.Fatal(err)
	}

	// list campaigns to force a rescan
	res := executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw", nil, "", GoodAPIKey, http.StatusOK)
	checkContentType(t, res)

	var camlist struct {
		Campaigns []string `json:"campaigns"`
	}

	if err := json.Unmarshal(res.Body.Bytes(), &camlist); err != nil {
		t.Fatal(err)
	}

	if len(camlist.Campaigns) < 1 {
		t.Fatal("missing campaign")
	}
}

func TestBadAuth(t *testing.T) {
	executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw", nil, "", "abadc0de", http.StatusForbidden)
}

func TestRawRoundtrip(t *testing.T) {
	// create a new campaign
	cmd := testCampaignMetadata{
		FileType:    "test",
		Owner:       "ptotest@mami-project.eu",
		Description: "a campaign filled with uninteresting test data",
	}
	t.Log("attempting to create http://ptotest.mami-project.eu/raw/test")

	res := executeWithJSON(TestRouter, t, "PUT", TestBaseURL+"/raw/test", cmd, GoodAPIKey, http.StatusCreated)

	// check campaign metadata download
	res = executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw/test", nil, "", GoodAPIKey, 200)
	err := json.Unmarshal(res.Body.Bytes(), &cmd)
	if err != nil {
		t.Fatal(err)
	}

	if cmd.Description != "a campaign filled with uninteresting test data" {
		t.Fatalf("campaign metadata retrieval failed; got description %s", cmd.Description)
	}

	// create a file within the campaign
	fmd := testFileMetadata{
		TimeStart: "2010-01-01T00:00:00Z",
		TimeEnd:   "2010-01-02T00:00:00Z",
	}
	t.Log("attempting to create http://ptotest.mami-project.eu/raw/test/file001.json")
	res = executeWithJSON(TestRouter, t, "PUT", TestBaseURL+"/raw/test/file001.json", fmd, GoodAPIKey, http.StatusCreated)

	// find the data link
	var rmd testRawMetadata
	err = json.Unmarshal(res.Body.Bytes(), &rmd)
	if err != nil {
		t.Fatal(err)
	}

	if rmd.DataURL == "" {
		t.Fatal("missing __data virtual after metadata upload")
	}

	t.Logf("attempting to upload file to %s", rmd.DataURL)

	// now upload the data
	data := []string{"this", "is", "a", "list", "of", "words"}
	res = executeWithJSON(TestRouter, t, "PUT", rmd.DataURL, data, GoodAPIKey, http.StatusCreated)

	// retrieve file metadata and check file size
	res = executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw/test/file001.json", nil, "", GoodAPIKey, http.StatusOK)
	checkContentType(t, res)

	err = json.Unmarshal(res.Body.Bytes(), &rmd)
	if err != nil {
		t.Fatal(err)
	}

	bytesup, _ := json.Marshal(data)
	if len(bytesup) != rmd.DataSize {
		t.Fatalf("file upload size mismatch: sent %d got %d", len(bytesup), rmd.DataSize)
	}

	// now download the file
	res = executeRequest(TestRouter, t, "GET", rmd.DataURL, nil, "", GoodAPIKey, 200)

	bytesdown := res.Body.Bytes()
	if !bytes.Equal(bytesup, bytesdown) {
		t.Fatalf("file download content mismatch: sent %s got %s", bytesup, bytesdown)
	}
}
