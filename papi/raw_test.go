package papi_test

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

type testCampaignList struct {
	Campaigns []string `json:"campaigns"`
}

type testCampaignFileList struct {
	Metadata testCampaignMetadata `json:"metadata"`
	Files    []string             `json:"files"`
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
		Description: "An empty campaign designed to force a ScanCampaigns() to run",
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

	var camlist testCampaignList

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
	cmd_up := testCampaignMetadata{
		FileType:    "test",
		Owner:       "ptotest@mami-project.eu",
		Description: "a campaign filled with uninteresting test data",
	}

	res := executeWithJSON(TestRouter, t, "PUT", TestBaseURL+"/raw/test", cmd_up, GoodAPIKey, http.StatusCreated)

	// check campaign metadata download
	var cmd_down testCampaignFileList
	res = executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw/test", nil, "", GoodAPIKey, 200)
	err := json.Unmarshal(res.Body.Bytes(), &cmd_down)
	if err != nil {
		t.Fatal(err)
	}

	if cmd_up.Description != cmd_down.Metadata.Description {
		t.Fatalf("campaign metadata retrieval failed; got description %s", cmd_down.Metadata.Description)
	}

	// create a file within the campaign
	fmd_up := testFileMetadata{
		TimeStart: "2010-01-01T00:00:00Z",
		TimeEnd:   "2010-01-02T00:00:00Z",
	}
	res = executeWithJSON(TestRouter, t, "PUT", TestBaseURL+"/raw/test/file001.json", fmd_up, GoodAPIKey, http.StatusCreated)

	// find the data link
	var fmd_refl testRawMetadata
	if err = json.Unmarshal(res.Body.Bytes(), &fmd_refl); err != nil {
		t.Fatal(err)
	}

	if fmd_refl.DataURL == "" {
		t.Fatal("missing __data virtual after metadata upload")
	}

	// now download metadata
	res = executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw/test/file001.json", nil, "", GoodAPIKey, 200)

	var fmd_down testRawMetadata
	if err = json.Unmarshal(res.Body.Bytes(), &fmd_down); err != nil {
		t.Fatal(err)
	}

	if fmd_refl.DataURL != fmd_down.DataURL {
		t.Fatalf("data URL mismatch, reflected %s, downloaded %s", fmd_refl.DataURL, fmd_down.DataURL)
	}

	if fmd_down.TimeStart != fmd_up.TimeStart {
		t.Fatalf("bad start time in downloaded metadata, got %s", fmd_down.TimeStart)
	}

	if fmd_down.Description != cmd_up.Description {
		t.Fatalf("did not inherit description in downloaded metadata, got %s", fmd_down.Description)
	}

	// now upload the data
	data := []string{"this", "is", "a", "list", "of", "words"}
	res = executeWithJSON(TestRouter, t, "PUT", fmd_refl.DataURL, data, GoodAPIKey, http.StatusCreated)

	// retrieve file metadata and check file size
	res = executeRequest(TestRouter, t, "GET", TestBaseURL+"/raw/test/file001.json", nil, "", GoodAPIKey, http.StatusOK)
	checkContentType(t, res)

	err = json.Unmarshal(res.Body.Bytes(), &fmd_down)
	if err != nil {
		t.Fatal(err)
	}

	bytesup, _ := json.Marshal(data)
	if len(bytesup) != fmd_down.DataSize {
		t.Fatalf("file upload size mismatch: sent %d got %d", len(bytesup), fmd_down.DataSize)
	}

	// now download the file
	res = executeRequest(TestRouter, t, "GET", fmd_refl.DataURL, nil, "", GoodAPIKey, 200)

	bytesdown := res.Body.Bytes()
	if !bytes.Equal(bytesup, bytesdown) {
		t.Fatalf("file download content mismatch: sent %s got %s", bytesup, bytesdown)
	}
}
