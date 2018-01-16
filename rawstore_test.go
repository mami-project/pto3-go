package pto3_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	pto3 "github.com/mami-project/pto3-go"
	"golang.org/x/crypto/sha3"
)

func TestRawRoundtrip(t *testing.T) {

	// create a campaign with some metadata
	var cammd_up pto3.RawMetadata
	campaignMetadata := `
		{
			"_owner": "brian@trammell.ch",
			"_file_type": "obs",
			"override_me_0": "campaign",
			"override_me_1": "campaign"
		}	
`
	if err := json.Unmarshal([]byte(campaignMetadata), &cammd_up); err != nil {
		t.Fatal(err)
	}

	cam, err := TestRDS.CreateCampaign("test_0", &cammd_up)
	if err != nil {
		t.Fatal(err)
	}

	// retrieve campaign metadata and verify stored value

	// create a file with some other metadata
	filemd_up, err := pto3.RawMetadataFromFile("testdata/test_raw_metadata.json", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := cam.PutFileMetadata("test_raw.ndjson", filemd_up); err != nil {
		t.Fatal(err)
	}

	// retrieve file metadata and verify stored value
	cammd_down, err := cam.GetCampaignMetadata()
	if err != nil {
		t.Fatal(err)
	}

	if cammd_down.Owner(true) != "brian@trammell.ch" {
		t.Fatal("owner mismatch on campaign metadata")
	}

	// retrieve file metadata and verify stored value
	filemd_down, err := cam.GetFileMetadata("test_raw.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if filemd_down.Get("override_me_0", true) != "campaign" {
		t.Fatalf("metadata retrieval error; raw metadata is %v", filemd_down.Metadata)
	}
	if filemd_down.Get("override_me_1", true) != "file" {
		t.Fatalf("metadata retrieval error; raw metadata is %v", filemd_down.Metadata)
	}

	// verify metadata inheritance works across overwrite in file
	filemd_up.Metadata["override_me_0"] = "file"

	if err := cam.PutFileMetadata("test_raw.ndjson", filemd_up); err != nil {
		t.Fatal(err)
	}
	filemd_down, err = cam.GetFileMetadata("test_raw.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if filemd_down.Get("override_me_0", true) != "file" {
		t.Fatalf("metadata retrieval error; raw metadata is %v", filemd_down.Metadata)
	}

	// verify metadata inheritance works across overwrite in campaign
	cammd_up.Metadata["override_me_2"] = "campaign"

	if err := cam.PutCampaignMetadata(&cammd_up); err != nil {
		t.Fatal(err)
	}
	filemd_down, err = cam.GetFileMetadata("test_raw.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if filemd_down.Get("override_me_2", true) != "campaign" {
		t.Fatalf("metadata retrieval error; raw metadata is %v", filemd_down.Metadata)
	}

	// verify metadata inheritance works across overwrite in file
	delete(filemd_up.Metadata, "override_me_1")

	if err := cam.PutFileMetadata("test_raw.ndjson", filemd_up); err != nil {
		t.Fatal(err)
	}
	filemd_down, err = cam.GetFileMetadata("test_raw.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if filemd_down.Get("override_me_1", true) != "campaign" {
		t.Fatalf("metadata retrieval error; raw metadata is %v", filemd_down.Metadata)
	}

	// now let's test data storage
	var testbytes []byte
	testhash := make([]byte, 64)

	// stream a file into the store
	func() {
		datafile, err := cam.WriteFileData("test_raw.ndjson", false)
		if err != nil {
			t.Fatal(err)
		}
		defer datafile.Close()

		testfile, err := os.Open("testdata/test_raw.ndjson")
		defer testfile.Close()

		// store the test file hash for later
		testbytes, err = ioutil.ReadAll(testfile)
		if err != nil {
			t.Fatal(err)
		}

		sha3.ShakeSum256(testhash, testbytes)

		if _, err := testfile.Seek(0, 0); err != nil {
			t.Fatal(err)
		}

		// now upload the file
		if err := pto3.StreamCopy(testfile, datafile); err != nil {
			t.Fatal(err)
		}
	}()

	var databytes []byte
	datahash := make([]byte, 64)

	// download the file and hash it to verify the contents match
	func() {
		datafile, err := cam.ReadFileData("test_raw.ndjson")
		if err != nil {
			t.Fatal(err)
		}
		defer datafile.Close()

		databytes, err = ioutil.ReadAll(datafile)
		if err != nil {
			t.Fatal(err)
		}

		sha3.ShakeSum256(datahash, databytes)
	}()

	if fmt.Sprintf("%x", datahash) != fmt.Sprintf("%x", testhash) {
		t.Fatalf("error retrieving raw data file: expected hash %x len %d, got hash %x len %d", testhash, len(testbytes), datahash, len(databytes))
	}

}
