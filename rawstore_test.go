package pto3_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	pto3 "github.com/mami-project/pto3-go"
	"golang.org/x/crypto/sha3"
)

func TestRawExisting(t *testing.T) {

	// get campaign
	cam, err := TestRDS.CampaignForName("test0")
	if err != nil {
		t.Fatal(err)
	}

	// list files
	files, err := cam.FileNames()
	if err != nil {
		t.Fatal(err)
	}

	if len(files) != 1 {
		t.Fatalf("expected one file in existing repository, found %d", len(files))
	}

	// get file metadata
	filemd, err := cam.GetFileMetadata(files[0])
	if err != nil {
		t.Fatal(err)
	}

	if filemd.Owner(true) != "brian@trammell.ch" {
		t.Fatalf("bad owner on existing file, found %s", filemd.Owner(true))
	}

	if filemd.Get("override_me_1", true) != "file" {
		t.Fatalf("bad overriden metadata on existing file, found %s", filemd.Get("override_me_1", true))
	}
}

func TestRawRoundtrip(t *testing.T) {

	// create a campaign with some metadata
	cammd_up, err := pto3.RawMetadataFromFile("testdata/test_raw_campaign_metadata.json", nil)
	if err != nil {
		t.Fatal(err)
	}

	cam, err := TestRDS.CreateCampaign("test1", cammd_up)
	if err != nil {
		t.Fatal(err)
	}

	// grab a timestamp (to verify creation/modification time)
	nowish := time.Now()

	// create file metadata
	filemd_up, err := pto3.RawMetadataFromFile("testdata/test_raw_metadata.json", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := cam.PutFileMetadata("test-1-0-obs.ndjson", filemd_up); err != nil {
		t.Fatal(err)
	}

	// retrieve campaign metadata and verify stored value
	cammd_down, err := cam.GetCampaignMetadata()
	if err != nil {
		t.Fatal(err)
	}

	if cammd_down.Owner(true) != "brian@trammell.ch" {
		t.Fatal("owner mismatch on campaign metadata")
	}

	// retrieve file metadata and verify stored value
	filemd_down, err := cam.GetFileMetadata("test-1-0-obs.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if filemd_down.Get("override_me_0", true) != "campaign" {
		t.Fatalf("metadata retrieval error; raw metadata is %v", filemd_down.Metadata)
	}
	if filemd_down.Get("override_me_1", true) != "file" {
		t.Fatalf("metadata retrieval error; raw metadata is %v", filemd_down.Metadata)
	}

	// store some data
	var testbytes []byte
	testhash := make([]byte, 64)

	func() {
		datafile, err := cam.WriteFileData("test-1-0-obs.ndjson", false)
		if err != nil {
			t.Fatal(err)
		}
		defer datafile.Close()

		testfile, err := os.Open("testdata/test_raw_data.ndjson")
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
		if _, err := io.Copy(datafile, testfile); err != nil {
			t.Fatal(err)
		}
	}()

	// verify creation time is reasonable
	creatime := filemd_down.CreationTime()
	if creatime == nil {
		t.Fatalf("nil creation time")
	}
	creationDelay := creatime.Sub(nowish)
	if creationDelay < 0 || creationDelay > 1*time.Minute {
		t.Fatalf("nonsensical creation delay %v", creationDelay)
	}

	// verify metadata inheritance works across overwrite in file
	filemd_up.Metadata["override_me_0"] = "file"

	if err := cam.PutFileMetadata("test-1-0-obs.ndjson", filemd_up); err != nil {
		t.Fatal(err)
	}
	filemd_down, err = cam.GetFileMetadata("test-1-0-obs.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if filemd_down.Get("override_me_0", true) != "file" {
		t.Fatalf("metadata retrieval error; raw metadata is %v", filemd_down.Metadata)
	}

	// verify metadata inheritance works across overwrite in campaign
	cammd_up.Metadata["override_me_2"] = "campaign"

	if err := cam.PutCampaignMetadata(cammd_up); err != nil {
		t.Fatal(err)
	}
	filemd_down, err = cam.GetFileMetadata("test-1-0-obs.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if filemd_down.Get("override_me_2", true) != "campaign" {
		t.Fatalf("metadata retrieval error; raw metadata is %v", filemd_down.Metadata)
	}

	// verify metadata inheritance works across overwrite in file
	delete(filemd_up.Metadata, "override_me_1")

	if err := cam.PutFileMetadata("test-1-0-obs.ndjson", filemd_up); err != nil {
		t.Fatal(err)
	}
	filemd_down, err = cam.GetFileMetadata("test-1-0-obs.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if filemd_down.Get("override_me_1", true) != "campaign" {
		t.Fatalf("metadata retrieval error; raw metadata is %v", filemd_down.Metadata)
	}

	// verify modification time is reasonable
	modtime := filemd_down.ModificationTime()
	if modtime == nil {
		t.Fatalf("nil modification time")
	}
	modificationDelay := modtime.Sub(*creatime)
	if modificationDelay < 0 || modificationDelay > 1*time.Minute {
		t.Fatalf("nonsensical modification delay %v", creationDelay)
	}

	// download the file and hash it to verify the contents match
	var databytes []byte
	datahash := make([]byte, 64)

	func() {
		datafile, err := cam.ReadFileData("test-1-0-obs.ndjson")
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
