package pto3_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	pto3 "github.com/mami-project/pto3-go"
)

func observationsInFile(r io.Reader) (map[string]struct{}, error) {
	m := make(map[string]struct{})
	s := bufio.NewScanner(r)
	for s.Scan() {
		var o pto3.Observation

		line := s.Text()
		if len(line) > 0 && line[0] == '[' {

			if err := json.Unmarshal([]byte(line), &o); err != nil {
				return nil, err
			}

			m[fmt.Sprintf("%s|%s|%s|%s",
				o.TimeStart.Format(time.RFC3339),
				o.TimeEnd.Format(time.RFC3339),
				o.Path.String,
				o.Condition.Name)] = struct{}{}
		}
	}

	return m, nil
}

// This test simulates the operation of a normalizer. It tests reads from the
// raw data store, writes to the observation store, and reads from the
// observation store (to verify the stored data)
// It uses the initial raw data in testdata/test_raw_init
func TestNormalization(t *testing.T) {

	// grab a file from the raw data store
	cam, err := TestRDS.CampaignForName("test0")
	if err != nil {
		t.Fatal(err)
	}

	obsdata, err := cam.ReadFileData("test0-0-obs.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer obsdata.Close()

	b, err := ioutil.ReadAll(obsdata)
	if err != nil {
		t.Fatal(err)
	}

	// build a set containing observations for later comparison
	rawobsset, err := observationsInFile(bytes.NewBuffer(b))
	if err != nil {
		t.Fatal(err)
	}

	// now dump the observations into a temporary file for loading
	tf, err := ioutil.TempFile("", "pto3-test-obs")
	if err != nil {
		t.Fatal(err)
	}
	defer tf.Close()
	defer os.Remove(tf.Name())

	if _, err := tf.Write(b); err != nil {
		t.Fatal(err)
	}

	// append some observation set metadata to the temporary file
	mdf, err := os.Open("testdata/test_obset_metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	defer mdf.Close()

	if _, err := io.Copy(tf, mdf); err != nil {
		t.Fatal(err)
	}

	if err := tf.Sync(); err != nil {
		t.Fatal(err)
	}

	// verify creation and modification time set
	nowish := time.Now()

	// create an observation set from this normalized file
	cidCache, err := pto3.LoadConditionCache(TestDB)
	if err != nil {
		t.Fatal(err)
	}

	pidCache := make(pto3.PathCache)

	set, err := pto3.CopySetFromObsFile(tf.Name(), TestDB, cidCache, pidCache)
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("created observation set ID:%d", set.ID)

	creationDelay := set.Created.Sub(nowish)
	if creationDelay < 0 || creationDelay > 1*time.Minute {
		t.Fatalf("nonsensical obset creation delay %v", creationDelay)
	}

	// retrieve stored observation data with the one we uploaded
	dataout := new(bytes.Buffer)

	if err := set.CopyDataToStream(TestDB, dataout); err != nil {
		t.Fatal(err)
	}

	outobsset, err := observationsInFile(bytes.NewReader(dataout.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for k := range rawobsset {
		if _, ok := outobsset[k]; ok == false {
			t.Fatalf("retrieved observation set missing observation %s at index %d", k, i)
		}
		i++
	}

	i = 0
	for k := range outobsset {
		if _, ok := rawobsset[k]; ok == false {
			t.Fatalf("retrieved observation has spurious observation %s at index %d", k, i)
		}
		i++
	}

}
