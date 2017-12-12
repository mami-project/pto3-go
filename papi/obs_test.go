package papi_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"testing"

	pto3 "github.com/mami-project/pto3-go"
)

type ClientObservationSet struct {
	Analyzer    string   `json:"_analyzer"`
	Sources     []string `json:"_sources"`
	Conditions  []string `json:"_conditions"`
	Description string   `json:"description"`
	Link        string   `json:"__link"`
	Datalink    string   `json:"__data"`
	Count       int      `json:"__obs_count"`
}

type ClientSetList struct {
	Sets []string `json:"sets"`
}

func WriteObservations(obsdat []pto3.Observation, out io.Writer) error {
	for _, obs := range obsdat {
		b, err := json.Marshal(&obs)
		if err != nil {
			return err
		}
		_, err = out.Write(b)
		if err != nil {
			return err
		}
		_, err = out.Write([]byte("\n"))
		if err != nil {
			return err
		}
	}
	return nil
}

func MarshalObservations(obsdat []pto3.Observation) ([]byte, error) {
	var out bytes.Buffer
	err := WriteObservations(obsdat, &out)
	if err != nil {
		return nil, err
	}
	return out.Bytes(), err
}

func ReadObservations(in io.Reader) ([]pto3.Observation, error) {
	sin := bufio.NewScanner(in)
	out := make([]pto3.Observation, 0)
	var obs pto3.Observation
	for sin.Scan() {
		log.Printf("got observation line %s", sin.Text())
		if err := json.Unmarshal([]byte(sin.Text()), &obs); err != nil {
			return nil, err
		}
		out = append(out, obs)
	}
	return out, nil
}

func UnmarshalObservations(in []byte) ([]pto3.Observation, error) {
	return ReadObservations(bytes.NewBuffer(in))
}

func TestObsRoundtrip(t *testing.T) {
	// create a new observation set and retrieve the set ID
	setUp := ClientObservationSet{
		Analyzer: "https://ptotest.mami-project.eu/analysis/passthrough",
		Sources:  []string{"https://ptotest.mami-project.eu/raw/test001.json"},
		Conditions: []string{
			"pto.test.schroedinger",
			"pto.test.failed",
			"pto.test.succeeded",
		},
		Description: "An observation set to exercise observation set metdata and data storage",
	}

	res := executeWithJSON(TestRouter, t, "POST", "https://ptotest.mami-project.eu/obs/create",
		setUp, GoodAPIKey, http.StatusCreated)

	setDown := ClientObservationSet{}
	if err := json.Unmarshal(res.Body.Bytes(), &setDown); err != nil {
		t.Fatal(err)
	}

	if setDown.Link == "" {
		t.Fatal("missing __link in /obs/create POST response")
	}

	setlink := setDown.Link

	// list observation sets to ensure it shows up in the list
	res = executeRequest(TestRouter, t, "GET", "https://ptotest.mami-project.eu/obs", nil, "", GoodAPIKey, http.StatusOK)

	var setlist ClientSetList
	if err := json.Unmarshal(res.Body.Bytes(), &setlist); err != nil {
		t.Fatal(err)
	}

	ok := false
	for i := range setlist.Sets {
		if setlist.Sets[i] == setDown.Link {
			ok = true
			break
		}
	}
	if !ok {
		t.Fatal("created observation set not listed")
	}

	// retrieve observation set to ensure the metadata is properly stored
	res = executeRequest(TestRouter, t, "GET", setlink, nil, "", GoodAPIKey, http.StatusOK)

	setDown = ClientObservationSet{}
	if err := json.Unmarshal(res.Body.Bytes(), &setDown); err != nil {
		t.Fatal(err)
	}

	if setDown.Analyzer != setUp.Analyzer {
		t.Fatalf("observation set metadata analyzer mismatch, sent %s got %s", setUp.Analyzer, setDown.Analyzer)
	}

	// compare condition lists order-independently
	conditionSeen := make(map[string]bool)
	for i := range setUp.Conditions {
		conditionSeen[setUp.Conditions[i]] = false
	}
	for i := range setDown.Conditions {
		conditionSeen[setDown.Conditions[i]] = true
	}
	for i := range conditionSeen {
		if !conditionSeen[i] {
			t.Fatalf("observation set metadata condition mismatch: sent %v got %v", setUp.Conditions, setDown.Conditions)
		}
	}

	if setDown.Datalink == "" {
		t.Fatal("missing __datalink in observation set")
	}

	datalink := setDown.Datalink

	// change the description and update the database
	setDown.Description = "An updated observation set to exercise observation set metdata and data storage"

	res = executeWithJSON(TestRouter, t, "PUT", setlink, setDown, GoodAPIKey, http.StatusCreated)

	setDown = ClientObservationSet{}
	if err := json.Unmarshal(res.Body.Bytes(), &setDown); err != nil {
		t.Fatal(err)
	}

	if setDown.Description != "An updated observation set to exercise observation set metdata and data storage" {
		t.Fatal("failed to update description via PUT")
	}

	// now write some data to the observation set data link
	observations_up_bytes := []byte(`["e1337", "2017-10-01T10:06:00Z", "2017-10-01T10:06:00Z", "10.0.0.1 * 10.0.0.2", "pto.test.succeeded"]
	["e1337", "2017-10-01T10:06:01Z", "2017-10-01T10:06:02Z", "10.0.0.1 AS1 * AS2 10.0.0.2", "pto.test.schroedinger"]
	["e1337", "2017-10-01T10:06:03Z", "2017-10-01T10:06:05Z", "* AS2 10.0.0.0/24", "pto.test.failed"]
	["e1337", "2017-10-01T10:06:07Z", "2017-10-01T10:06:11Z", "[2001:db8::33:a4] * [2001:db8:3]/64", "pto.test.succeeded"]
	["e1337", "2017-10-01T10:06:09Z", "2017-10-01T10:06:14Z", "[2001:db8::33:a4] * [2001:db8:3]/64", "pto.test.succeeded"]`)

	observations_up, err := UnmarshalObservations(observations_up_bytes)
	if err != nil {
		t.Fatal(err)
	}

	res = executeRequest(TestRouter, t, "PUT", datalink, bytes.NewBuffer(observations_up_bytes),
		"application/vnd.mami.ndjson", GoodAPIKey, http.StatusCreated)

	// check count in resulting metadata
	setDown = ClientObservationSet{}
	if err := json.Unmarshal(res.Body.Bytes(), &setDown); err != nil {
		t.Fatal(err)
	}

	if setDown.Count != len(observations_up) {
		t.Fatalf("bad observation set __obs_count after data PUT: expected %d got %d", len(observations_up), setDown.Count)
	}

	// and try downloading it again
	res = executeRequest(TestRouter, t, "GET", datalink, nil, "", GoodAPIKey, http.StatusOK)

	observations_down, err := ReadObservations(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	if len(observations_up) != len(observations_down) {
		t.Fatalf("observation count mismatch: sent %d got %d", len(observations_up), len(observations_down))
	}

	// compate paths on observations
	// FIXME check timing too
	for i := range observations_up {
		if observations_up[i].Path.String != observations_down[i].Path.String {
			t.Errorf("path mismatch on observation %d sent %s got %s", i, observations_up[i].Path.String, observations_down[i].Path.String)
		}
		if observations_up[i].Condition.Name != observations_down[i].Condition.Name {
			t.Errorf("path mismatch on observation %d sent %s got %s", i, observations_up[i].Condition.Name, observations_down[i].Condition.Name)
		}
	}
}
