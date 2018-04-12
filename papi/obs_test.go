package papi_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

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
	Prev string   `json:"prev"`
	Next string   `json:"prev"`
}

// func WriteObservations(obsdat []pto3.Observation, out io.Writer) error {
// 	for _, obs := range obsdat {
// 		b, err := json.Marshal(&obs)
// 		if err != nil {
// 			return err
// 		}
// 		_, err = out.Write(b)
// 		if err != nil {
// 			return err
// 		}
// 		_, err = out.Write([]byte("\n"))
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func MarshalObservations(obsdat []pto3.Observation) ([]byte, error) {
// 	var out bytes.Buffer
// 	err := WriteObservations(obsdat, &out)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return out.Bytes(), err
// }

func ReadObservations(in io.Reader) ([]pto3.Observation, error) {
	sin := bufio.NewScanner(in)
	out := make([]pto3.Observation, 0)
	var obs pto3.Observation
	for sin.Scan() {
		if err := json.Unmarshal([]byte(sin.Text()), &obs); err != nil {
			return nil, err
		}
		out = append(out, obs)
	}
	return out, nil
}

func compareObservationSlices(a, b []pto3.Observation) error {
	ma := make(map[string]struct{})
	mb := make(map[string]struct{})

	for _, oa := range a {
		oas := fmt.Sprintf("%s|%s|%s|%s",
			oa.TimeStart.Format(time.RFC3339),
			oa.TimeEnd.Format(time.RFC3339),
			oa.Path.String,
			oa.Condition.Name)
		ma[oas] = struct{}{}
	}

	for _, ob := range b {
		obs := fmt.Sprintf("%s|%s|%s|%s",
			ob.TimeStart.Format(time.RFC3339),
			ob.TimeEnd.Format(time.RFC3339),
			ob.Path.String,
			ob.Condition.Name)
		mb[obs] = struct{}{}
	}

	for ka := range ma {
		if _, ok := mb[ka]; ok == false {
			return fmt.Errorf("second observation set missing observation %s", ka)
		}
	}

	for kb := range mb {
		if _, ok := ma[kb]; ok == false {
			return fmt.Errorf("first observation set missing observation %s", kb)
		}
	}

	return nil
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

	// save the datalink, we'll use it later
	datalink := setDown.Datalink

	// change the description, update the database, make sure PUT update works
	setUp = setDown

	setUp.Description = "An updated observation set to exercise observation set metdata and data storage"

	res = executeWithJSON(TestRouter, t, "PUT", setlink, setUp, GoodAPIKey, http.StatusCreated)

	setDown = ClientObservationSet{}
	if err := json.Unmarshal(res.Body.Bytes(), &setDown); err != nil {
		t.Fatal(err)
	}

	if setDown.Description != "An updated observation set to exercise observation set metdata and data storage" {
		t.Fatal("failed to update description via PUT")
	}

	// now upload some data
	observations_up_bytes := []byte(`["e1337", "2017-10-01T10:06:00Z", "2017-10-01T10:06:00Z", "10.0.0.1 * 10.0.0.2", "pto.test.succeeded"]
	["e1337", "2017-10-01T10:06:01Z", "2017-10-01T10:06:02Z", "10.0.0.1 AS1 * AS2 10.0.0.2", "pto.test.schroedinger"]
	["e1337", "2017-10-01T10:06:03Z", "2017-10-01T10:06:05Z", "* AS2 10.0.0.0/24", "pto.test.failed"]
	["e1337", "2017-10-01T10:06:07Z", "2017-10-01T10:06:11Z", "[2001:db8::33:a4] * [2001:db8:3]/64", "pto.test.succeeded"]
	["e1337", "2017-10-01T10:06:09Z", "2017-10-01T10:06:14Z", "[2001:db8::33:a4] * [2001:db8:3]/64", "pto.test.succeeded"]`)

	observations_up, err := ReadObservations(bytes.NewBuffer(observations_up_bytes))
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

	if err := compareObservationSlices(observations_up, observations_down); err != nil {
		t.Fatal(err)
	}
}

func TestObsQuery(t *testing.T) {

	res := executeRequest(TestRouter, t, "GET", "https://ptotest.mami-project.eu/obs/by_metadata?k=this_is_the_query_test_obset", nil, "", GoodAPIKey, http.StatusOK)

	var setlist ClientSetList
	if err := json.Unmarshal(res.Body.Bytes(), &setlist); err != nil {
		t.Fatal(err)
	}

	if len(setlist.Sets) != 1 || setlist.Sets[0] != fmt.Sprintf("https://ptotest.mami-project.eu/obs/%x", TestQueryCacheSetID) {
		t.Fatalf("unexpected result for ?k=this_is_the_query_test_obset: %v", setlist.Sets)
	}

	res = executeRequest(TestRouter, t, "GET", "https://ptotest.mami-project.eu/obs/by_metadata?k=test_obset_type&v=query", nil, "", GoodAPIKey, http.StatusOK)

	if err := json.Unmarshal(res.Body.Bytes(), &setlist); err != nil {
		t.Fatal(err)
	}

	if len(setlist.Sets) != 1 || setlist.Sets[0] != fmt.Sprintf("https://ptotest.mami-project.eu/obs/%x", TestQueryCacheSetID) {
		t.Fatalf("unexpected result for ?k=test_obset_type&v=query: %v", setlist.Sets)
	}

	res = executeRequest(TestRouter, t, "GET", "https://ptotest.mami-project.eu/obs/by_metadata?analyzer=https%3A//localhost%3A8383/query_test_analyzer.json", nil, "", GoodAPIKey, http.StatusOK)

	if err := json.Unmarshal(res.Body.Bytes(), &setlist); err != nil {
		t.Fatal(err)
	}

	if len(setlist.Sets) != 1 || setlist.Sets[0] != fmt.Sprintf("https://ptotest.mami-project.eu/obs/%x", TestQueryCacheSetID) {
		t.Fatalf("unexpected result for analyzer query: %v", setlist.Sets)
	}

	res = executeRequest(TestRouter, t, "GET", "https://ptotest.mami-project.eu/obs/by_metadata?k=this_is_the_query_test_obset&condition=pto.test.color.orange", nil, "", GoodAPIKey, http.StatusOK)

	if err := json.Unmarshal(res.Body.Bytes(), &setlist); err != nil {
		t.Fatal(err)
	}

	if len(setlist.Sets) != 1 || setlist.Sets[0] != fmt.Sprintf("https://ptotest.mami-project.eu/obs/%x", TestQueryCacheSetID) {
		t.Fatalf("unexpected result for ?k=this_is_the_query_test_obset&condition=pto.test.color.orange: %v", setlist.Sets)
	}

}
