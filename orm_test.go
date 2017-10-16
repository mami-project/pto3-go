package pto3_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/go-pg/pg"
	pto3 "github.com/mami-project/pto3-go"
)

var TestDB *pg.DB

func TestObservationRoundtrip(t *testing.T) {

	TestDB := pg.Connect(&(TestConfig.ObsDatabase))

	set_json := []byte(`
	{"_sources": 
		["https://ptotest.mami-project.eu/raw/test/test001.ndjson",
		"https://ptotest.mami-project.eu/raw/test/test002.ndjson"],
	 "_analyzer": "https://ptotest.mami-project.eu/analysis/passthrough",
	 "description": "a simple test observation set",
	 "contact": "test@corvid.ch"}
	 `)

	obs_json := []byte(`[0,"2009-02-20T13:00:34Z","2009-02-20T13:15:17Z","[1.2.3.4, *, 5.6.7.8]","pto.test.succeeded"]`)

	var set pto3.ObservationSet
	if err := json.Unmarshal(set_json, &set); err != nil {
		t.Fatal(err)
	}

	if err := set.Insert(TestDB, true); err != nil {
		t.Fatal(err)
	}

	var obsin pto3.Observation
	if err := json.Unmarshal(obs_json, &obsin); err != nil {
		t.Fatal(err)
	}

	if err := obsin.InsertInSet(TestDB, &set); err != nil {
		t.Fatal(err)
	}

	var obsout pto3.Observation
	obsout.ID = obsin.ID
	if err := TestDB.Select(&obsout); err != nil {
		t.Fatal(err)
	}

	// This just feels slow. We almost certainly want a condition cache. We
	// might want a path cache, or to store paths as
	obsout.Condition = &pto3.Condition{ID: obsout.ConditionID}
	if err := TestDB.Select(obsout.Condition); err != nil {
		t.Fatal(err)
	}

	obsout.Path = &pto3.Path{ID: obsout.PathID}
	if err := TestDB.Select(obsout.Path); err != nil {
		t.Fatal(err)
	}

	obs_json_out, err := json.Marshal(&obsout)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(obs_json[3:], obs_json_out[3:]) {
		t.Fatalf("posted observation %s, got observation %s", obs_json, obs_json_out)
	}
}
