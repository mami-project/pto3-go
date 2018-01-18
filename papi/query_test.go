package papi_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"
)

type testQueryMetadata struct {
	Link        string `json:"__link"`
	Result      string `json:"__result"`
	Encoded     string `json:"__encoded"`
	Error       string `json:"__error"`
	State       string `json:"__state"`
	Submitted   string `json:"__time_submitted"`
	Executed    string `json:"__time_executed"`
	Completed   string `json:"__time_completed"`
	ExtRef      string `json:"_ext_ref"`
	Description string `json:"description"`
}

type testResultSet struct {
	Prev         string          `json:"prev"`
	Next         string          `json:"next"`
	Observations [][]string      `json:"obs"`
	Groups       [][]interface{} `json:"groups"`
}

func TestQueryLifecycle(t *testing.T) {

	// here's a simple selection query to play with
	queryParams := "time_start=" + url.QueryEscape("2017-12-05T14:00:00Z") +
		"time_end=" + url.QueryEscape("2017-12-05T14:00:00Z") +
		"condition=pto.test.color.blue"

	q := new(testQueryMetadata)

	// wait until the query completes or fails
	for {
		res := executeRequest(TestRouter, t, "GET", "https://ptotest.mami-project.eu/query/submit?"+queryParams, nil, "", GoodAPIKey, http.StatusOK)

		if err := json.Unmarshal(res.Body.Bytes(), &q); err != nil {
			t.Fatal(err)
		}

		if q.State == "failed" {
			t.Fatalf("Query failed with error %s", q.Error)
		} else if q.State == "complete" {
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}

	// update the query metadata and verify we can retrieve it
	q.Description = "this is a test query, yay!"

	res := executeWithJSON(TestRouter, t, "PUT", q.Link, q, GoodAPIKey, http.StatusAccepted)

	if err := json.Unmarshal(res.Body.Bytes(), &q); err != nil {
		t.Fatal(err)
	}

	res = executeRequest(TestRouter, t, "GET", q.Link, nil, "", GoodAPIKey, http.StatusOK)

	q = new(testQueryMetadata)

	if err := json.Unmarshal(res.Body.Bytes(), &q); err != nil {
		t.Fatal(err)
	}

	if q.Description != "this is a test query, yay!" {
		t.Fatalf("got unexpected description after update %s", q.Description)
	}

	// grab results and parse them
	res = executeRequest(TestRouter, t, "GET", q.Result, nil, "", GoodAPIKey, http.StatusOK)

	if (res.Header().Get("Content-Type")) != "application/json" {
		t.Fatalf("unexpected result content type %s", res.Header().Get("Content-Type"))
	}

	qr := new(testResultSet)

	if err := json.Unmarshal(res.Body.Bytes(), &qr); err != nil {
		t.Fatal(err)
	}

	// WORK POINTER
}
