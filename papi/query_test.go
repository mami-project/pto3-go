package papi_test

import (
	"encoding/json"
	"fmt"
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
	Prev       string          `json:"prev"`
	Next       string          `json:"next"`
	Obs        [][]string      `json:"obs"`
	Groups     [][]interface{} `json:"groups"`
	TotalCount int             `json:"total_count"`
}

func TestQueryLifecycle(t *testing.T) {

	// here's a simple selection query to play with
	queryParams := fmt.Sprintf("set=%x&time_start=%s&time_end=%s&condition=pto.test.color.blue",
		TestQueryCacheSetID, url.QueryEscape("2017-12-05T14:00:00Z"), url.QueryEscape("2017-12-05T15:00:00Z"))

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

	// test retrieval
	executeRequest(TestRouter, t, "GET", "https://ptotest.mami-project.eu/query/retrieve?"+queryParams, nil, "", GoodAPIKey, http.StatusOK)

	// grab results, iterating over pagination, and parse them
	resultLink := q.Result
	rowCount := 0
	const expectedRowCount = 396

	for resultLink != "" {

		res := executeRequest(TestRouter, t, "GET", resultLink, nil, "", GoodAPIKey, http.StatusOK)

		if (res.Header().Get("Content-Type")) != "application/json" {
			t.Fatalf("unexpected result content type %s", res.Header().Get("Content-Type"))
		}

		qr := new(testResultSet)

		if err := json.Unmarshal(res.Body.Bytes(), &qr); err != nil {
			t.Fatal(err)
		}

		if qr.Obs == nil {
			t.Fatal("Result retrieval missing observations")
		}

		if qr.TotalCount != 0 && qr.TotalCount != expectedRowCount {
			t.Fatalf("Got pagination total count %d, expected %d", qr.TotalCount, expectedRowCount)
		}

		rowCount += len(qr.Obs)
		resultLink = qr.Next
	}

	if rowCount != expectedRowCount {
		t.Fatalf("expected %d rows, got %d", expectedRowCount, rowCount)
	}

	// update the query metadata and verify we can retrieve it
	q.Description = "this is a test query, yay!"

	res := executeWithJSON(TestRouter, t, "PUT", q.Link, q, GoodAPIKey, http.StatusOK)

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

	// now make the query permanent by writing to its external reference

	q.ExtRef = "https://example.com/this-is-a-test-external-reference"

	res = executeWithJSON(TestRouter, t, "PUT", q.Link, q, GoodAPIKey, http.StatusOK)

	if err := json.Unmarshal(res.Body.Bytes(), &q); err != nil {
		t.Fatal(err)
	}

	// check reflected state
	q = new(testQueryMetadata)

	if err := json.Unmarshal(res.Body.Bytes(), &q); err != nil {
		t.Fatal(err)
	}

	if q.State != "permanent" {
		t.Fatal("adding external reference did not make query permanent")
	}

}
