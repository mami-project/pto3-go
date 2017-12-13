package pto3_test

import (
	"testing"
)

func TestQueryParsing(t *testing.T) {

	urlEncodedQueries := []string{
		"time_start=2009-01-01T00%3A00%3A00Z&time_end=2009-02-02T00%3A00%3A00Z",
	}

	for i := range urlEncodedQueries {
		q, err := TestQueryCache.NewQueryFromURLEncoded(urlEncodedQueries[i])
		if err != nil {
			t.Fatal(err)
		}

		qe := q.URLEncoded()

		if qe != urlEncodedQueries[i] {
			t.Fatalf("parsed query %s, got urlencoded %s", urlEncodedQueries[i], qe)
		}
	}
}
