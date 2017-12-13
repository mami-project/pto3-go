package pto3_test

import (
	"testing"
)

func TestQueryParsing(t *testing.T) {

	encodedTestQueries := []string{
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.red",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*",
	}

	for i := range encodedTestQueries {
		// first create a zero query
		q0, err := TestQueryCache.NewQueryFromURLEncoded(encodedTestQueries[i])
		if err != nil {
			t.Fatal(err)
		}
		q0e := q0.URLEncoded()

		// now parse it again
		q1, err := TestQueryCache.NewQueryFromURLEncoded(q0e)
		if err != nil {
			t.Fatal(err)
		}
		q1e := q1.URLEncoded()

		if q0e != q1e {
			t.Fatalf("parsed query %s, got first round %s and second round %s", encodedTestQueries[i], q0e, q1e)
		}
	}
}
