package pto3_test

import (
	"testing"
)

func TestQueryParsing(t *testing.T) {
	encodedTestQueries := []string{
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.red",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*&group=condition",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*&group=condition&group=week",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*&option=sets_only",
	}

	for i := range encodedTestQueries {
		// first create an initial query, then render it in normalized form
		q0, err := TestQueryCache.ParseQueryFromURLEncoded(encodedTestQueries[i])
		if err != nil {
			t.Fatal(err)
		}
		q0e := q0.URLEncoded()

		// now parse and normalize it again
		q1, err := TestQueryCache.ParseQueryFromURLEncoded(q0e)
		if err != nil {
			t.Fatal(err)
		}
		q1e := q1.URLEncoded()

		// compare normalized forms
		if q0e != q1e {
			t.Fatalf("parsed query %s, got first round %s and second round %s", encodedTestQueries[i], q0e, q1e)
		}
	}
}

func TestTimeQuery(t *testing.T) {

	testQuery := "time_start=2017-12-05T15%3A00%3A00Z&time_end=2017-12-05T15%3A05%3A00Z"

	q, err := TestQueryCache.SubmitQueryFromURLEncoded(testQuery)
	if err != nil {
		t.Fatal(err)
	}

	// execute query and wait for result
	done := make(chan struct{})
	q.Execute(done)
	<-done

	//

}
