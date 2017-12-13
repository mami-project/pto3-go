package pto3_test

import (
	"testing"
)

func TestQueryParsing(t *testing.T) {

	queryTests := []struct {
		q string
		n string
	}{
		{
			q: "time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z",
			n: "time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z",
		},
		{
			q: "time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.red",
			n: "time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.red",
		},
		{
			q: "time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*",
			n: "time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.blue&condition=pto.test.color.green&condition=pto.test.color.indigo&condition=pto.test.color.none_more_black&condition=pto.test.color.orange&condition=pto.test.color.red&condition=pto.test.color.violet&condition=pto.test.color.yellow",
		},
	}

	for i := range queryTests {
		q, err := TestQueryCache.NewQueryFromURLEncoded(queryTests[i].q)
		if err != nil {
			t.Fatal(err)
		}

		qe := q.URLEncoded()

		if qe != queryTests[i].n {
			t.Fatalf("parsed query %s, got urlencoded %s", queryTests[i].n, qe)
		}
	}
}
