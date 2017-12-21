package pto3_test

import (
	"bufio"
	"fmt"
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

func TestSelectQueries(t *testing.T) {
	testSelectQueries := []struct {
		encoded string
		count   int
	}{
		{"time_start=2017-12-05T15%3A00%3A00Z&time_end=2017-12-05T15%3A05%3A00Z", 601},
		{"time_start=2017-12-05T15%3A00%3A00Z&time_end=2017-12-05T15%3A05%3A00Z&condition=pto.test.color.green&condition=pto.test.color.indigo", 124},
		{"time_start=2017-12-05T15%3A00%3A00Z&time_end=2017-12-05T15%3A05%3A00Z&target=10.13.14.253", 0},
	}

	for i, qspec := range testSelectQueries {

		// verify we're only querying our test set, for repeatability
		encoded := qspec.encoded + fmt.Sprintf("&set=%x", TestQueryCacheSetID)

		q, err := TestQueryCache.SubmitQueryFromURLEncoded(encoded)
		if err != nil {
			t.Fatal(err)
		}

		// execute query and wait for result
		done := make(chan struct{})
		q.Execute(done)
		<-done

		// verify we think have a result
		if q.Completed == nil {
			t.Fatal("Query %d did not complete", i)
		}

		// verify the query thinks it completed
		if q.ExecutionError != nil {
			t.Fatalf("Query %d failed: %v", i, q.ExecutionError)
		}

		// basic correctness check: look for the right number of rows
		resfile, err := q.ReadResultFile()
		if err != nil {
			t.Fatalf("Could not open result file for query %d: %v", i, err)
		}
		defer resfile.Close()

		resscan := bufio.NewScanner(resfile)
		j := 0
		for resscan.Scan() {
			j++
		}
		if j != qspec.count {
			t.Fatalf("Query %d failed: expected %d rows got %d", i, qspec.count, j)
		}
	}
}

// func TestOneGroupQueries(t *testing.T) {

// 	testGroupQueries := []struct {
// 		encoded string
// 		group   string
// 		count   int
// 	}{
// 		{"time_start=2017-12-05&time_end=2017-12-06&group=conditions", "pto.test.color.red", 0},
// 	}

// 	for i, qspec := range testGroupQueries {

// 		// verify we're only querying our test set, for repeatability
// 		encoded := qspec.encoded + fmt.Sprintf("&set=%x", TestQueryCacheSetID)

// 		q, err := TestQueryCache.SubmitQueryFromURLEncoded(encoded)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		// execute query and wait for result
// 		done := make(chan struct{})
// 		q.Execute(done)
// 		<-done

// 		// verify we think have a result
// 		if q.Completed == nil {
// 			t.Fatal("Query %d did not complete", i)
// 		}

// 		// verify the query thinks it completed
// 		if q.ExecutionError != nil {
// 			t.Fatalf("Query %d failed: %v", i, q.ExecutionError)
// 		}

// 		// load query data from file
// 		resfile, err := q.ReadResultFile()
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		defer resfile.Close()

// 		// FIXME work pointer
// 		// we need a utility function here that iterates over ndjson arrays for reading stored result data
// 	}
// }
