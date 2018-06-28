package pto3_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"testing"

	pto3 "github.com/mami-project/pto3-go"
)

type groupQueryResult struct {
	groups []string
	count  int
}

func parseGroupQueryResults(in io.Reader) ([]groupQueryResult, error) {
	s := bufio.NewScanner(in)
	out := make([]groupQueryResult, 0)
	lineno := 0

	for s.Scan() {
		var line []interface{}
		lineno++

		if err := json.Unmarshal([]byte(s.Text()), &line); err != nil {
			return nil, err
		}

		switch len(line) {
		case 2:
			g0, ok := line[0].(string)
			if !ok {
				return nil, pto3.PTOErrorf("result group 0 not a string at line %d", lineno)
			}
			count, ok := line[1].(float64)
			if !ok {
				return nil, pto3.PTOErrorf("result count not a number at line %d", lineno)
			}
			out = append(out, groupQueryResult{[]string{g0}, int(count)})
		case 3:
			g0, ok := line[0].(string)
			if !ok {
				return nil, pto3.PTOErrorf("result group 0 not a string at line %d", lineno)
			}
			g1, ok := line[1].(string)
			if !ok {
				return nil, pto3.PTOErrorf("result group 1 not a string at line %d", lineno)
			}
			count, ok := line[2].(float64)
			if !ok {
				return nil, pto3.PTOErrorf("result count not a number at line %d", lineno)
			}
			out = append(out, groupQueryResult{[]string{g0, g1}, int(count)})
		}
	}

	return out, nil
}

func TestQueryParsing(t *testing.T) {
	encodedTestQueries := []string{
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.red",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*&group=condition",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*&group=condition&group=week",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*&option=sets_only",
		"time_start=2017-12-05T14%3A31%3A26Z&time_end=2017-12-05T16%3A31%3A53Z&condition=pto.test.color.*&value=0",
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
		{"time_start=2017-12-05T15%3A00%3A00Z&time_end=2017-12-05T15%3A05%3A00Z&feature=pto", 601},
		{"time_start=2017-12-05T15%3A00%3A00Z&time_end=2017-12-05T15%3A05%3A00Z&aspect=pto.test.color", 601},
		{"time_start=2017-12-05T15%3A00%3A00Z&time_end=2017-12-05T15%3A05%3A00Z&target=10.13.14.253", 0},
		{"time_start=2017-12-05T15%3A00%3A00Z&time_end=2017-12-05T15%3A05%3A00Z&value=nonesuch", 0},
	}

	for i, qspec := range testSelectQueries {

		// verify we're only querying our test set, for repeatability
		encoded := qspec.encoded + fmt.Sprintf("&set=%x", TestQueryCacheSetID)

		// submit query and wait for result
		done := make(chan struct{})
		q, _, err := TestQueryCache.ExecuteQueryFromURLEncoded(encoded, done)
		if err != nil {
			t.Fatal(err)
		}
		<-done

		// verify we think have a result
		if q.Completed == nil {
			t.Fatalf("Query %d did not complete", i)
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

func TestOneGroupQueries(t *testing.T) {

	testQueries := []struct {
		encoded string
		group   string
		count   int
	}{
		{"time_start=2017-12-05&time_end=2017-12-06&group=condition", "pto.test.color.red", 3195},
		{"time_start=2017-12-05&time_end=2017-12-06&group=source", "2001:db8:e55:5::33", 3273},
		{"time_start=2017-12-05&time_end=2017-12-06&group=target", "10.15.16.17", 7},
		{"time_start=2017-12-05&time_end=2017-12-06&group=day_hour", "14", 3412},
		{"time_start=2017-12-05&time_end=2017-12-06&group=condition&option=count_targets", "pto.test.color.red", 1832},
		{"time_start=2017-12-05&time_end=2017-12-06&group=value", "0", 14400},
		{"time_start=2017-12-05&time_end=2017-12-06&group=feature", "pto", 14400},
		{"time_start=2017-12-05&time_end=2017-12-06&group=aspect", "pto.test.color", 14400},
	}

	for i, qspec := range testQueries {

		// verify we're only querying our test set, for repeatability
		encoded := qspec.encoded + fmt.Sprintf("&set=%x", TestQueryCacheSetID)

		// submit query and wait for result
		done := make(chan struct{})
		q, _, err := TestQueryCache.ExecuteQueryFromURLEncoded(encoded, done)
		if err != nil {
			t.Fatal(err)
		}
		<-done

		// verify we think have a result
		if q.Completed == nil {
			t.Fatalf("Query %d did not complete", i)
		}

		// verify the query thinks it completed
		if q.ExecutionError != nil {
			t.Fatalf("Query %d failed: %v", i, q.ExecutionError)
		}

		// load query data from file
		resfile, err := q.ReadResultFile()
		if err != nil {
			t.Fatal(err)
		}
		defer resfile.Close()

		// load query results
		groupResults, err := parseGroupQueryResults(resfile)
		if err != nil {
			t.Fatal(err)
		}

		// search through them to find the group we care about
		gotExpectedGroup := false
		for j := range groupResults {
			if groupResults[j].groups[0] == qspec.group {
				gotExpectedGroup = true
				if groupResults[j].count != qspec.count {
					t.Fatalf("Query %d expected count %d for group %s, got %d", i, qspec.count, qspec.group, groupResults[j].count)
				}
			}
		}
		if !gotExpectedGroup {
			t.Fatalf("Query %d results missing group %s", i, qspec.group)
		}
	}
}

func TestTwoGroupQueries(t *testing.T) {

	testQueries := []struct {
		encoded string
		group0  string
		group1  string
		count   int
	}{
		{"time_start=2017-12-05&time_end=2017-12-06&group=condition&group=day_hour", "pto.test.color.red", "14", 758},
		{"time_start=2017-12-05&time_end=2017-12-06&group=condition&group=day_hour&option=count_targets", "pto.test.color.red", "14", 653},
	}

	for i, qspec := range testQueries {

		// verify we're only querying our test set, for repeatability
		encoded := qspec.encoded + fmt.Sprintf("&set=%x", TestQueryCacheSetID)

		// submit query and wait for result
		done := make(chan struct{})
		q, _, err := TestQueryCache.ExecuteQueryFromURLEncoded(encoded, done)
		if err != nil {
			t.Fatal(err)
		}
		<-done

		// verify we think have a result
		if q.Completed == nil {
			t.Fatalf("Query %d did not complete", i)
		}

		// verify the query thinks it completed
		if q.ExecutionError != nil {
			t.Fatalf("Query %d failed: %v", i, q.ExecutionError)
		}

		// load query data from file
		resfile, err := q.ReadResultFile()
		if err != nil {
			t.Fatal(err)
		}
		defer resfile.Close()

		// load query results
		groupResults, err := parseGroupQueryResults(resfile)
		if err != nil {
			t.Fatal(err)
		}

		// search through them to find the group we care about
		gotExpectedGroup := false
		for j := range groupResults {
			if groupResults[j].groups[0] == qspec.group0 && groupResults[j].groups[1] == qspec.group1 {
				gotExpectedGroup = true
				if groupResults[j].count != qspec.count {
					t.Fatalf("Query %d expected count %d for group %s/%s, got %d", i, qspec.count, qspec.group0, qspec.group1, groupResults[j].count)
				}
			}
		}
		if !gotExpectedGroup {
			t.Fatalf("Query %d results missing group %s/%s", i, qspec.group0, qspec.group1)
		}
	}
}
