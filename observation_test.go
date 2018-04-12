package pto3_test

import (
	"testing"

	pto3 "github.com/mami-project/pto3-go"
)

func TestObsetQuery(t *testing.T) {
	setIds, err := pto3.ObservationSetIDsWithMetadata(TestDB, "this_is_the_query_test_obset")

	if err != nil {
		t.Fatal(err)
	}

	if len(setIds) != 1 || setIds[0] != TestQueryCacheSetID {
		t.Fatalf("unexpected result for this_is_the_query_test_obset: %v", setIds)
	}

	setIds, err = pto3.ObservationSetIDsWithMetadataValue(TestDB, "test_obset_type", "query")

	if err != nil {
		t.Fatal(err)
	}

	if len(setIds) != 1 || setIds[0] != TestQueryCacheSetID {
		t.Fatalf("unexpected result for test_obset_type == query: %v", setIds)
	}

	setIds, err = pto3.ObservationSetIDsWithSource(TestDB, "https://localhost:8383/raw/test1/test1-0-obs.ndjson")

	if err != nil {
		t.Fatal(err)
	}

	if len(setIds) != 1 || setIds[0] != TestQueryCacheSetID {
		t.Fatalf("unexpected result for set ID query by source: %v", setIds)
	}

	setIds, err = pto3.ObservationSetIDsWithAnalyzer(TestDB, "https://localhost:8383/query_test_analyzer.json")

	if err != nil {
		t.Fatal(err)
	}

	if len(setIds) != 1 || setIds[0] != TestQueryCacheSetID {
		t.Fatalf("unexpected result for set ID query by analyzer: %v", setIds)
	}

	cidCache, err := pto3.LoadConditionCache(TestDB)
	if err != nil {
		t.Fatalf("condition cache load failed")
	}

	setIds, err = pto3.ObservationSetIDsWithCondition(TestDB, cidCache, "pto.test.color.none_more_black")

	if err != nil {
		t.Fatal(err)
	}

	if len(setIds) < 1 {
		t.Fatalf("no sets found with none_more_black")
	}

}
