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

}
