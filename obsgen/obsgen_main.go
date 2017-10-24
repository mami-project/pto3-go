package main

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"
)

func randomPath(sourcePrefixes []string, targetPrefixes []string) string {

	sourcePrefix := sourcePrefixes[rand.Intn(len(sourcePrefixes))]
	targetPrefix := sourcePrefixes[rand.Intn(len(targetPrefixes))]

	return fmt.Sprintf("%s%d * %s%d", sourcePrefix, rand.Intn(256), targetPrefix, rand.Intn(256))
}

func randomTime(base time.Time) (time.Time, time.Time) {
	duration := rand.Int63n(1000000000 * 2)
	offset := rand.Int63n(1000000000)

	return base.Add(time.Duration(duration)), base.Add(time.Duration(offset))
}

func generateTestObservations(count int, out io.Writer) {

	// define condition prevalences
	conditionPrevalence := map[string]int{
		"pto.test.color.red":             8,
		"pto.test.color.orange":          7,
		"pto.test.color.yellow":          6,
		"pto.test.color.green":           5,
		"pto.test.color.blue":            4,
		"pto.test.color.indigo":          3,
		"pto.test.color.violet":          2,
		"pto.test.color.none_more_black": 1,
	}

	// and create a condition die we can roll
	conditions := make([]string, 0)
	for k, v := range conditionPrevalence {
		for i := 0; i < v; i++ {
			conditions = append(conditions, k)
		}
	}

	// some source prefixes
	sourcePrefixes := []string{
		"10.1.2.",
		"10.3.4.",
		"10.5.6.",
		"10.7.8.",
		"10.9.10.",
	}

	// some destination prefixes
	targetPrefixes := []string{
		"10.11.12.",
		"10.13.14.",
		"10.15.16.",
		"10.17.18.",
		"10.19.20.",
	}

	// start the clock
	clock := time.Now().UTC()
	start := clock
	end := clock

	// now emit some observations as ndjson
	for i := 0; i < count; i++ {

		// make a random path
		path := randomPath(sourcePrefixes, targetPrefixes)

		// pick a random condition
		condition := conditions[rand.Intn(len(conditions))]

		// get start and end times and advance the clock
		start = clock
		end, clock = randomTime(clock)

		// now print a row
		fmt.Fprintf(out, "[0, \"%s\", \"%s\", \"%s\", \"%s\"]\n",
			start.Format(time.RFC3339), end.Format(time.RFC3339), path, condition)
	}

}

func main() {
	generateTestObservations(7200, os.Stdout)
}
