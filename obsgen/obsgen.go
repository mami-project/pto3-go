package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"
)

func randomTime(base time.Time) (time.Time, time.Time) {
	duration := rand.Int63n(1000000000 * 2)
	offset := rand.Int63n(1000000000)

	return base.Add(time.Duration(duration)), base.Add(time.Duration(offset))
}

type testObservationSpec struct {
	SourceIP4           string
	SourceIP6           string
	IP4Bias             int
	Target4Prefixes     []string
	Target6Prefixes     []string
	ConditionPrevalence map[string]int
}

func generateTestObservations(spec *testObservationSpec, count int, out io.Writer) {

	// generate a condition die we can roll
	conditions := make([]string, 0)
	for k, v := range spec.ConditionPrevalence {
		for i := 0; i < v; i++ {
			conditions = append(conditions, k)
		}
	}

	// start the clock
	clock := time.Now().UTC()
	start := clock
	end := clock

	// now emit some observations as ndjson
	for i := 0; i < count; i++ {

		// generate v4 or v6 path
		var path string
		if rand.Intn(256) < spec.IP4Bias {
			path = fmt.Sprintf("%s * %s%d",
				spec.SourceIP4,
				spec.Target4Prefixes[rand.Intn(len(spec.Target4Prefixes))],
				rand.Intn(256))
		} else {
			path = fmt.Sprintf("%s * %s%x",
				spec.SourceIP6,
				spec.Target6Prefixes[rand.Intn(len(spec.Target6Prefixes))],
				rand.Intn(65536))
		}

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

	spec := testObservationSpec{
		IP4Bias: 200,
		Target4Prefixes: []string{
			"10.11.12.",
			"10.13.14.",
			"10.15.16.",
			"10.17.18.",
			"10.19.20.",
		},
		Target6Prefixes: []string{
			"2001:db8:82:83::",
			"2001:db8:84:8a::",
		},
		ConditionPrevalence: map[string]int{
			"pto.test.color.red":             8,
			"pto.test.color.orange":          7,
			"pto.test.color.yellow":          6,
			"pto.test.color.green":           5,
			"pto.test.color.blue":            4,
			"pto.test.color.indigo":          3,
			"pto.test.color.violet":          2,
			"pto.test.color.none_more_black": 1,
		},
	}

	sources4 := []string{
		"10.33.44.55",
		"10.33.44.66",
		"10.33.44.77",
		"10.33.44.88",
		"10.33.44.99",
		"10.33.44.121",
	}
	sources6 := []string{
		"2001:db8:e55:5::",
		"2001:db8:e66:6::",
		"2001:db8:e77:7::",
		"2001:db8:e88:8::",
		"2001:db8:e99:9::",
		"2001:db8:eaa:a::",
	}

	for i := range sources4 {
		spec.SourceIP4 = sources4[i]
		spec.SourceIP6 = sources6[i]
		file, err := os.Create(fmt.Sprintf("testdata/7200_testobs_%d.ndjson", i))
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()
		generateTestObservations(&spec, 7200, file)
	}

}
