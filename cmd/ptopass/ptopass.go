// ptopass is a passthrough normalizer. It reads raw data as observation
// files, producing the same observations on standard out, passing through
// metadata. It is meant to allow the storage of preprocessed observations as
// raw data in the PTO, as well as to provide for self-contained testing of
// the local analysis runtime.

package main

import (
	"bufio"
	"compress/bzip2"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/mami-project/pto3-go"
)

// ObsPassthrough implements a simple passthrough analyzer taking input and metadata on
func ObsPassthrough(in io.Reader, metain io.Reader, out io.Writer) error {

	// unmarshal metadata into an RDS metadata object
	md, err := pto3.RDSMetadataFromReader(metain, nil)
	if err != nil {
		return fmt.Errorf("could not read metadata: %s", err.Error())
	}

	// check filetype and select scanner
	var scanner *bufio.Scanner
	switch md.Filetype() {
	case "obs":
		scanner = bufio.NewScanner(in)
	case "obs-bz2":
		scanner = bufio.NewScanner(bzip2.NewReader(in))
	default:
		return fmt.Errorf("unsupported filetype %s", md.Filetype())
	}

	// track conditions in the input
	hasCondition := make(map[string]bool)

	// now scan input for observations
	var lineno int
	for scanner.Scan() {
		lineno++
		line := strings.TrimSpace(scanner.Text())
		switch line[0] {
		case '{':
			// metadata. ignore.
			continue
		case '[':
			var obs pto3.Observation
			if err := obs.UnmarshalJSON([]byte(line)); err != nil {
				return fmt.Errorf("parse error for observation at line %d: %s", lineno, err.Error())
			}
			hasCondition[obs.Condition.Name] = true
			_, err := fmt.Fprintln(out, line)
			if err != nil {
				return fmt.Errorf("error writing observation: %s", err.Error())
			}
		}
	}

	// selectively pass through metadata
	mdout := make(map[string]interface{})
	mdcond := make([]string, 0)

	// copy all aux metadata from the file
	for k := range md.Metadata {
		mdout[k] = md.Metadata[k]
	}

	// create condition list from observed conditions
	for k := range hasCondition {
		mdcond = append(mdcond, k)
	}
	mdout["_conditions"] = mdcond

	// add start and end time and owner, since we have it
	mdout["_owner"] = md.Owner
	mdout["_time_start"] = md.TimeStart.Format(time.RFC3339)
	mdout["_time_end"] = md.TimeStart.Format(time.RFC3339)

	// hardcode analyzer path (FIXME, tag?)
	mdout["_analyzer"] = "https://github.com/mami-project/pto3-go/tree/master/ptopass/ptopass_analyzer.json"

	// note that we expect the local serialization harness to fill in _sources, so we will leave it blank.

	// serialize and write to stdout
	b, err := json.Marshal(mdout)
	if err != nil {
		return fmt.Errorf("error marshaling metadata: %s", err.Error())
	}

	if _, err := fmt.Fprintf(out, "%s\n", b); err != nil {
		return fmt.Errorf("error writing metadata: %s", err.Error())
	}

	return nil
}

func main() {
	// wrap a file around the metadata stream
	mdfile := os.NewFile(3, ".piped_metadata.json")

	// and go
	if err := ObsPassthrough(os.Stdin, mdfile, os.Stdout); err != nil {
		log.Fatal(err)
	}

}
