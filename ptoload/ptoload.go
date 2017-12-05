// ptoload is a command-line utility to load an observation set file into a PTO observation database.

package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/go-pg/pg"

	pto3 "github.com/mami-project/pto3-go"
)

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "ptoconfig.json", "path to PTO configuration `file` with DB connection information")
var initdbFlag = flag.Bool("initdb", false, "Create database tables on startup")

// extractFirstPass scans a file, getting metadata (in the form of an observation set) and a set of paths
func extractFirstPass(r *os.File) (*pto3.ObservationSet, map[string]struct{}, error) {
	filename := r.Name()

	// create an observation set to hold metadata
	set := pto3.ObservationSet{}

	// and a map to hold the set of paths
	pathSeen := make(map[string]struct{})

	// now scan the file for metadata and paths
	var lineno = 0
	in := bufio.NewScanner(r)
	for in.Scan() {
		lineno++
		line := strings.TrimSpace(in.Text())
		switch line[0] {
		case '{':
			if err := set.UnmarshalJSON([]byte(line)); err != nil {
				return nil, nil, fmt.Errorf("error in metadata at %s line %d: %s", filename, lineno, err.Error())
			}
		case '[':
			var obs []string
			if err := json.Unmarshal([]byte(line), &obs); err != nil {
				return nil, nil, fmt.Errorf("error looking for path at %s line %d: %s", filename, lineno, err.Error())
			}
			if len(obs) < 4 {
				return nil, nil, fmt.Errorf("short observation looking for path at %s line %d", filename, lineno)
			}
			pathSeen[obs[3]] = struct{}{}
		}
	}

	// done
	return &set, pathSeen, nil
}

func cacheConditionIDs(db *pg.DB, set *pto3.ObservationSet) (map[string]int, error) {
	cidCache := make(map[string]int)

	for _, c := range set.Conditions {
		if err := c.InsertOnce(db); err != nil {
			return nil, err
		}

		cidCache[c.Name] = c.ID
	}

	return cidCache, nil
}

func cachePathIDs(db *pg.DB, pathSet map[string]struct{}) (map[string]int, error) {
	pidCache := make(map[string]int)

	for pathstring := range pathSet {
		p := pto3.Path{String: pathstring}

		if err := p.InsertOnce(db); err != nil {
			return nil, err
		}

		pidCache[p.String] = p.ID
	}

	return pidCache, nil
}

func writeObsToCSV(setID int, cidCache map[string]int, pidCache map[string]int, line string, out *csv.Writer) error {
	var jslice []string

	if err := json.Unmarshal([]byte(line), &jslice); err != nil {
		return err
	}

	// add zero value if missing
	if len(jslice) == 5 {
		jslice = append(jslice, "0")
	}

	// replace set ID
	jslice[0] = fmt.Sprintf("%d", setID)

	// replace path string with path ID
	jslice[3] = fmt.Sprintf("%d", pidCache[jslice[3]])

	// replace condition name with condition ID
	jslice[4] = fmt.Sprintf("%d", cidCache[jslice[4]])

	// write as CSV to output writer
	return out.Write(jslice)
}

func loadObservations(cidCache map[string]int, pidCache map[string]int, t *pg.Tx, set *pto3.ObservationSet, r *os.File) error {
	lineno := 0

	dbpipe, obspipe, err := os.Pipe()
	if err != nil {
		return err
	}
	defer dbpipe.Close()

	converr := make(chan error, 1)

	// start a reader goroutine to convert observations to CSV
	// and write them to a pipe we'll COPY FROM
	go func() {
		defer obspipe.Close()

		in := bufio.NewScanner(r)
		out := csv.NewWriter(obspipe)

		for in.Scan() {
			lineno++
			line := strings.TrimSpace(in.Text())
			if line[0] == '[' {
				if err := writeObsToCSV(set.ID, cidCache, pidCache, line, out); err != nil {
					converr <- err
				}
			}
		}
		converr <- nil
	}()

	// now copy from the CSV pipe
	if _, err := t.CopyFrom(dbpipe, "COPY observations (set_id, start_time, end_time, path_id, condition_id, value) FROM STDIN WITH CSV"); err != nil {
		return err
	}

	// wait on the converter goroutine
	return <-converr
}

func loadObservationFile(filename string, db *pg.DB) (*pto3.ObservationSet, error) {

	obsfile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer obsfile.Close()

	set, pathSet, err := extractFirstPass(obsfile)
	if err != nil {
		return nil, err
	}

	cidCache, err := cacheConditionIDs(db, set)
	if err != nil {
		return nil, err
	}

	pidCache, err := cachePathIDs(db, pathSet)
	if err != nil {
		return nil, err
	}

	if _, err := obsfile.Seek(0, 0); err != nil {
		return nil, err
	}

	log.Printf("%s: first pass complete, cached %d conditions and %d paths", filename, len(cidCache), len(pidCache))

	err = db.RunInTransaction(func(t *pg.Tx) error {

		if err := set.Insert(t, true); err != nil {
			return err
		}

		return loadObservations(cidCache, pidCache, t, set, obsfile)
	})

	if err != nil {
		return nil, err
	}

	return set, nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: load observations from a file into a PTO database\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s <flags> input-files\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if *helpFlag {
		flag.Usage()
		os.Exit(1)
	}

	args := flag.Args()

	if len(args) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	config, err := pto3.NewConfigFromFile(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	db := pg.Connect(&config.ObsDatabase)
	if *initdbFlag {
		if err := pto3.CreateTables(db); err != nil {
			log.Fatal(err)
		}
	}

	for _, filename := range args {
		var set *pto3.ObservationSet
		set, err = loadObservationFile(filename, db)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("created observation set %x:", set.ID)
		b, _ := json.MarshalIndent(set, "  ", "  ")
		os.Stderr.Write(b)
	}
}
