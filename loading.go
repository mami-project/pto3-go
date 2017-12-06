package pto3

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
	"github.com/go-pg/pg/orm"
)

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "ptoconfig.json", "path to PTO configuration `file` with DB connection information")
var initdbFlag = flag.Bool("initdb", false, "Create database tables on startup")

// extractFirstPass scans a file, getting metadata (in the form of an observation set) and a set of paths
func extractFirstPass(r *os.File) (*ObservationSet, map[string]struct{}, error) {
	filename := r.Name()

	// create an observation set to hold metadata
	set := ObservationSet{}

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

func cacheConditionIDs(db orm.DB, set *ObservationSet) (map[string]int, error) {
	cidCache := make(map[string]int)

	for _, c := range set.Conditions {
		if err := c.InsertOnce(db); err != nil {
			return nil, err
		}

		cidCache[c.Name] = c.ID
	}

	return cidCache, nil
}

// PathCache maps a path string to a path ID
type PathCache map[string]int

// CacheNewPaths takes a set of path names, and adds those not already
// appearing to the cache and the underlying database. It modifies the pathSet
// to contain only those paths added.
func (cache PathCache) CacheNewPaths(db orm.DB, pathSet map[string]struct{}) error {
	// first, reduce to paths not already in the cache
	for ps := range pathSet {
		if cache[ps] > 0 {
			delete(pathSet, ps)
		}
	}

	// allocate a range of IDs in the database
	var nv struct {
		Nextval int
	}

	if _, err := db.QueryOne(&nv, "SELECT nextval('paths_id_seq')"); err != nil {
		return err
	}
	pidseq := nv.Nextval

	if _, err := db.Exec("SELECT setval('paths_id_seq', ?)", pidseq+len(pathSet)); err != nil {
		return err
	}

	// now add entries to the path cache while streaming into the database
	streamerr := make(chan error, 1)
	dbpipe, pathpipe, err := os.Pipe()
	if err != nil {
		return err
	}
	defer dbpipe.Close()

	go func() {
		out := csv.NewWriter(pathpipe)
		defer pathpipe.Close()

		for pathstring := range pathSet {
			p := []string{fmt.Sprintf("%d", pidseq), pathstring}
			cache[pathstring] = pidseq

			if err := out.Write(p); err != nil {
				streamerr <- err
			}

			pidseq++
		}

		out.Flush()
		streamerr <- nil
	}()

	// copy from the goroutine to the database
	if _, err = db.CopyFrom(dbpipe, "COPY paths (id, string) FROM STDIN WITH CSV"); err != nil {
		return err
	}

	// wait for goroutine to complete and return its error
	return <-streamerr
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

func loadObservations(cidCache map[string]int, pidCache map[string]int, t *pg.Tx, set *ObservationSet, r *os.File) error {
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
		in := bufio.NewScanner(r)
		out := csv.NewWriter(obspipe)
		defer obspipe.Close()

		for in.Scan() {
			lineno++
			line := strings.TrimSpace(in.Text())
			if line[0] == '[' {
				if err := writeObsToCSV(set.ID, cidCache, pidCache, line, out); err != nil {
					converr <- err
				}
			}
		}
		out.Flush()
		converr <- nil
	}()

	// now copy from the CSV pipe
	if _, err := t.CopyFrom(dbpipe, "COPY observations (set_id, start_time, end_time, path_id, condition_id, value) FROM STDIN WITH CSV"); err != nil {
		return err
	}

	// wait on the converter goroutine
	return <-converr
}

// LoadObservationFile loads an observation file into a database, using a given cache to cache path IDs.
func LoadObservationFile(filename string, db *pg.DB, pidCache PathCache) (*ObservationSet, error) {

	obsfile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer obsfile.Close()

	set, pathSet, err := extractFirstPass(obsfile)
	if err != nil {
		return nil, err
	}

	if _, err := obsfile.Seek(0, 0); err != nil {
		return nil, err
	}

	err = db.RunInTransaction(func(t *pg.Tx) error {

		cidCache, err := cacheConditionIDs(t, set)
		if err != nil {
			return err
		}
		log.Printf("%s: first pass cached %d conditions", filename, len(cidCache))

		if err := pidCache.CacheNewPaths(t, pathSet); err != nil {
			return err
		}

		log.Printf("%s: first pass cached %d paths", filename, len(pidCache))

		if err := set.Insert(t, true); err != nil {
			return err
		}
		log.Printf("%s: allocated set id %x, loading observations", filename, set.ID)

		return loadObservations(cidCache, pidCache, t, set, obsfile)
	})

	if err != nil {
		return nil, err
	}

	return set, nil
}
