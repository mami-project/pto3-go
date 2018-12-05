package pto3

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
)

// Observation data model for PTO3 obs and query
// including PostgreSQL object-relational mapping

// ObservationSet represents an PTO observation set and its metadata.
type ObservationSet struct {
	// Observation set ID in the database
	ID int
	// Array of source URLs, from _sources metadata key
	Sources []string `pg:",array"`
	// Analyzer metadata URL, from _analyzer metadata key
	Analyzer string
	// Conditions declared to appear in this observation set,
	Conditions []Condition `pg:",many2many:observation_set_conditions"`
	// Arbitrary metadata
	Metadata map[string]string
	// Metadata creation timestamp
	Created *time.Time
	// Metadata modification timestamp
	Modified *time.Time
	// Cached row count
	Count int
	// Cached observation start time
	TimeStart *time.Time
	// Cached observation end time
	TimeEnd *time.Time
	// system metadata
	datalink string
	link     string
}

// ObservationSetCondition implements a linking table between observation sets
// and conditions appearing therein.
type ObservationSetCondition struct {
	ObservationSetID int
	ConditionID      int
}

// MarshalJSON serializes this ObservationSet into a JSON observation set metadata
// object suitable for use with the PTO API or as a line in an Observation Set
// File.
func (set *ObservationSet) MarshalJSON() ([]byte, error) {
	jmap := make(map[string]interface{})

	jmap["_sources"] = set.Sources
	jmap["_analyzer"] = set.Analyzer

	if set.link != "" {
		jmap["__link"] = set.link
	}

	if set.datalink != "" {
		jmap["__data"] = set.datalink
	}

	if set.Count != 0 {
		jmap["__obs_count"] = set.Count
	}

	if set.TimeStart != nil {
		jmap["__time_start"] = set.TimeStart
	}

	if set.TimeEnd != nil {
		jmap["__time_end"] = set.TimeEnd
	}

	if set.Created != nil {
		jmap["__created"] = set.Created.Format(time.RFC3339)
	}

	if set.Modified != nil {
		jmap["__modified"] = set.Modified.Format(time.RFC3339)
	}

	conditionNames := make([]string, len(set.Conditions))
	for i := range set.Conditions {
		conditionNames[i] = set.Conditions[i].Name
	}
	if len(conditionNames) > 0 {
		jmap["_conditions"] = conditionNames
	}

	for k, v := range set.Metadata {
		jmap[k] = v
	}

	return json.Marshal(jmap)
}

// UnmarshalJSON fills in an ObservationSet from a JSON observation set
// metadata object suitable for use with the PTO API.
func (set *ObservationSet) UnmarshalJSON(b []byte) error {
	set.Metadata = make(map[string]string)

	var jmap map[string]interface{}
	err := json.Unmarshal(b, &jmap)
	if err != nil {
		return PTOWrapError(err)
	}

	// zero ID, it will be assigned on insertion or from the URI
	set.ID = 0

	var ok bool
	for k, v := range jmap {
		if k == "_sources" {
			set.Sources, ok = AsStringArray(v)
			if !ok {
				return PTOErrorf("_sources not a string array")
			}
		} else if k == "_analyzer" {
			set.Analyzer = AsString(v)
		} else if k == "_conditions" {
			// Create new condition objects with name only and zero ID.
			// Caller will have to fill in condition names and create many2many links.
			conditionNames, ok := AsStringArray(v)
			if !ok {
				return PTOErrorf("_conditions not a string array")
			}
			set.Conditions = make([]Condition, len(conditionNames))
			for i := range conditionNames {
				set.Conditions[i] = *NewCondition(conditionNames[i])
			}
		} else if k == "__link" {
			set.link = AsString(v)
		} else if k == "__data_link" {
			set.datalink = AsString(v)
		} else if strings.HasPrefix(k, "__") {
			// Ignore all other incoming __ keys instead of stuffing them in metadata
		} else {
			// Everything else is metadata
			set.Metadata[k] = AsString(v)
		}
	}

	// make sure we got values for everything
	if set.Sources == nil {
		return PTOErrorf("ObservationSet missing _sources")
	}

	if set.Analyzer == "" {
		return PTOErrorf("ObservationSet missing _analyzer")
	}

	if set.Conditions == nil {
		return PTOErrorf("ObservationSet missing _conditions")
	}

	return nil
}

func (set *ObservationSet) ensureConditionsInDB(db orm.DB) error {
	for i := range set.Conditions {
		if err := set.Conditions[i].InsertOnce(db); err != nil {
			return err
		}
	}
	return nil
}

// Insert inserts an ObservationSet into the database. A row is inserted if
// the observation set has not already been inserted (i.e., has no ID) or if
// the force flag is set.
func (set *ObservationSet) Insert(db orm.DB, force bool) error {
	if force {
		set.ID = 0
	}

	if set.ID == 0 {
		// set creation and modification timestamps
		ctime := time.Now().UTC()
		set.Created = &ctime
		set.Modified = &ctime

		// ensure conditions have IDs
		if err := set.ensureConditionsInDB(db); err != nil {
			log.Printf("error ensuring condition is in DB: %v", err)
			return err
		}

		// main insertion
		if err := db.Insert(set); err != nil {
			log.Printf("error inserting set: %v", err)
			return PTOWrapError(err)
		}

		// TODO file a bug against go-pg or its docs: this should be automatic.
		for i := range set.Conditions {
			_, err := db.Exec("INSERT INTO observation_set_conditions VALUES (?, ?)", set.ID, set.Conditions[i].ID)
			if err != nil {
				log.Printf("error on INSERT INTO observation_set_conditions: %v", err)
				return PTOWrapError(err)
			}
		}
	}
	return nil
}

// SelectByID selects values for this ObservationSet from the database by its
// ID. FIXME the implicit contract for this function allows it to be compared
// against pg.ErrNoRows, so its error output cannot (yet) be wrapped.
func (set *ObservationSet) SelectByID(db orm.DB) error {

	if err := db.Model(set).Column("observation_set.*").Where("id = ?", set.ID).First(); err != nil {
		return err
	}

	var conditionIDs []int
	err := db.Model(&ObservationSetCondition{}).
		ColumnExpr("array_agg(condition_id)").
		Where("observation_set_id = ?", set.ID).Select(pg.Array(&conditionIDs))
	if err != nil {
		return err
	}

	//log.Printf("set ID %x has conditions %v", set.ID, conditionIDs)

	set.Conditions = make([]Condition, len(conditionIDs))
	for i := range conditionIDs {
		set.Conditions[i].ID = conditionIDs[i]
		if err := set.Conditions[i].SelectByID(db); err != nil {
			// FIXME a miss here will throw a pg.ErrNoRows, which will look
			// to client code like a 404. Figure out how to fix this.
			// For now, rebuild conditions manually.
			return err
		}
	}

	return nil
}

// Update updates this ObservationSet in the database by overwriting the DB's
// values with its own, by ID.
func (set *ObservationSet) Update(db orm.DB) error {
	// set modified timestamp
	mtime := time.Now().UTC()
	set.Modified = &mtime

	// ensure new conditions are in the database
	if err := set.ensureConditionsInDB(db); err != nil {
		return err
	}

	// main update
	if err := db.Update(set); err != nil {
		return PTOWrapError(err)
	}

	// now delete and restore conditions
	_, err := db.Exec("DELETE FROM observation_set_conditions WHERE observation_set_id = ?", set.ID)
	if err != nil {
		return PTOWrapError(err)
	}

	for i := range set.Conditions {
		_, err := db.Exec("INSERT INTO observation_set_conditions VALUES (?, ?)", set.ID, set.Conditions[i].ID)
		if err != nil {
			return PTOWrapError(err)
		}
	}

	return nil
}

// LinkForSetID generates a link from given PTO configuration and a set ID. Observation set
// links are given by set ID as a hexadecimal string.
func LinkForSetID(config *PTOConfiguration, setid int) string {
	out, _ := config.LinkTo(fmt.Sprintf("obs/%x", setid))
	return out
}

// LinkVia sets this ObservationSet's link and datalink given a configuration
func (set *ObservationSet) LinkVia(config *PTOConfiguration) {
	set.link = LinkForSetID(config, set.ID)
	set.datalink = set.link + "/data"
}

func (set *ObservationSet) Link() string {
	return set.link
}

func (set *ObservationSet) TimeInterval(db orm.DB) (*time.Time, *time.Time, error) {
	if set.TimeStart == nil || set.TimeEnd == nil {
		// No start time or end time. Do we have any observations?
		obscount, err := set.CountObservations(db)
		if err != nil {
			return nil, nil, err
		}

		if obscount == 0 {
			// no, we don't
			return nil, nil, nil
		}

		// No start time or end time. Try to select from database.
		err = db.Model(&Observation{}).ColumnExpr("min(time_start)").Where("set_id = ?", set.ID).Select(&set.TimeStart)
		if err != nil {
			return nil, nil, PTOWrapError(err)
		}

		err = db.Model(&Observation{}).ColumnExpr("max(time_end)").Where("set_id = ?", set.ID).Select(&set.TimeEnd)
		if err != nil {
			return nil, nil, PTOWrapError(err)
		}

		// If we actually updated the time range, cache it by doing a simple update
		if set.TimeStart != nil && set.TimeEnd != nil {
			if err = db.Update(set); err != nil {
				return nil, nil, PTOWrapError(err)
			}
		}
	}

	return set.TimeStart, set.TimeEnd, nil
}

// CountObservations counts observations in the database for this ObservationSet,
// caching the result and storing it in the database if appropriate
func (set *ObservationSet) CountObservations(db orm.DB) (int, error) {
	var err error
	if set.Count == 0 {
		set.Count, err = db.Model(&Observation{}).Where("set_id = ?", set.ID).Count()
		if err != nil {
			return 0, PTOWrapError(err)
		}

		// if we actually updated the count, cache it by doing a simple update
		if set.Count != 0 {
			if err = db.Update(set); err != nil {
				return 0, PTOWrapError(err)
			}
		}
	}
	return set.Count, nil
}

func (set *ObservationSet) verifyConditionSet(conditionNames map[string]struct{}) error {
	// make a set condition names declared in the condition set
	conditionDeclared := make(map[string]struct{})
	for _, c := range set.Conditions {
		conditionDeclared[c.Name] = struct{}{}
	}

	// look for name in condition names not declared in the set, raise error if so
	for conditionName := range conditionNames {
		if _, ok := conditionDeclared[conditionName]; !ok {
			return PTOErrorf("observation has condition %s not declared in set", conditionName)
		}
	}

	return nil
}

// Observation represents a single observation, within an observation set
type Observation struct {
	ID          int `sql:",pk"`
	SetID       int
	Set         *ObservationSet
	TimeStart   *time.Time
	TimeEnd     *time.Time
	PathID      int
	Path        *Path
	ConditionID int
	Condition   *Condition
	Value       string
}

// MarshalJSON turns this Observation into a JSON array suitable for use as a
// line in an observation file.
func (obs *Observation) MarshalJSON() ([]byte, error) {
	jslice := []string{
		fmt.Sprintf("%x", obs.SetID),
		obs.TimeStart.UTC().Format(time.RFC3339),
		obs.TimeEnd.UTC().Format(time.RFC3339),
		obs.Path.String,
		obs.Condition.Name,
	}

	if obs.Value != "" {
		jslice = append(jslice, fmt.Sprintf("%s", obs.Value))
	}

	return json.Marshal(&jslice)
}

// unmarshalStringSlice fills in this observation from a string slice. This is used by both JSON unmarshaling and CSV unmarshaling (in CopyDataToStream)
func (obs *Observation) unmarshalStringSlice(jslice []string, time_format string) error {

	obs.ID = 0
	if len(jslice[0]) > 0 {
		setid, err := strconv.ParseUint(jslice[0], 16, 64) // fill in Set ID, will be ignored by force insert
		if err != nil {
			return PTOWrapError(err)
		}
		obs.SetID = int(setid)
	} else {
		obs.SetID = 0
	}

	starttime, err := time.Parse(time_format, jslice[1])
	if err != nil {
		return PTOWrapError(err)
	}
	obs.TimeStart = &starttime

	endtime, err := time.Parse(time_format, jslice[2])
	if err != nil {
		return PTOWrapError(err)
	}
	obs.TimeEnd = &endtime

	obs.Path = &Path{String: jslice[3]}
	obs.Path.Parse()

	obs.Condition = NewCondition(jslice[4])

	if len(jslice) >= 6 {
		obs.Value = jslice[5]
		if err != nil {
			return err
		}
	}

	return nil
}

// UnmarshalJSON fills in this Observation from a JSON array line in an
// observation file.
func (obs *Observation) UnmarshalJSON(b []byte) error {
	var jslice []string

	err := json.Unmarshal(b, &jslice)
	if err != nil {
		return err
	}

	if len(jslice) < 5 {
		return PTOErrorf("Observation requires at least five elements")
	}

	return obs.unmarshalStringSlice(jslice, time.RFC3339)

}

// CreateTables insures that the tables used by the ORM exist in the given
// database. This is used for testing, and the (not yet implemented) ptodb init
// command.
func CreateTables(db *pg.DB) error {
	opts := orm.CreateTableOptions{
		IfNotExists:   true,
		FKConstraints: true,
	}

	// Create functions first.
	// EWW EWW
	// PostgreSQL doesn't have IF NOT EXISTS for these,
	// so just run them and ignore the error, since any error other than
	// "exists" will probably cause later failures in the subsequent
	// EWW EWW
	db.Exec("create function like_rev (text, text) returns boolean as $$ select $2 like $1 $$ language SQL")
	db.Exec("create operator ~~~~ (procedure = like_rev,  leftarg=text, rightarg=text)")

	return db.RunInTransaction(func(tx *pg.Tx) error {
		if err := db.CreateTable(&Condition{}, &opts); err != nil {
			return PTOWrapError(err)
		}

		if err := db.CreateTable(&Path{}, &opts); err != nil {
			return PTOWrapError(err)
		}

		if err := db.CreateTable(&ObservationSet{}, &opts); err != nil {
			return PTOWrapError(err)
		}

		if err := db.CreateTable(&ObservationSetCondition{}, &opts); err != nil {
			return PTOWrapError(err)
		}

		if err := db.CreateTable(&Observation{}, &opts); err != nil {
			return PTOWrapError(err)
		}

		// index to select observations by set ID
		if _, err := db.Exec("CREATE INDEX ON observations (set_id)"); err != nil {
			return PTOWrapError(err)
		}

		return nil
	})
}

// DropTables removes the tables used by the ORM from the database. Use this for
// testing only, please.
func DropTables(db *pg.DB) error {
	return db.RunInTransaction(func(tx *pg.Tx) error {
		if err := db.DropTable(&Observation{}, nil); err != nil {
			return PTOWrapError(err)
		}

		if err := db.DropTable(&ObservationSetCondition{}, nil); err != nil {
			return PTOWrapError(err)
		}

		if err := db.DropTable(&ObservationSet{}, nil); err != nil {
			return PTOWrapError(err)
		}

		if err := db.DropTable(&Condition{}, nil); err != nil {
			return PTOWrapError(err)
		}

		if err := db.DropTable(&Path{}, nil); err != nil {
			return PTOWrapError(err)
		}

		return nil
	})
}

func EnableQueryLogging(db *pg.DB) {
	db.OnQueryProcessed(func(event *pg.QueryProcessedEvent) {
		query, err := event.FormattedQuery()
		if err != nil {
			panic(err)
		}

		log.Printf("%s %s", time.Since(event.StartTime), query)
	})
}

func WriteObservations(obsdat []Observation, out io.Writer) error {
	for _, obs := range obsdat {
		b, err := json.Marshal(&obs)
		if err != nil {
			return PTOWrapError(err)
		}
		_, err = out.Write(b)
		if err != nil {
			return PTOWrapError(err)
		}
		_, err = out.Write([]byte("\n"))
		if err != nil {
			return PTOWrapError(err)
		}
	}
	return nil
}

// obsFileFirstPass scans a file, getting metadata (in the form of an observation set), a set of paths, and a set of conditions
func obsFileFirstPass(r *os.File) (*ObservationSet, map[string]struct{}, map[string]struct{}, error) {
	filename := r.Name()

	// create an observation set to hold metadata
	set := ObservationSet{}

	// and maps to hold paths and conditions
	pathSeen := make(map[string]struct{})
	conditionSeen := make(map[string]struct{})

	// now scan the file for metadata, paths, and conditions
	var lineno = 0
	in := bufio.NewScanner(r)
	for in.Scan() {
		lineno++
		line := strings.TrimSpace(in.Text())
		switch line[0] {
		case '{':
			if err := set.UnmarshalJSON([]byte(line)); err != nil {
				return nil, nil, nil, PTOErrorf("error in metadata at %s line %d: %s", filename, lineno, err.Error())
			}
		case '[':
			var obs []string
			if err := json.Unmarshal([]byte(line), &obs); err != nil {
				return nil, nil, nil, PTOErrorf("error looking for path at %s line %d: %s", filename, lineno, err.Error())
			}
			if len(obs) < 4 {
				return nil, nil, nil, PTOErrorf("short observation looking for path at %s line %d", filename, lineno)
			}
			pathSeen[obs[3]] = struct{}{}
			conditionSeen[obs[4]] = struct{}{}
		}
	}

	// done
	return &set, pathSeen, conditionSeen, nil
}

// writeObsToCSV writes an unparsed observation to a CSV writer, for COPY FROM
// loading of observations into a PostgreSQL table.
func writeObsToCSV(
	set *ObservationSet,
	cidCache ConditionCache,
	pidCache PathCache,
	line string,
	out *csv.Writer) error {

	var jslice []string

	if err := json.Unmarshal([]byte(line), &jslice); err != nil {
		return err
	}

	// add zero value if missing
	if len(jslice) == 5 {
		jslice = append(jslice, "0")
	}

	// replace set ID
	jslice[0] = fmt.Sprintf("%d", set.ID)

	// replace path string with path ID
	id, _ := pidCache.IDFromName(jslice[3])
	jslice[3] = fmt.Sprintf("%d", id)

	// replace condition name with condition ID
	id, _ = cidCache.IDFromName(jslice[4])
	jslice[4] = fmt.Sprintf("%d", id)

	// write as CSV to output writer
	return out.Write(jslice)
}

func loadObservations(
	cidCache ConditionCache,
	pidCache PathCache,
	t *pg.Tx,
	set *ObservationSet,
	r *os.File) error {

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
				if err := writeObsToCSV(set, cidCache, pidCache, line, out); err != nil {
					converr <- PTOWrapError(err)
				}
			}
		}
		out.Flush()
		converr <- nil
	}()

	// now copy from the CSV pipe
	if _, err := t.CopyFrom(dbpipe, "COPY observations (set_id, time_start, time_end, path_id, condition_id, value) FROM STDIN WITH CSV"); err != nil {
		return PTOWrapError(err)
	}

	// wait on the converter goroutine
	return <-converr
}

// CopySetFromObsFile loads an observation file from a local path into the
// database. It uses given caches to cache condition and path IDs, and creates the
// ObservationSet from the metadata found in the file. This is used by ptoload
// to load observation sets created by local analysis into the database.
func CopySetFromObsFile(
	filename string,
	db *pg.DB,
	cidCache ConditionCache,
	pidCache PathCache) (*ObservationSet, error) {

	obsfile, err := os.Open(filename)
	if err != nil {
		log.Printf("can't open \"%s\": %v", filename, err)
		return nil, err
	}
	defer obsfile.Close()

	// first pass: extract paths, conditions, and metadata
	set, pathSet, conditionSet, err := obsFileFirstPass(obsfile)
	if err != nil {
		log.Printf("error on first pass of \"%s\": %v", filename, err)
		return nil, err
	}

	// ensure every condition is declared
	if err := set.verifyConditionSet(conditionSet); err != nil {
		log.Printf("error on verifying conditions of \"%s\": %v", filename, err)
		return nil, err
	}

	// now rewind for a second pass
	if _, err := obsfile.Seek(0, 0); err != nil {
		log.Printf("error on rewinding \"%s\": %v", filename, err)
		return nil, PTOWrapError(err)
	}

	// spin up a transaction
	err = db.RunInTransaction(func(t *pg.Tx) error {

		// make sure conditions are inserted
		if err := cidCache.FillConditionIDsInSet(t, set); err != nil {
			log.Printf("error on filling condition IDs of \"%s\": %v", filename, err)
			return err
		}

		// make sure paths are inserted
		if err := pidCache.CacheNewPaths(t, pathSet); err != nil {
			log.Printf("error on inserting paths of \"%s\": %v", filename, err)
			return err
		}

		// insert the set
		if err := set.Insert(t, true); err != nil {
			log.Printf("error on inserting set of \"%s\": %v", filename, err)
			return err
		}

		// now insert the observations
		if err := loadObservations(cidCache, pidCache, t, set, obsfile); err != nil {
			log.Printf("error on loading observations of \"%s\": %v", filename, err)
			return err
		}

		// Force the observation set count and time interval to update
		if _, err := set.CountObservations(t); err != nil {
			log.Printf("error on counting observations of \"%s\": %v", filename, err)
			return err
		}

		_, _, err := set.TimeInterval(t)
		if err != nil {
			log.Printf("error on setting time interval of \"%s\": %v", filename, err)
		}
		return err
	})

	if err != nil {
		log.Printf("error on running transaction for \"%s\": %v", filename, err)
		return nil, err
	}

	return set, nil
}

// CopyDataFromObsFile loads an observation file from a local path into the
// database. It requires an ObservationSet to already exist in the database.
// It uses given caches to cache condition and path IDs, and checks conditions
// against those declared. This is used by ptoload to load observation sets
// created by local analysis into the database.
func CopyDataFromObsFile(
	filename string,
	db *pg.DB, set *ObservationSet,
	cidCache ConditionCache,
	pidCache PathCache) error {

	obsfile, err := os.Open(filename)
	if err != nil {
		return PTOWrapError(err)
	}
	defer obsfile.Close()

	// first pass: extract paths and conditions
	_, pathSet, conditionSet, err := obsFileFirstPass(obsfile)
	if err != nil {
		return err
	}

	// ensure every condition is declared
	if err := set.verifyConditionSet(conditionSet); err != nil {
		return err
	}

	// now rewind for a second pass
	if _, err := obsfile.Seek(0, 0); err != nil {
		return PTOWrapError(err)
	}

	// spin up a transaction
	return db.RunInTransaction(func(t *pg.Tx) error {

		// make sure paths are inserted
		if err := pidCache.CacheNewPaths(t, pathSet); err != nil {
			return err
		}

		// now insert the observations
		return loadObservations(cidCache, pidCache, t, set, obsfile)
	})
}

// CopyDataToStream copies all the observations in this observation set in
// observation file format to the given stream
func (set *ObservationSet) CopyDataToStream(db orm.DB, out io.Writer) error {

	// create some pipes
	obspipe, dbpipe, err := os.Pipe()
	if err != nil {
		return PTOWrapError(err)
	}
	defer dbpipe.Close()

	converr := make(chan error, 1)

	// wrap a CSV reader around the read side
	in := csv.NewReader(obspipe)

	// COPY TO STDOUT doesn't seem to close the pipe, so we need to know when to stop.
	obscount, err := set.CountObservations(db)
	if err != nil {
		return err
	}

	// set up goroutine to parse observations and dump them to the writer as JSON
	go func() {
		defer obspipe.Close()
		var obs Observation
		i := 0
		for {
			cslice, err := in.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				converr <- PTOWrapError(err)
				return
			}

			if err := obs.unmarshalStringSlice(cslice, PostgresTime); err != nil {
				converr <- err
				return
			}

			b, err := obs.MarshalJSON()
			if err != nil {
				converr <- err
				return
			}

			if _, err := fmt.Fprintf(out, "%s\n", b); err != nil {
				converr <- PTOWrapError(err)
				return
			}

			i++
			if i >= obscount {
				converr <- nil
				return
			}
		}

		converr <- nil
	}()

	// now kick off a copy query
	if _, err := db.CopyTo(dbpipe, "COPY (SELECT set_id, time_start, time_end, string, name, value from observations JOIN conditions ON conditions.id = observations.condition_id JOIN paths ON paths.id = observations.path_id WHERE set_id = ?) TO STDOUT WITH CSV", set.ID); err != nil {
		return PTOWrapError(err)
	}

	// and wait for the copy goroutine to finish
	return <-converr
}

// AllObservationSetIDs lists all observation set IDs in the database.
func AllObservationSetIDs(db orm.DB) ([]int, error) {
	var setIds []int

	err := db.Model(&ObservationSet{}).ColumnExpr("array_agg(id)").Select(pg.Array(&setIds))
	if err == pg.ErrNoRows {
		return make([]int, 0), nil
	} else if err != nil {
		return nil, PTOWrapError(err)
	}

	sort.Slice(setIds, func(i, j int) bool { return setIds[i] < setIds[j] })

	return setIds, nil
}

// ObservationSetIDsWithMetadata lists all observation set IDs in the database
// where a given metadata key is present.
func ObservationSetIDsWithMetadata(db orm.DB, k string) ([]int, error) {
	var setIds []int

	err := db.Model(&ObservationSet{}).ColumnExpr("array_agg(id)").Where("metadata->? IS NOT NULL", k).Select(pg.Array(&setIds))
	if err == pg.ErrNoRows {
		return make([]int, 0), nil
	} else if err != nil {
		return nil, PTOWrapError(err)
	}

	sort.Slice(setIds, func(i, j int) bool { return setIds[i] < setIds[j] })

	return setIds, nil
}

// ObservationSetIDsWithMetadataValue lists all observation set IDs in the
// database where a given metadata key has a given value.
func ObservationSetIDsWithMetadataValue(db orm.DB, k string, v string) ([]int, error) {
	var setIds []int

	err := db.Model(&ObservationSet{}).
		ColumnExpr("array_agg(id)").
		Where("metadata->? = ?", k, fmt.Sprintf("\"%s\"", v)).
		Select(pg.Array(&setIds))
	if err == pg.ErrNoRows {
		return make([]int, 0), nil
	} else if err != nil {
		return nil, PTOWrapError(err)
	}

	sort.Slice(setIds, func(i, j int) bool { return setIds[i] < setIds[j] })

	return setIds, nil
}

// ObservationSetIDsWithSource lists all observation set IDs in the database
// where the given source is present in the sources list. The source must be
// given as a fully qualified analyzer URL.
func ObservationSetIDsWithSource(db orm.DB, source string) ([]int, error) {
	var setIds []int

	err := db.Model(&ObservationSet{}).
		ColumnExpr("array_agg(id)").
		Where("? ~~~~ ANY(sources)", source+"%").
		//		Where("? = ANY(sources)", source).
		Select(pg.Array(&setIds))
	if err == pg.ErrNoRows {
		return make([]int, 0), nil
	} else if err != nil {
		return nil, PTOWrapError(err)
	}

	sort.Slice(setIds, func(i, j int) bool { return setIds[i] < setIds[j] })

	return setIds, nil
}

// ObservationSetIDsWithAnalyzer lists all observation set IDs in the
// database with a given analyzer. The analyzer must be given as a fully qualified analyzer URL.
func ObservationSetIDsWithAnalyzer(db orm.DB, analyzer string) ([]int, error) {
	var setIds []int

	err := db.Model(&ObservationSet{}).
		ColumnExpr("array_agg(id)").
		Where("analyzer LIKE ?", analyzer+"%").
		Select(pg.Array(&setIds))
	if err == pg.ErrNoRows {
		return make([]int, 0), nil
	} else if err != nil {
		return nil, PTOWrapError(err)
	}

	sort.Slice(setIds, func(i, j int) bool { return setIds[i] < setIds[j] })

	return setIds, nil
}

// ObservationSetIDsWithCondition lists all observation set IDs in the
// database with a given condition. The condition must be a fully qualified condition.
func ObservationSetIDsWithCondition(db orm.DB, cc ConditionCache, condition string) ([]int, error) {
	var setIds []int

	conditions, err := cc.ConditionsByName(db, condition)
	if err != nil {
		return nil, err
	}

	conditionIds := make([]int, len(conditions))
	for i, condition := range conditions {
		conditionIds[i] = condition.ID
	}

	err = db.Model(&ObservationSetCondition{}).
		ColumnExpr("array_agg(observation_set_id)").
		Where("condition_id = ANY(?)", pg.Array(conditionIds)).
		Select(pg.Array(&setIds))
	if err == pg.ErrNoRows {
		return make([]int, 0), nil
	} else if err != nil {
		return nil, PTOWrapError(err)
	}

	sort.Slice(setIds, func(i, j int) bool { return setIds[i] < setIds[j] })

	return setIds, nil
}
