package pto3

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"github.com/gorilla/mux"
)

type IntersectCondition struct {
	Condition
	Negate bool
}

type GroupSpec interface {
	GroupBy(q *orm.Query) *orm.Query
}

type SimpleGroupSpec struct {
	ColumnName string
}

func (gs *SimpleGroupSpec) GroupBy(q *orm.Query) *orm.Query {
	return q.Group(gs.ColumnName)
}

type Query struct {
	// Reference to cache containing query
	qc *QueryCache

	// Hash-based identifier
	Identifier string

	// State and metadata
	IsExecuting    bool
	HasResult      bool
	ExecutionError error
	ExtRef         string
	Sources        []int
	Metadata       map[string]string

	// Parsed query parameters
	timeStart           *time.Time
	timeEnd             *time.Time
	selectSets          []int
	selectOnPath        []string
	selectSource        []string
	selectTarget        []string
	selectConditions    []Condition
	groups              []GroupSpec
	intersectConditions []IntersectCondition
	options             []string
}

func NewQueryFromForm(qc *QueryCache, form url.Values) (*Query, error) {
	var q Query
	var ok bool
	var err error

	// Parse start and end times
	timeStartStrs, ok := form["time_start"]
	if !ok || len(timeStartStrs) < 1 || timeStartStrs[0] == "" {
		return nil, errors.New("Query missing mandatory time_start parameter")
	}
	timeStart, err := time.Parse(time.RFC3339, timeStartStrs[0])
	if err != nil {
		return nil, fmt.Errorf("Error parsing time_start: %s", err.Error())
	}
	q.timeStart = &timeStart

	timeEndStrs, ok := form["time_end"]
	if !ok || len(timeEndStrs) < 1 || timeEndStrs[0] == "" {
		return nil, errors.New("Query missing mandatory time_end parameter")
	}
	timeEnd, err := time.Parse(time.RFC3339, timeEndStrs[0])
	if err != nil {
		return nil, fmt.Errorf("Error parsing time_end: %s", err.Error())
	}
	q.timeEnd = &timeEnd

	if q.timeStart.After(*q.timeEnd) {
		q.timeStart, q.timeEnd = q.timeEnd, q.timeStart
	}

	// Parse set parameters into set IDs as integers
	setStrs, ok := form["set"]
	if ok {
		q.selectSets = make([]int, len(setStrs))
		for i := range setStrs {
			seti64, err := strconv.ParseInt(setStrs[i], 16, 32)
			if err != nil {
				return nil, fmt.Errorf("Error parsing set ID: %s", err.Error())
			}
			q.selectSets[i] = int(seti64)
		}
	}

	// Can't really validate paths, so just store these slices directly from the form.
	q.selectOnPath = form["on_path"]
	q.selectSource = form["source"]
	q.selectTarget = form["target"]

	conditionStrs, ok := form["condition"]
	if ok {
		q.selectConditions = make([]Condition, 0)
		for _, conditionStr := range conditionStrs {
			conditions, err := ConditionsByName(conditionStr, qc.db)
			if err != nil {
				return nil, fmt.Errorf("Error validating condition: %s", err.Error())
			}
			for _, condition := range conditions {
				q.selectConditions = append(q.selectConditions, condition)
			}
		}
	}

	groupStrs, ok := form["groups"]
	if ok {
		return nil, errors.New("group support not yet implemented")

		q.groups = make([]GroupSpec, 0)
		for _, groupStr := range groupStrs {
			switch groupStr {
			case "year":
				// FIXME determine how to group by year of start date
				// in postgres: date_trunc('year', column)
			case "month":
				// FIXME determine how to group by month of start date
				// in postgres: date_trunc('month', column)
			case "week":
				// FIXME determine how to group by week of start date
				// in postgres: date_trunc('week', column)
			case "day":
				// FIXME determine how to group by day of start date
				// in postgres: date_trunc('day', column)
			case "hour":
				// FIXME determine how to group by hour of start date
				// date_trunc('hour', column)
			case "week_day":
				// FIXME determine how to group by weekday of start date
				// date_part('dow', column)
			case "day_hour":
				// FIXME determine how to group by day hour of start date
				// date_part('hour', column)
			case "condition":
				q.groups = append(q.groups, &SimpleGroupSpec{ColumnName: "condition"})
			case "source":
				// FIXME need to denormalize model to make source groups work
			case "target":
				// FIXME need to denormalize model to make target groups work
			}
		}
	}

	// FIXME parse intersect conditions
	_, ok = form["intersect_condition"]
	if ok {
		return nil, errors.New("intersecting path support not yet implemented")

	}

	// FIXME parse options
	_, ok = form["option"]
	if ok {
		return nil, errors.New("query option support not yet implemented")
	}

	// hash everything into an identifier
	q.generateIdentifier()

	return &q, nil
}

func NewQueryFromURLEncoded(qc *QueryCache, urlencoded string) (*Query, error) {
	v, err := url.ParseQuery(urlencoded)
	if err != nil {
		return nil, err
	}
	return NewQueryFromForm(qc, v)
}

func (q *Query) URLEncoded() string {
	// generate query specification as normalized, urlencoded

	// start with start and end time
	out := fmt.Sprintf("time_start=%s&time_end=%s",
		url.QueryEscape(q.timeStart.Format(time.RFC3339)),
		url.QueryEscape(q.timeEnd.Format(time.RFC3339)))

	// add sorted observation sets
	sort.SliceStable(q.selectSets, func(i, j int) bool {
		return q.selectSets[i] < q.selectSets[j]
	})
	for i := range q.selectSets {
		out += fmt.Sprintf("&set=%d", q.selectSets[i])
	}

	// add sorted path elements
	sort.SliceStable(q.selectOnPath, func(i, j int) bool {
		return q.selectOnPath[i] < q.selectOnPath[j]
	})
	for i := range q.selectOnPath {
		out += fmt.Sprintf("&on_path=%s", q.selectOnPath[i])
	}

	// add sorted sources
	sort.SliceStable(q.selectSource, func(i, j int) bool {
		return q.selectSource[i] < q.selectSource[j]
	})
	for i := range q.selectSource {
		out += fmt.Sprintf("&source=%s", q.selectSource[i])
	}

	// add sorted target
	sort.SliceStable(q.selectTarget, func(i, j int) bool {
		return q.selectTarget[i] < q.selectTarget[j]
	})
	for i := range q.selectTarget {
		out += fmt.Sprintf("&target=%s", q.selectTarget[i])
	}

	// add sorted conditions
	sort.SliceStable(q.selectConditions, func(i, j int) bool {
		return q.selectConditions[i].Name < q.selectConditions[j].Name
	})
	for i := range q.selectTarget {
		out += fmt.Sprintf("&conditions=%s", q.selectConditions[i].Name)
	}

	return out
}

func (q *Query) generateIdentifier() {
	// normalize form of the specification and generate a query identifier
	hashbytes := sha256.Sum256([]byte(q.URLEncoded()))
	q.Identifier = hex.EncodeToString(hashbytes[:])
}

func (q *Query) MarshalJSON() ([]byte, error) {
	// TODO write me
	return nil, nil
}

func (q *Query) UnmarshalJSON(b []byte) error {
	// TODO write me
	return nil
}

// selectAndStoreObservations selects observations from this query and dumps
// them to the data file for this query as an NDJSON observation file.
func (q *Query) selectAndStoreObservations() error {
	return nil
}

// selectAndStoreObservationSetIDs selects observation set IDs responding to
// this query and dumps them to the data file as NDJSON: one URL per line.
func (q *Query) selectAndStoreObservationSetIDs() error {
	return nil
}

// selectAndStoreGroups selects groups responding to this query and dumps them
// to the data file as NDJSON, one line containing a JSON array per group,
// with elements 0 to n-1 being group names, and element n being the count of
// observations in the group.
func (q *Query) selectAndStoreGroups() error {
	return nil
}

// selectAndStoreIntersectPaths selects paths in the condition intersection of
// this query and dumps them to the data file as NDJSON, one path per line.
func (q *Query) selectAndStoreIntersectPaths() error {
	return nil
}

func (q *Query) Execute() error {
	// So Execute() should open an output file,
	// run the query, and dump the result to the file.

	// Results should be ndjson.
	return nil
}

type QueryCache struct {
	// Server configuration
	config *PTOServerConfig
	// Authorizer
	azr *Authorizer
	// Database connection
	db *pg.DB

	// Path to result cache directory
	path string

	// Submitted queries not yet running
	submitted []*Query

	// Queries executing and cached in memory (recently executed)
	cached map[string]*Query
}

// NewQueryCache creates a query cache given a configuration and an
// authorizer. The query cache contains metadata and results (when available),
// backed by permanent storage on disk, and an in-memory cache of recently executed

func NewQueryCache(config *PTOServerConfig, azr *Authorizer) (*QueryCache, error) {

	qc := QueryCache{
		config:    config,
		azr:       azr,
		db:        pg.Connect(&config.ObsDatabase),
		path:      config.QueryCacheRoot,
		submitted: make([]*Query, 0),
		cached:    make(map[string]*Query),
	}

	return &qc, nil
}

// Ensure that the directories backing the query cache exist. Used for testing.
func (qc *QueryCache) CreateDirectories() error {
	return os.Mkdir(qc.path, 0755)
}

// Remove the directories backing the query cache incluing all their contents.
// Used for testing.
func (qc *QueryCache) RemoveDirectories() error {
	return os.RemoveAll(qc.path)
}

func (qc *QueryCache) QueryByIdentifier(identifier string) (*Query, error) {
	// check in memory

	// then check on disk

	return nil, nil
}

// Flush ensures that any state stored in the in-memory cache is written to disk.
func (qc *QueryCache) Flush() error {
	return nil
}

func (qc *QueryCache) HandleList(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func (qc *QueryCache) HandleSubmit(w http.ResponseWriter, r *http.Request) {
	// Create a query specifier

	// Stick it in queue

	// Q: do we need a context here?
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func (qc *QueryCache) HandleGetMetadata(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)

}

func (qc *QueryCache) HandlePutMetadata(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)

}

func (qc *QueryCache) HandleGetResults(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)

}

func (qc *QueryCache) AddRoutes(r *mux.Router) {
	r.HandleFunc("/query", qc.HandleList).Methods("GET")
	r.HandleFunc("/query/submit", qc.HandleSubmit).Methods("POST")
	r.HandleFunc("/query/{query}", qc.HandleGetMetadata).Methods("GET")
	r.HandleFunc("/query/{query}", qc.HandlePutMetadata).Methods("PUT")
	r.HandleFunc("/query/{query}/data", qc.HandleGetResults).Methods("GET")
}
