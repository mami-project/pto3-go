package pto3

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
)

type QueryCache struct {
	// Server configuration
	config *PTOConfiguration

	// Database connection
	db *pg.DB

	// Cache of conditions
	cidCache ConditionCache

	// Path to result cache directory
	path string

	// Submitted queries not yet running
	submitted map[string]*Query

	// Queries executing and cached in memory (recently executed)
	cached map[string]*Query

	// Lock for submitted and cached maps
	lock sync.RWMutex
}

// NewQueryCache creates a query cache given a configuration and an
// authorizer. The query cache contains metadata and results (when available),
// backed by permanent storage on disk, and an in-memory cache of recently executed
func NewQueryCache(config *PTOConfiguration) (*QueryCache, error) {

	qc := QueryCache{
		config:    config,
		db:        pg.Connect(&config.ObsDatabase),
		path:      config.QueryCacheRoot,
		submitted: make(map[string]*Query),
		cached:    make(map[string]*Query),
	}

	var err error
	qc.cidCache, err = LoadConditionCache(qc.db)
	if err != nil {
		return nil, err
	}

	return &qc, nil
}

// LoadTestData loads an observation file into a database. It is used as part
// of the setup for testing the query cache, and should not be called in the
// normal case.
func (qc *QueryCache) LoadTestData(obsFilename string) error {
	pidCache := make(PathCache)
	_, err := CopySetFromObsFile(obsFilename, qc.db, qc.cidCache, pidCache)
	return err
}

func (qc *QueryCache) writeResultFile(identifier string) (*os.File, error) {
	return os.Create(filepath.Join(qc.config.QueryCacheRoot, fmt.Sprintf("%s.ndjson", identifier)))
}

func (qc *QueryCache) readResultFile(identifier string) (*os.File, error) {
	return os.Open(filepath.Join(qc.config.QueryCacheRoot, fmt.Sprintf("%s.ndjson", identifier)))
}

func (qc *QueryCache) writeMetadataFile(identifier string) (*os.File, error) {
	return os.Create(filepath.Join(qc.config.QueryCacheRoot, fmt.Sprintf("%s.json", identifier)))
}

func (qc *QueryCache) readMetadataFile(identifier string) (*os.File, error) {
	return os.Open(filepath.Join(qc.config.QueryCacheRoot, fmt.Sprintf("%s.json", identifier)))
}

func (qc *QueryCache) fetchQuery(identifier string) (*Query, error) {
	// we're modifying the cache
	qc.lock.Lock()
	defer qc.lock.Unlock()

	in, err := qc.readMetadataFile(identifier)
	if err != nil {
		if os.IsNotExist(err) {
			// nothing on disk, but that's not an error.
			return nil, nil
		} else {
			return nil, err
		}
	}
	defer in.Close()

	b, err := ioutil.ReadAll(in)
	if err != nil {
		return nil, err
	}

	var q Query
	if err := json.Unmarshal(b, &q); err != nil {
		return nil, err
	}

	if q.Identifier != identifier {
		return nil, PTOErrorf("Identifier mismatch on query fetch: fetched %s got %s", identifier, q.Identifier)
	}

	if q.Completed == nil {
		qc.submitted[identifier] = &q
	} else {
		qc.cached[identifier] = &q
	}

	// FIXME any query we fetch in executing state necessarily crashed. should we restart it?

	return &q, nil
}

func (qc *QueryCache) QueryByIdentifier(identifier string) (*Query, error) {
	// in in-memory submitted queue?
	q := qc.submitted[identifier]
	if q != nil {
		return q, nil
	}

	// in in-memory cache?
	q = qc.cached[identifier]
	if q != nil {
		return q, nil
	}

	// nope, check on disk
	return qc.fetchQuery(identifier)
}

// IntersectCondition represents a condition together with a negation flag
type IntersectCondition struct {
	Condition
	Negate bool
}

// GroupSpec can group a pg-go query by some set of criteria
type GroupSpec interface {
	GroupBy(q *orm.Query) *orm.Query
	URLEncoded() string
}

// SimpleGroupSpec groups a pg-go query by a single column
type SimpleGroupSpec struct {
	ColumnName string
}

func (gs *SimpleGroupSpec) GroupBy(q *orm.Query) *orm.Query {
	return q.Group(gs.ColumnName)
}

func (gs *SimpleGroupSpec) URLEncoded() string {
	return gs.ColumnName
}

// DateTruncGroupSpec groups a pg-go query by applying PostgreSQL's date_trunc function to a column
type DateTruncGroupSpec struct {
	Truncation string
	ColumnName string
}

func (gs *DateTruncGroupSpec) GroupBy(q *orm.Query) *orm.Query {
	return q.Group(fmt.Sprintf("date_trunc(%s, %s)"), gs.Truncation, gs.ColumnName)
}

func (gs *DateTruncGroupSpec) URLEncoded() string {
	return gs.Truncation
}

// DatePartGroupSpec groups a pg-go query by applying PostgreSQL's date_part function to a column
type DatePartGroupSpec struct {
	Part       string
	ColumnName string
}

func (gs *DatePartGroupSpec) GroupBy(q *orm.Query) *orm.Query {
	return q.Group(fmt.Sprintf("date_part(%s, %s)"), gs.Part, gs.ColumnName)
}

func (gs *DatePartGroupSpec) URLEncoded() string {
	switch gs.Part {
	case "dow":
		return "week_day"
	case "hour":
		return "day_hour"
	default:
		panic("bad date part group specification")
	}
}

type Query struct {
	// Reference to cache containing query
	qc *QueryCache

	// Hash-based identifier
	Identifier string

	// Timestamps for state management
	Submitted     *time.Time
	Executed      *time.Time
	Completed     *time.Time
	MadePermanent *time.Time

	// Errors, references, and sources
	ExecutionError error
	ExtRef         string
	Sources        []int

	// Arbitrary metadata
	Metadata map[string]string

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

	// Query options
	optionSetsOnly bool
}

// ParseQueryFromForm creates a new query from an HTTP form, but does not
// submit it. Used by SubmitQueryFromForm, and for testing.
func (qc *QueryCache) ParseQueryFromForm(form url.Values) (*Query, error) {
	var ok bool
	var err error

	// new query bound to this cache
	q := Query{qc: qc}

	// Parse start and end times
	timeStartStrs, ok := form["time_start"]
	if !ok || len(timeStartStrs) < 1 || timeStartStrs[0] == "" {
		return nil, errors.New("Query missing mandatory time_start parameter")
	}
	timeStart, err := ParseTime(timeStartStrs[0])
	if err != nil {
		return nil, fmt.Errorf("Error parsing time_start: %s", err.Error())
	}
	q.timeStart = &timeStart

	timeEndStrs, ok := form["time_end"]
	if !ok || len(timeEndStrs) < 1 || timeEndStrs[0] == "" {
		return nil, errors.New("Query missing mandatory time_end parameter")
	}
	timeEnd, err := ParseTime(timeEndStrs[0])
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
			conditions, err := qc.cidCache.ConditionsByName(qc.db, conditionStr)
			if err != nil {
				return nil, err
			}
			for _, condition := range conditions {
				q.selectConditions = append(q.selectConditions, condition)
			}
		}
	}

	groupStrs, ok := form["group"]
	if ok {

		q.groups = make([]GroupSpec, 0)
		for _, groupStr := range groupStrs {
			switch groupStr {
			case "year":
				q.groups = append(q.groups, &DateTruncGroupSpec{Truncation: "year", ColumnName: "time_start"})
			case "month":
				q.groups = append(q.groups, &DateTruncGroupSpec{Truncation: "month", ColumnName: "time_start"})
			case "week":
				q.groups = append(q.groups, &DateTruncGroupSpec{Truncation: "week", ColumnName: "time_start"})
			case "day":
				q.groups = append(q.groups, &DateTruncGroupSpec{Truncation: "day", ColumnName: "time_start"})
			case "hour":
				q.groups = append(q.groups, &DateTruncGroupSpec{Truncation: "hour", ColumnName: "time_start"})
			case "week_day":
				q.groups = append(q.groups, &DatePartGroupSpec{Part: "dow", ColumnName: "time_start"})
			case "day_hour":
				q.groups = append(q.groups, &DatePartGroupSpec{Part: "hour", ColumnName: "time_start"})
			case "condition":
				q.groups = append(q.groups, &SimpleGroupSpec{ColumnName: "condition"})
			case "source":
				return nil, errors.New("source groups not yet implemented")
			case "target":
				return nil, errors.New("target groups not yet implemented")
			}
		}
	}

	// FIXME parse intersect conditions
	_, ok = form["intersect_condition"]
	if ok {
		return nil, errors.New("intersecting path support not yet implemented")
	}

	// FIXME parse options
	optionStrs, ok := form["option"]
	if ok {
		for _, optionStr := range optionStrs {
			if optionStr == "sets_only" {
				q.optionSetsOnly = true
			}
		}
	}

	// hash everything into an identifier
	q.generateIdentifier()

	return &q, nil
}

// SubmitQueryFromForm submits a new query to a cache from an HTTP form. If an
// identical query is already cached, it returns the cached query. Use this to
// handle POST queries.
func (qc *QueryCache) SubmitQueryFromForm(form url.Values) (*Query, error) {
	// parse the query
	q, err := qc.ParseQueryFromForm(form)
	if err != nil {
		return nil, err
	}

	// check to see if it's been cached
	oq, err := qc.QueryByIdentifier(q.Identifier)
	if err != nil {
		return nil, err
	}
	if oq != nil {
		return oq, nil
	}

	// nope, new query. set submitted timestamp.
	t := time.Now()
	q.Submitted = &t

	// and add to submitted queue
	qc.submitted[q.Identifier] = q

	return q, nil
}

// ParseQueryFromURLEncoded creates a new query bound to a cache from a URL encoded query string. Used for parser testing and JSON unmarshaling.
func (qc *QueryCache) ParseQueryFromURLEncoded(urlencoded string) (*Query, error) {
	v, err := url.ParseQuery(urlencoded)
	if err != nil {
		return nil, err
	}
	return qc.ParseQueryFromForm(v)
}

// SubmitQueryFromURLEncoded creates a new query bound to a cache from a URL encoded query string. Use this to handle GET queries.
func (qc *QueryCache) SubmitQueryFromURLEncoded(urlencoded string) (*Query, error) {
	v, err := url.ParseQuery(urlencoded)
	if err != nil {
		return nil, err
	}
	return qc.SubmitQueryFromForm(v)
}

// URLEncoded returns the normalized query string representing this query. This is used to generate query identifiers.
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
	for i := range q.selectConditions {
		out += fmt.Sprintf("&condition=%s", q.selectConditions[i].Name)
	}

	// add sorted groups
	sort.SliceStable(q.groups, func(i, j int) bool {
		return q.groups[i].URLEncoded() < q.groups[j].URLEncoded()
	})
	for i := range q.selectTarget {
		out += fmt.Sprintf("&group=%s", q.groups[i].URLEncoded())
	}

	// add options
	if q.optionSetsOnly {
		out += "&option=sets_only"
	}

	return out
}

func (q *Query) generateIdentifier() {
	// normalize form of the specification and generate a query identifier
	hashbytes := sha256.Sum256([]byte(q.URLEncoded()))
	q.Identifier = hex.EncodeToString(hashbytes[:])
}

func (q *Query) generateSources() error {
	if len(q.selectSets) > 0 {
		// Sets specified in query. Let's just use them.
		q.Sources = q.selectSets
	} else {
		// We have to actually run a query here.
		var err error
		if q.Sources, err = q.selectObservationSetIDs(); err != nil {
			return err
		}
	}
	return nil
}

// ResultLink generates a link to the file containing query results.
func (q *Query) ResultLink() string {
	link, _ := q.qc.config.LinkTo(fmt.Sprintf("query/%s/result", q.Identifier))
	return link
}

// SourceLinks generates links to the observation sets contributing to this query
func (q *Query) SourceLinks() []string {
	out := make([]string, len(q.Sources))
	for i, setid := range q.Sources {
		out[i] = LinkForSetID(q.qc.config, setid)
	}
	return out
}

func (q *Query) MarshalJSON() ([]byte, error) {
	jobj := make(map[string]interface{})

	// Store the query itself in its urlencoded form
	jobj["__encoded"] = q.URLEncoded()

	// Cache the identifier (we'll ignore this on unmarshaling, but it's nice to have)
	jobj["__id"] = q.Identifier

	// Determine state and additional information
	if q.Completed != nil {
		if q.ExecutionError != nil {
			jobj["__state"] = "failed"
			jobj["__error"] = q.ExecutionError.Error()
		} else if q.ExtRef != "" {
			jobj["__state"] = "permanent"
			jobj["_ext_ref"] = q.ExtRef
		} else {
			jobj["__state"] = "complete"
		}
		jobj["__completion_time"] = q.Completed.Format(time.RFC3339)
		jobj["__execution_time"] = q.Executed.Format(time.RFC3339)
		jobj["__submission_time"] = q.Submitted.Format(time.RFC3339)
	} else if q.Executed != nil {
		jobj["__state"] = "pending"
		jobj["__execution_time"] = q.Executed.Format(time.RFC3339)
		jobj["__submission_time"] = q.Submitted.Format(time.RFC3339)
	} else {
		if q.Submitted != nil {
			jobj["__submission_time"] = q.Submitted.Format(time.RFC3339)
		}
		jobj["__state"] = "submitted"
	}

	// copy metadata
	for k := range q.Metadata {
		if !strings.HasPrefix(q.Metadata[k], "__") {
			jobj[k] = q.Metadata[k]
		}
	}

	return json.Marshal(jobj)
}

func (q *Query) UnmarshalJSON(b []byte) error {
	// TODO write me
	return nil
}

func (q *Query) whereClauses(pq *orm.Query) *orm.Query {
	// time
	pq = pq.Where("time_start > ?", q.timeStart).Where("time_end < ?", q.timeEnd)

	// sets
	if len(q.selectSets) > 0 {
		pq = pq.WhereGroup(func(qq *orm.Query) (*orm.Query, error) {
			for _, setid := range q.selectSets {
				qq = qq.WhereOr("set_id = ?", setid)
			}
			return qq, nil
		})
	}

	// conditions
	if len(q.selectConditions) > 0 {
		pq = pq.WhereGroup(func(qq *orm.Query) (*orm.Query, error) {
			for _, c := range q.selectConditions {
				qq = qq.WhereOr("condition_id = ?", c.ID)
			}
			return qq, nil
		})
	}

	// FIXME source

	// FIXME target

	// FIXME on path

	return pq
}

// selectAndStoreObservations selects observations from this query and dumps
// them to the data file for this query as an NDJSON observation file.
func (q *Query) selectAndStoreObservations() error {
	var obsdat []Observation

	pq := q.qc.db.Model(&obsdat).Column("observation.*", "Condition", "Path")
	pq = q.whereClauses(pq)
	if err := pq.Select(); err != nil {
		return err
	}

	outfile, err := q.qc.writeResultFile(q.Identifier)
	if err != nil {
		return err
	}
	defer outfile.Close()

	if err := WriteObservations(obsdat, outfile); err != nil {
		return err
	}

	return nil
}

// selectObservationSetIDs selects observation set IDs responding to
// this query.
func (q *Query) selectObservationSetIDs() ([]int, error) {
	var setids []int

	pq := q.qc.db.Model(&setids).ColumnExpr("DISTINCT set_id")
	pq = q.whereClauses(pq)
	if err := pq.Select(); err != nil {
		return nil, err
	}

	return setids, nil

}

// selectAndStoreObservationSetIDs selects observation set IDs responding to
// this query and dumps them to the data file as NDJSON: one URL per line.
func (q *Query) selectAndStoreObservationSetLinks() error {
	setids, err := q.selectObservationSetIDs()
	if err != nil {
		return err
	}

	outfile, err := q.qc.writeResultFile(q.Identifier)
	if err != nil {
		return err
	}
	defer outfile.Close()

	for _, setid := range setids {
		if _, err := fmt.Fprintf(outfile, "\"%s\"\n", LinkForSetID(q.qc.config, setid)); err != nil {
			return err
		}
	}

	return nil
}

// selectAndStoreGroups selects groups responding to this query and dumps them
// to the data file as NDJSON, one line containing a JSON array per group,
// with elements 0 to n-1 being group names, and element n being the count of
// observations in the group.
func (q *Query) selectAndStoreGroups() error {
	return errors.New("Group query execution not yet supported")
}

// selectAndStoreIntersectPaths selects paths in the condition intersection of
// this query and dumps them to the data file as NDJSON, one path per line.
func (q *Query) selectAndStoreIntersectPaths() error {
	return errors.New("Intersection query execution not yet supported")
}

func (q *Query) Execute(done chan<- struct{}) {
	// fire off a goroutine to actually run the query
	go func() {
		// mark query as executing
		startTime := time.Now()
		q.Executed = &startTime

		// switch and run query
		if len(q.intersectConditions) > 0 {
			q.ExecutionError = q.selectAndStoreIntersectPaths()
		} else if len(q.groups) > 0 {
			q.ExecutionError = q.selectAndStoreGroups()
		} else if q.optionSetsOnly {
			q.ExecutionError = q.selectAndStoreObservationSetLinks()
		} else {
			q.ExecutionError = q.selectAndStoreObservations()
		}

		// mark query as done
		endTime := time.Now()
		q.Completed = &endTime

		// and notify if we have a channel
		if done != nil {
			done <- struct{}{}
		}
	}()
}
