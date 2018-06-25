package pto3

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
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

	// Cached queries we know about
	query map[string]*Query

	// channel for execution tokens
	exectokens chan struct{}

	// Lock for submitted and cached maps
	lock sync.RWMutex
}

// NewQueryCache creates a query cache given a configuration and an
// authorizer. The query cache contains metadata and results (when available),
// backed by permanent storage on disk, and an in-memory cache of recently executed
func NewQueryCache(config *PTOConfiguration) (*QueryCache, error) {

	qc := QueryCache{
		config:     config,
		db:         pg.Connect(&config.ObsDatabase),
		path:       config.QueryCacheRoot,
		query:      make(map[string]*Query),
		exectokens: make(chan struct{}, config.ConcurrentQueries),
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
func (qc *QueryCache) LoadTestData(obsFilename string) (int, error) {
	pidCache := make(PathCache)
	set, err := CopySetFromObsFile(obsFilename, qc.db, qc.cidCache, pidCache)
	if err != nil {
		return 0, err
	} else {
		return set.ID, nil
	}
}

func (qc *QueryCache) EnableQueryLogging() {
	EnableQueryLogging(qc.db)
}

func (qc *QueryCache) writeMetadataFile(identifier string) (*os.File, error) {
	return os.Create(filepath.Join(qc.config.QueryCacheRoot, fmt.Sprintf("%s.json", identifier)))
}

func (qc *QueryCache) readMetadataFile(identifier string) (*os.File, error) {
	return os.Open(filepath.Join(qc.config.QueryCacheRoot, fmt.Sprintf("%s.json", identifier)))
}

func (qc *QueryCache) statMetadataFile(identifier string) (os.FileInfo, error) {
	return os.Stat(filepath.Join(qc.config.QueryCacheRoot, fmt.Sprintf("%s.json", identifier)))
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
			return nil, PTOWrapError(err)
		}
	}
	defer in.Close()

	b, err := ioutil.ReadAll(in)
	if err != nil {
		return nil, PTOWrapError(err)
	}

	var q Query
	if err := json.Unmarshal(b, &q); err != nil {
		return nil, PTOWrapError(err)
	}

	if q.Identifier != identifier {
		return nil, PTOErrorf("Identifier mismatch on query fetch: fetched %s got %s", identifier, q.Identifier)
	}

	// Stick query in the cache
	qc.query[identifier] = &q

	// FIXME any query we fetch in executing state necessarily crashed. need a way to revive these.

	return &q, nil
}

func (qc *QueryCache) QueryByIdentifier(identifier string) (*Query, error) {

	q := func() *Query {
		qc.lock.RLock()
		defer qc.lock.RUnlock()

		// in in-memory cache?
		q := qc.query[identifier]
		if q != nil {
			return q
		}

		return nil

	}()

	if q != nil {
		return q, nil
	}

	// nope, check on disk
	return qc.fetchQuery(identifier)
}

func (qc *QueryCache) CachedQueryLinks() ([]string, error) {
	out := make([]string, 0)

	// FIXME: cache this somewhere, allow invalidation
	direntries, err := ioutil.ReadDir(qc.config.QueryCacheRoot)

	if err != nil {
		return nil, PTOWrapError(err)
	}

	for _, direntry := range direntries {
		metafilename := direntry.Name()
		if strings.HasSuffix(metafilename, ".json") {
			linkname := metafilename[0 : len(metafilename)-len(".json")]
			link, _ := qc.config.LinkTo(fmt.Sprintf("/query/%s", linkname))
			out = append(out, link)
		}
	}

	return out, nil
}

// GroupSpec can group a pg-go query by some set of criteria
type GroupSpec interface {
	URLEncoded() string
	ColumnSpec() string
}

// SimpleGroupSpec groups a pg-go query by a single column
type SimpleGroupSpec struct {
	Name     string
	Column   string
	ExtTable string
}

func (gs *SimpleGroupSpec) URLEncoded() string {
	return gs.Name
}

func (gs *SimpleGroupSpec) ColumnSpec() string {
	return gs.Column
}

// DateTruncGroupSpec groups a pg-go query by applying PostgreSQL's date_trunc function to a column
type DateTruncGroupSpec struct {
	Truncation string
	Column     string
}

func (gs *DateTruncGroupSpec) URLEncoded() string {
	return gs.Truncation
}

func (gs *DateTruncGroupSpec) ColumnSpec() string {
	return fmt.Sprintf("date_trunc('%s', %s)", gs.Truncation, gs.Column)
}

// DatePartGroupSpec groups a pg-go query by applying PostgreSQL's date_part function to a column
type DatePartGroupSpec struct {
	Part   string
	Column string
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

func (gs *DatePartGroupSpec) ColumnSpec() string {
	return fmt.Sprintf("date_part('%s', %s)", gs.Part, gs.Column)
}

type Query struct {
	// Reference to cache containing query
	qc *QueryCache

	// Hash-based identifier
	Identifier string

	// Timestamps for state management
	Submitted *time.Time
	Executed  *time.Time
	Completed *time.Time

	// Result Row Count (cached)
	resultRowCount int

	// Errors, references, and sources
	ExecutionError error
	ExtRef         string
	Sources        []int

	// Arbitrary metadata
	Metadata map[string]string

	// Parsed query parameters
	timeStart        *time.Time
	timeEnd          *time.Time
	selectSets       []int
	selectOnPath     []string
	selectSources    []string
	selectTargets    []string
	selectConditions []Condition
	selectValues     []string
	groups           []GroupSpec

	// Query options
	optionSetsOnly             bool
	optionCountDistinctTargets bool
}

func (q *Query) populateFromForm(form url.Values) error {
	var ok bool

	// Parse start and end times
	timeStartStrs, ok := form["time_start"]
	if !ok || len(timeStartStrs) < 1 || timeStartStrs[0] == "" {
		return PTOErrorf("Query missing mandatory time_start parameter").StatusIs(http.StatusBadRequest)
	}
	timeStart, err := ParseTime(timeStartStrs[0])
	if err != nil {
		return PTOErrorf("Error parsing time_start: %s", err.Error()).StatusIs(http.StatusBadRequest)
	}
	q.timeStart = &timeStart

	timeEndStrs, ok := form["time_end"]
	if !ok || len(timeEndStrs) < 1 || timeEndStrs[0] == "" {
		return PTOErrorf("Query missing mandatory time_start parameter").StatusIs(http.StatusBadRequest)
	}
	timeEnd, err := ParseTime(timeEndStrs[0])
	if err != nil {
		return PTOErrorf("Error parsing time_end: %s", err.Error()).StatusIs(http.StatusBadRequest)
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
				return PTOErrorf("Error parsing set ID: %s", err.Error()).StatusIs(http.StatusBadRequest)
			}
			q.selectSets[i] = int(seti64)
		}
	}

	// Can't really validate paths or values so just store these slices directly from the form.
	q.selectOnPath = form["on_path"]
	q.selectSources = form["source"]
	q.selectTargets = form["target"]
	q.selectValues = form["value"]

	// Validate and expand conditions
	conditionStrs, ok := form["condition"]
	if ok {

		// don't panic on nil qc/cidcache (DEBUG)
		if q.qc == nil {
			return PTOErrorf("qc is nil expanding condition array %v", form["condition"])
		} else if q.qc.cidCache == nil {
			return PTOErrorf("cidCache is nil expanding condition array %v", form["condition"])
		}

		q.selectConditions = make([]Condition, 0)
		for _, conditionStr := range conditionStrs {
			conditions, err := q.qc.cidCache.ConditionsByName(q.qc.db, conditionStr)
			if err != nil {
				return err
			}
			for _, condition := range conditions {
				q.selectConditions = append(q.selectConditions, condition)
			}
		}
	}

	groupStrs, ok := form["group"]
	if ok {
		if len(groupStrs) > 2 {
			return PTOErrorf("Group by more than two dimensions not supported").StatusIs(http.StatusBadRequest)
		}
		q.groups = make([]GroupSpec, len(groupStrs))
		for i, groupStr := range groupStrs {
			switch groupStr {
			case "year":
				q.groups[i] = &DateTruncGroupSpec{Truncation: "year", Column: "time_start"}
			case "month":
				q.groups[i] = &DateTruncGroupSpec{Truncation: "month", Column: "time_start"}
			case "week":
				q.groups[i] = &DateTruncGroupSpec{Truncation: "week", Column: "time_start"}
			case "day":
				q.groups[i] = &DateTruncGroupSpec{Truncation: "day", Column: "time_start"}
			case "hour":
				q.groups[i] = &DateTruncGroupSpec{Truncation: "hour", Column: "time_start"}
			case "week_day":
				q.groups[i] = &DatePartGroupSpec{Part: "dow", Column: "time_start"}
			case "day_hour":
				q.groups[i] = &DatePartGroupSpec{Part: "hour", Column: "time_start"}
			case "condition":
				q.groups[i] = &SimpleGroupSpec{Name: "condition", Column: "condition.name", ExtTable: "conditions"}
			case "feature":
				q.groups[i] = &SimpleGroupSpec{Name: "feature", Column: "substring(condition.name from '^[^\\\\.]+')", ExtTable: "conditions"}
			case "source":
				q.groups[i] = &SimpleGroupSpec{Name: "source", Column: "path.source", ExtTable: "paths"}
			case "target":
				q.groups[i] = &SimpleGroupSpec{Name: "target", Column: "path.target", ExtTable: "paths"}
			case "value":
				q.groups[i] = &SimpleGroupSpec{Name: "value", Column: "value", ExtTable: ""}
			default:
				return PTOErrorf("unsupported group name %s", groupStr).StatusIs(http.StatusBadRequest)
			}
		}
	}

	// parse options
	optionStrs, ok := form["option"]
	if ok {
		for _, optionStr := range optionStrs {
			switch optionStr {
			case "sets_only":
				q.optionSetsOnly = true
			case "count_targets":
				q.optionCountDistinctTargets = true
			}
		}
	}

	// hash everything into an identifier
	q.generateIdentifier()

	return nil
}

func (q *Query) populateFromEncoded(urlencoded string) error {
	v, err := url.ParseQuery(urlencoded)
	if err != nil {
		return PTOWrapError(err)
	}

	return q.populateFromForm(v)
}

// ParseQueryFromForm creates a new query from an HTTP form, but does not
// submit it. Used by SubmitQueryFromForm, and for testing.
func (qc *QueryCache) ParseQueryFromForm(form url.Values) (*Query, error) {
	// new query bound to this cache
	q := Query{qc: qc}

	if err := q.populateFromForm(form); err != nil {
		return nil, err
	}

	return &q, nil
}

// SubmitQueryFromForm submits a new query to a cache from an HTTP form. If an
// identical query is already cached, it returns the cached query. Use this to
// handle POST queries.

func (qc *QueryCache) SubmitQueryFromForm(form url.Values) (*Query, bool, error) {
	// parse the query
	q, err := qc.ParseQueryFromForm(form)
	if err != nil {
		return nil, false, err
	}

	// check to see if it's been cached
	oq, err := qc.QueryByIdentifier(q.Identifier)
	if err != nil {
		return nil, false, err
	}
	if oq != nil {
		return oq, false, nil
	}

	// nope, new query. set submitted timestamp.
	t := time.Now()
	q.Submitted = &t

	// we're modifying the cache
	qc.lock.Lock()
	defer qc.lock.Unlock()

	// write to disk
	if err := q.FlushMetadata(); err != nil {
		return nil, false, err
	}

	// add to cache
	qc.query[q.Identifier] = q

	return q, true, nil
}

func (qc *QueryCache) ExecuteQueryFromForm(form url.Values, done chan struct{}) (*Query, bool, error) {

	// submit the query
	q, new, err := qc.SubmitQueryFromForm(form)
	if err != nil {
		return nil, false, err
	}

	// execute and do an immediate wait for it if it's new
	if new {
		q.ExecuteWaitImmediate(done)
	}

	return q, new, nil
}

// ParseQueryFromURLEncoded creates a new query bound to a cache from a URL encoded query string. Used for parser testing and JSON unmarshaling.
func (qc *QueryCache) ParseQueryFromURLEncoded(urlencoded string) (*Query, error) {
	// new query bound to this cache
	q := Query{qc: qc}

	if err := q.populateFromEncoded(urlencoded); err != nil {
		return nil, err
	}

	return &q, nil
}

// SubmitQueryFromURLEncoded creates a new query bound to a cache from a URL encoded query string. Use this to handle GET queries.
func (qc *QueryCache) SubmitQueryFromURLEncoded(urlencoded string) (*Query, bool, error) {
	v, err := url.ParseQuery(urlencoded)
	if err != nil {
		return nil, false, err
	}
	return qc.SubmitQueryFromForm(v)
}

func (qc *QueryCache) ExecuteQueryFromURLEncoded(encoded string, done chan struct{}) (*Query, bool, error) {

	// submit the query
	q, new, err := qc.SubmitQueryFromURLEncoded(encoded)
	if err != nil {
		return nil, false, err
	}

	// execute and do an immediate wait for it if it's new
	if new {
		q.ExecuteWaitImmediate(done)
	}

	return q, new, nil
}

// URLEncoded returns the normalized query string representing this query.
// This is used to generate query identifiers, and to serialize queries to
// disk.
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
	sort.SliceStable(q.selectSources, func(i, j int) bool {
		return q.selectSources[i] < q.selectSources[j]
	})
	for i := range q.selectSources {
		out += fmt.Sprintf("&source=%s", q.selectSources[i])
	}

	// add sorted target
	sort.SliceStable(q.selectTargets, func(i, j int) bool {
		return q.selectTargets[i] < q.selectTargets[j]
	})
	for i := range q.selectTargets {
		out += fmt.Sprintf("&target=%s", q.selectTargets[i])
	}

	// add sorted conditions
	sort.SliceStable(q.selectConditions, func(i, j int) bool {
		return q.selectConditions[i].Name < q.selectConditions[j].Name
	})
	for i := range q.selectConditions {
		out += fmt.Sprintf("&condition=%s", q.selectConditions[i].Name)
	}

	// add sorted values
	sort.SliceStable(q.selectValues, func(i, j int) bool {
		return q.selectValues[i] < q.selectValues[j]
	})
	for i := range q.selectValues {
		out += fmt.Sprintf("&value=%s", q.selectValues[i])
	}

	// add sorted groups
	sort.SliceStable(q.groups, func(i, j int) bool {
		return q.groups[i].URLEncoded() < q.groups[j].URLEncoded()
	})
	for i := range q.groups {
		out += fmt.Sprintf("&group=%s", q.groups[i].URLEncoded())
	}

	// add options
	if q.optionSetsOnly {
		out += "&option=sets_only"
	}
	if q.optionCountDistinctTargets {
		out += "&option=count_targets"
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

func (q *Query) ResultRowCount() int {
	if q.resultRowCount > 0 {
		return q.resultRowCount
	}

	if q.Completed == nil {
		return 0
	}

	// okay, we have to scan the file
	resultFile, err := q.ReadResultFile()
	if err != nil {
		return 0
	}
	defer resultFile.Close()

	q.resultRowCount = 0
	resultScanner := bufio.NewScanner(resultFile)
	for resultScanner.Scan() {
		q.resultRowCount++
	}

	return q.resultRowCount
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

func (q *Query) modificationTime() *time.Time {
	fi, err := q.qc.statMetadataFile(q.Identifier)
	if err != nil {
		return q.Submitted
	} else {
		mt := fi.ModTime()
		return &mt
	}
}

func (q *Query) MarshalJSON() ([]byte, error) {
	jobj := make(map[string]interface{})

	// Store the query itself in its urlencoded form
	jobj["__encoded"] = q.URLEncoded()

	// Store a link to the query using the API
	var err error
	jobj["__link"], err = q.qc.config.LinkTo("query/" + q.Identifier)
	if err != nil {
		return nil, err
	}

	// Determine state and additional information
	if q.Completed != nil {
		if q.ExecutionError != nil {
			jobj["__state"] = "failed"
			jobj["__error"] = q.ExecutionError.Error()
		} else if q.ExtRef != "" {
			jobj["__state"] = "permanent"
			jobj["_ext_ref"] = q.ExtRef
			jobj["__result"] = jobj["__link"].(string) + "/result"
			jobj["__row_count"] = q.ResultRowCount()
		} else {
			jobj["__state"] = "complete"
			jobj["__result"] = jobj["__link"].(string) + "/result"
			jobj["__row_count"] = q.ResultRowCount()
		}
		jobj["__completed"] = q.Completed.Format(time.RFC3339)
		jobj["__executed"] = q.Executed.Format(time.RFC3339)
		jobj["__created"] = q.Submitted.Format(time.RFC3339)
		jobj["__modified"] = q.modificationTime().Format(time.RFC3339)
	} else {
		jobj["__state"] = "pending"
		if q.Executed != nil {
			jobj["__executed"] = q.Executed.Format(time.RFC3339)
		}
		if q.Submitted != nil {
			jobj["__created"] = q.Submitted.Format(time.RFC3339)
			jobj["__modified"] = q.modificationTime().Format(time.RFC3339)
		}
	}

	// copy metadata
	for k := range q.Metadata {
		if !strings.HasPrefix(k, "__") {
			jobj[k] = q.Metadata[k]
		}
	}

	return json.Marshal(jobj)
}

func (q *Query) setMetadata(jmap map[string]string) {
	// store external reference
	q.ExtRef = jmap["_ext_ref"]

	// copy and replace arbitrary metadata
	q.Metadata = make(map[string]string)
	for k := range jmap {
		if !strings.HasPrefix(k, "__") && k != "_ext_ref" {
			q.Metadata[k] = jmap[k]
		}
	}
}

func (q *Query) UnmarshalJSON(b []byte) error {
	// get a JSON map
	var jmap map[string]string
	if err := json.Unmarshal(b, &jmap); err != nil {
		return PTOWrapError(err)
	}

	// parse the query from its encoded representation
	encoded := jmap["__encoded"]
	if err := q.populateFromEncoded(encoded); err != nil {
		return err
	}

	// store timestamps
	if jmap["__time_submitted"] != "" {
		ts, err := time.Parse(time.RFC3339, jmap["__time_submitted"])
		if err != nil {
			return PTOWrapError(err)
		}
		q.Submitted = &ts
	}

	if jmap["__time_executed"] != "" {
		ts, err := time.Parse(time.RFC3339, jmap["__time_executed"])
		if err != nil {
			return PTOWrapError(err)
		}
		q.Executed = &ts
	}

	if jmap["__time_completed"] != "" {
		ts, err := time.Parse(time.RFC3339, jmap["__time_completed"])
		if err != nil {
			return PTOWrapError(err)
		}
		q.Completed = &ts
	}

	if jmap["__error"] != "" {
		q.ExecutionError = errors.New(jmap["__error"])
	}

	q.setMetadata(jmap)

	return nil
}

func (q *Query) UpdateFromJSON(b []byte) error {
	// get a JSON map
	var jmap map[string]string
	if err := json.Unmarshal(b, &jmap); err != nil {
		return PTOWrapError(err)
	}

	q.setMetadata(jmap)

	return nil
}

func (q *Query) FlushMetadata() error {
	out, err := q.qc.writeMetadataFile(q.Identifier)
	if err != nil {
		return PTOWrapError(err)
	}

	b, err := q.MarshalJSON()
	if err != nil {
		return PTOWrapError(err)
	}

	if _, err = out.Write(b); err != nil {
		return PTOWrapError(err)
	}

	return nil
}

func (q *Query) writeResultFile() (*os.File, error) {
	return os.Create(filepath.Join(q.qc.config.QueryCacheRoot, fmt.Sprintf("%s.ndjson", q.Identifier)))
}

func (q *Query) ReadResultFile() (*os.File, error) {
	return os.Open(filepath.Join(q.qc.config.QueryCacheRoot, fmt.Sprintf("%s.ndjson", q.Identifier)))
}

func (q *Query) PaginateResultObject(offset int, count int) (map[string]interface{}, bool, error) {

	// create output object
	outData := make([]interface{}, 0)

	// open result file
	resultFile, err := q.ReadResultFile()
	if err != nil {
		return nil, false, PTOWrapError(err)
	}
	defer resultFile.Close()

	// attempt to seek to offset
	lineno := 0
	resultScanner := bufio.NewScanner(resultFile)
	for resultScanner.Scan() {
		lineno++

		if offset >= lineno {
			continue
		}
		if lineno > offset+count {
			break
		}

		// unmarshal data from JSON and add to output
		var lineData interface{}
		if err := json.Unmarshal([]byte(resultScanner.Text()), &lineData); err != nil {
			return nil, false, PTOWrapError(err)
		}
		outData = append(outData, lineData)
	}

	out := make(map[string]interface{})
	out[q.resultObjectLabel()] = outData

	return out, lineno > offset+count, nil
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

	// values
	if len(q.selectValues) > 0 {
		pq = pq.WhereGroup(func(qq *orm.Query) (*orm.Query, error) {
			for _, val := range q.selectValues {
				qq = qq.WhereOr("value = ?", val)
			}
			return qq, nil
		})
	}

	// source
	if len(q.selectSources) > 0 {
		pq = pq.WhereGroup(func(qq *orm.Query) (*orm.Query, error) {
			for _, src := range q.selectSources {
				qq = qq.WhereOr("path.source = ?", src)
			}
			return qq, nil
		})
	}

	// target
	if len(q.selectTargets) > 0 {
		pq = pq.WhereGroup(func(qq *orm.Query) (*orm.Query, error) {
			for _, tgt := range q.selectTargets {
				qq = qq.WhereOr("path.target = ?", tgt)
			}
			return qq, nil
		})
	}

	// on path
	if len(q.selectOnPath) > 0 {
		pq = pq.WhereGroup(func(qq *orm.Query) (*orm.Query, error) {
			for _, onpath := range q.selectOnPath {
				qq = qq.WhereOr("position(? in path.string) > 0", onpath)
			}
			return qq, nil
		})
	}

	return pq
}

// selectAndStoreObservations selects observations from this query and dumps
// them to the data file for this query as an NDJSON observation file.
func (q *Query) selectAndStoreObservations() error {
	var obsdat []Observation

	pq := q.qc.db.Model(&obsdat).Column("observation.*", "Condition", "Path")
	pq = q.whereClauses(pq)
	if err := pq.Select(); err != nil {
		return PTOWrapError(err)
	}

	outfile, err := q.writeResultFile()
	if err != nil {
		return err
	}
	defer outfile.Close()

	if err := WriteObservations(obsdat, outfile); err != nil {
		return err
	}

	return outfile.Sync()
}

// selectObservationSetIDs selects observation set IDs responding to
// this query.
func (q *Query) selectObservationSetIDs() ([]int, error) {
	var setids []int

	pq := q.qc.db.Model(&setids).ColumnExpr("DISTINCT set_id")
	pq = q.whereClauses(pq)
	if err := pq.Select(); err != nil {
		return nil, PTOWrapError(err)
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

	outfile, err := q.writeResultFile()
	if err != nil {
		return err
	}
	defer outfile.Close()

	for _, setid := range setids {
		if _, err := fmt.Fprintf(outfile, "\"%s\"\n", LinkForSetID(q.qc.config, setid)); err != nil {
			return err
		}
	}

	return outfile.Sync()
}

func joinGroupExtTable(q *orm.Query, extTable string) *orm.Query {
	switch extTable {
	case "conditions":
		return q.Join("JOIN conditions AS condition ON condition.id = observation.condition_id")
	case "paths":
		return q.Join("JOIN paths AS path ON path.id = observation.path_id")
	case "":
		return q
	default:
		panic("internal error: attempt to join unjoinable external table while grouping")
	}
}

func (q *Query) selectAndStoreOneGroup() error {

	var results []struct {
		tableName struct{} `sql:"observations,alias:observation"` // OMG this is a freaking hack
		Group0    string
		Count     int
	}

	var countClause string
	if q.optionCountDistinctTargets {
		countClause = "count(distinct path.target)"
	} else {
		countClause = "count(*)"
	}

	pq := q.qc.db.Model(&results).ColumnExpr(q.groups[0].ColumnSpec() + " as group0, " + countClause)

	// add join clause if necessary
	joinedPaths := false
	if q.optionCountDistinctTargets {
		pq = joinGroupExtTable(pq, "paths")
		joinedPaths = true
	}

	sgs, ok := q.groups[0].(*SimpleGroupSpec)
	if ok && sgs.ExtTable != "" {
		if sgs.ExtTable != "paths" || !joinedPaths {
			pq = joinGroupExtTable(pq, sgs.ExtTable)
		}
	}

	// now group
	pq = q.whereClauses(pq).Group("group0")
	if err := pq.Select(); err != nil {
		return PTOWrapError(err)
	}

	outfile, err := q.writeResultFile()
	if err != nil {
		return err
	}
	defer outfile.Close()

	for _, result := range results {
		out := make([]interface{}, 2)
		out[0] = result.Group0
		out[1] = result.Count

		b, err := json.Marshal(out)
		if err != nil {
			return PTOWrapError(err)
		}

		if _, err := fmt.Fprintf(outfile, "%s\n", b); err != nil {
			return PTOWrapError(err)
		}
	}

	return outfile.Sync()
}

func (q *Query) selectAndStoreTwoGroups() error {

	var results []struct {
		tableName struct{} `sql:"observations,alias:observation"` // OMG this is a freaking hack
		Group0    string
		Group1    string
		Count     int
	}

	var countClause string
	if q.optionCountDistinctTargets {
		countClause = "count(distinct path.target)"
	} else {
		countClause = "count(*)"
	}

	pq := q.qc.db.Model(&results).ColumnExpr(
		q.groups[0].ColumnSpec() + " as group0, " +
			q.groups[1].ColumnSpec() + "as group1, " + countClause)

	// now join as necessary
	extTableSet := make(map[string]struct{})

	if q.optionCountDistinctTargets {
		extTableSet["paths"] = struct{}{}
	}

	for i := 0; i < 2; i++ {
		sgs, ok := q.groups[i].(*SimpleGroupSpec)
		if ok && sgs.ExtTable != "" {
			extTableSet[sgs.ExtTable] = struct{}{}
		}
	}

	for k := range extTableSet {
		pq = joinGroupExtTable(pq, k)
	}

	// and group
	pq = q.whereClauses(pq).Group("group0").Group("group1")
	if err := pq.Select(); err != nil {
		return PTOWrapError(err)
	}

	outfile, err := q.writeResultFile()
	if err != nil {
		return err
	}
	defer outfile.Close()

	for _, result := range results {
		out := make([]interface{}, 3)
		out[0] = result.Group0
		out[1] = result.Group1
		out[2] = result.Count

		b, err := json.Marshal(out)
		if err != nil {
			return PTOWrapError(err)
		}

		if _, err := fmt.Fprintf(outfile, "%s\n", b); err != nil {
			return PTOWrapError(err)
		}
	}

	return outfile.Sync()
}

// selectAndStoreGroups selects groups responding to this query and dumps them
// to the data file as NDJSON, one line containing a JSON array per group,
// with elements 0 to n-1 being group names, and element n being the count of
// observations in the group.
func (q *Query) selectAndStoreGroups() error {
	switch len(q.groups) {
	case 0:
		panic("Programmer error: Query.selectAndStoreGroups() called on a non-group query")
	case 1:
		return q.selectAndStoreOneGroup()
	case 2:
		return q.selectAndStoreTwoGroups()
	default:
		return PTOErrorf("Group by more than two dimensions not presently supported").StatusIs(http.StatusBadRequest)
	}
}

func (q *Query) executionFunc() func() error {
	if len(q.groups) > 0 {
		return q.selectAndStoreGroups
	} else if q.optionSetsOnly {
		return q.selectAndStoreObservationSetLinks
	} else {
		return q.selectAndStoreObservations
	}
}

func (q *Query) resultObjectLabel() string {
	if len(q.groups) > 0 {
		return "groups"
	} else if q.optionSetsOnly {
		return "sets"
	} else {
		return "obs"
	}
}

func (q *Query) ExecuteWaitImmediate(done chan struct{}) {
	// start the immediate delay timer
	itimer := time.NewTimer(time.Duration(q.qc.config.ImmediateQueryDelay) * time.Millisecond)

	// start the query
	q.Execute(done)

	// wait for either the done timer or the immediate timer
	select {
	case <-itimer.C:
	case <-done:
	}
}

func (q *Query) Execute(done chan struct{}) {
	// fire off a goroutine to actually run the query
	go func() {
		// grab a token
		q.qc.exectokens <- struct{}{}

		// mark query as executing
		startTime := time.Now()
		q.Executed = &startTime

		// flush to disk
		q.FlushMetadata()

		// switch and run query
		q.ExecutionError = q.executionFunc()()

		// mark query as done
		endTime := time.Now()
		q.Completed = &endTime

		// flush to disk
		q.FlushMetadata()

		// return the waitgroup token
		<-q.qc.exectokens

		// and notify that we're done
		close(done)
	}()
}
