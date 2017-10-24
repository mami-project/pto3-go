package pto3

import (
	"errors"
	"fmt"
	"net/http"
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

func NewQueryFromForm(form map[string][]string, db orm.DB) (*Query, error) {
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
			conditions, err := ConditionsByName(conditionStr, db)
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
		q.groups = make([]GroupSpec, 0)
		for _, groupStr := range groupStrs {
			switch groupStr {
			case "year":
				// FIXME determine how to group by year of start date
			case "month":
				// FIXME determine how to group by month of start date
			case "week":
				// FIXME determine how to group by week of start date
			case "day":
				// FIXME determine how to group by day of start date
			case "hour":
				// FIXME determine how to group by hour of start date
			case "week_day":
				// FIXME determine how to group by weekday of start date
			case "day_hour":
				// FIXME determine how to group by day hour of start date
			case "condition":
				q.groups = append(q.groups, &SimpleGroupSpec{ColumnName: "condition"})
			case "source":
				// FIXME denormalize model to make source groups work
			case "target":
				// FIXME denormalize model to make target groups work
			}
		}
	}

	// FIXME parse intersect conditions

	// FIXME parse options

	// hash everything into an identifier
	q.generateIdentifier()

	return &q, nil
}

func NewQueryFromURLEncoded(urlencoded string, db *orm.DB) (*Query, error) {
	return nil, nil
}

func (spec *Query) URLEncoded() string {
	// generate query specification as a URL
	return ""
}

func (spec *Query) generateIdentifier() {
	// normalize form of the specification and generate a query identifier
}

func (spec *Query) selectObservations() ([]Observation, error) {
	return nil, nil
}

func (spec *Query) selectObservationSetIDs() ([]int, error) {
	return nil, nil
}

func (spec *Query) selectGroups() (map[string]int, error) {
	return nil, nil
}

func (spec *Query) selectIntersectPaths() ([]Path, error) {
	return nil, nil
}

func (spec *Query) Execute() error {
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
	// FIXME are these flat files? or is this a JSON table?
	path string

	// Submitted queries not yet running
	submitted []*Query

	// Currently cached queries (includes those executing)
	cached map[string]*Query
}

func NewQueryCache(config *PTOServerConfig, azr *Authorizer) error {
	return nil
}

func (qc *QueryCache) HandleList(w http.ResponseWriter, r *http.Request) {

}

func (qc *QueryCache) HandleSubmit(w http.ResponseWriter, r *http.Request) {
	// Create a query specifier

	// Stick it in queue

	// Q: do we need a context here?
}

func (qc *QueryCache) HandleGetMetadata(w http.ResponseWriter, r *http.Request) {

}

func (qc *QueryCache) HandlePutMetadata(w http.ResponseWriter, r *http.Request) {

}

func (qc *QueryCache) HandleGetResults(w http.ResponseWriter, r *http.Request) {

}

func (qc *QueryCache) AddRoutes(r *mux.Router) {
	r.HandleFunc("/query", qc.HandleList).Methods("GET")
	r.HandleFunc("/query/submit", qc.HandleSubmit).Methods("POST")
	r.HandleFunc("/query/{query}", qc.HandleGetMetadata).Methods("GET")
	r.HandleFunc("/query/{query}", qc.HandlePutMetadata).Methods("PUT")
	r.HandleFunc("/query/{query}/data", qc.HandleGetResults).Methods("GET")
}
