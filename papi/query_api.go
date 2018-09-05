package papi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

type QueryAPI struct {
	config *pto3.PTOConfiguration
	qc     *pto3.QueryCache
	azr    Authorizer
}

func (qa *QueryAPI) queryResponse(w http.ResponseWriter, status int, q *pto3.Query) {
	b, err := json.Marshal(q)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshalling query", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	qa.additionalHeaders(w)
	w.WriteHeader(status)
	w.Write(b)
}

type queryList struct {
	Queries []string `json:"queries"`
}

func (qa *QueryAPI) handleList(w http.ResponseWriter, r *http.Request) {
	// FIXME this isn't terribly useful (See #25)
	// there should at least be a way to list pending queries only,
	// and completed queries only, but this would require the cache
	// to keep everything in memory. investigate this after we get things running.

	// fail if not authorized
	if !qa.azr.IsAuthorized(w, r, "read_query") {
		return
	}

	// grab links and stuff them in JSON.
	links, err := qa.qc.CachedQueryLinks()
	if err != nil {
		pto3.HandleErrorHTTP(w, "scanning cached queries", err)
		return
	}

	out := queryList{Queries: links}

	outb, err := json.Marshal(out)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshaling query list", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	qa.additionalHeaders(w)
	w.WriteHeader(http.StatusOK)
	w.Write(outb)
}

func (qa *QueryAPI) authorizedToSubmit(w http.ResponseWriter, r *http.Request, form url.Values) bool {
	// check by query type
	perm := "submit_query_obs"

	if _, ok := form["group"]; ok {
		perm = "submit_query_group"
	}

	return qa.azr.IsAuthorized(w, r, perm)
}

func (qa *QueryAPI) handleSubmit(w http.ResponseWriter, r *http.Request) {

	// Parse the form (we need this to check authorization)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "error parsing form", http.StatusBadRequest)
	}

	// fail if not authorized
	if !qa.authorizedToSubmit(w, r, r.Form) {
		return
	}

	// execute query, but don't wait for it beyond the immediate wait.
	// This will give us an existing query if it's already in the cache.
	q, _, err := qa.qc.ExecuteQueryFromForm(r.Form, make(chan struct{}))
	if err != nil {
		pto3.HandleErrorHTTP(w, "parsing query", err)
		return
	}

	qa.queryResponse(w, http.StatusOK, q)
}

func (qa *QueryAPI) handleRetrieve(w http.ResponseWriter, r *http.Request) {

	// Parse the form (we need this to check authorization)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "error parsing form", http.StatusBadRequest)
	}

	// fail if not authorized
	if !qa.azr.IsAuthorized(w, r, "read_query") {
		return
	}

	// parse the query and try to retrieve it by value
	q, err := qa.qc.ParseQueryFromForm(r.Form)
	if err != nil {
		pto3.HandleErrorHTTP(w, "parsing query", err)
		return
	}

	// check to see if it's been cached
	oq, err := qa.qc.QueryByIdentifier(q.Identifier)
	if err != nil {
		pto3.HandleErrorHTTP(w, "retrieving query", err)
		return
	}

	qa.queryResponse(w, http.StatusOK, oq)
}

func (qa *QueryAPI) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	qid, ok := vars["query"]
	if !ok {
		http.Error(w, "missing query", http.StatusBadRequest)
		return
	}

	// fail if not authorized
	if !qa.azr.IsAuthorized(w, r, "read_query") {
		return
	}

	// get query metadata
	q, err := qa.qc.QueryByIdentifier(qid)
	if err != nil {
		pto3.HandleErrorHTTP(w, "fetching query", err)
		return
	}

	qa.queryResponse(w, http.StatusOK, q)
}

func (qa *QueryAPI) handlePutMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	if err := r.ParseForm(); err != nil {
		http.Error(w, "error parsing form", http.StatusBadRequest)
	}

	qid, ok := vars["query"]
	if !ok {
		http.Error(w, "missing query", http.StatusBadRequest)
		return
	}

	// fail if not authorized
	if !qa.azr.IsAuthorized(w, r, "update_query") {
		return
	}

	// get query
	q, err := qa.qc.QueryByIdentifier(qid)
	if err != nil {
		pto3.HandleErrorHTTP(w, "fetching query", err)
	}

	// fail if not JSON
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, fmt.Sprintf("Content-type for query metadata must be application/json; got %s instead",
			r.Header.Get("Content-Type")), http.StatusUnsupportedMediaType)
		return
	}

	// update query with JSON
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	if err := q.UpdateFromJSON(b); err != nil {
		pto3.HandleErrorHTTP(w, "updating query metadata", err)
		return
	}

	// make sure the query is saved to disk
	if err := q.FlushMetadata(); err != nil {
		pto3.HandleErrorHTTP(w, "writing query metadata", err)
		return
	}

	qa.queryResponse(w, http.StatusOK, q)
}

func (qa *QueryAPI) handleGetResults(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	qid, ok := vars["query"]
	if !ok {
		http.Error(w, "missing query", http.StatusBadRequest)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "error parsing form", http.StatusBadRequest)
	}

	// fail if not authorized
	if !qa.azr.IsAuthorized(w, r, "read_query") {
		return
	}

	// get query
	q, err := qa.qc.QueryByIdentifier(qid)
	if err != nil {
		pto3.HandleErrorHTTP(w, "fetching query", err)
		return
	}

	// verify that the query thinks that it's completed
	if q.Completed == nil {
		http.Error(w, "results not available", http.StatusNotFound)
	}

	// get page number from query, default to zero
	page, _ := strconv.ParseInt(r.Form.Get("page"), 10, 64)

	// retrieve and paginate result
	robj, more, err := q.PaginateResultObject(int(page)*qa.config.PageLength, qa.config.PageLength)
	if err != nil {
		pto3.HandleErrorHTTP(w, "retrieving result", err)
		return
	}

	if more {
		nextLink, _ := qa.config.LinkTo(fmt.Sprintf("/query/%s/result?page=%d", q.Identifier, page+1))
		robj["next"] = nextLink
		robj["total_count"] = q.ResultRowCount()
	}

	if page > 0 {
		prevLink, _ := qa.config.LinkTo(fmt.Sprintf("/query/%s/result?page=%d", q.Identifier, page-1))
		robj["prev"] = prevLink
		robj["total_count"] = q.ResultRowCount()
	}

	outb, err := json.Marshal(robj)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshaling result", err)
	}

	w.Header().Set("Content-Type", "application/json")
	qa.additionalHeaders(w)
	w.WriteHeader(http.StatusOK)
	w.Write(outb)
}

func (qa *QueryAPI) additionalHeaders(w http.ResponseWriter) {
	if qa.config.AllowOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", qa.config.AllowOrigin)
	}
}

func (qa *QueryAPI) addRoutes(r *mux.Router, l *log.Logger) {
	r.HandleFunc("/query", LogAccess(l, qa.handleList)).Methods("GET")
	r.HandleFunc("/query/submit", LogAccess(l, qa.handleSubmit)).Methods("GET", "POST")
	r.HandleFunc("/query/retrieve", LogAccess(l, qa.handleRetrieve)).Methods("GET", "POST")
	r.HandleFunc("/query/{query}", LogAccess(l, qa.handleGetMetadata)).Methods("GET")
	r.HandleFunc("/query/{query}", LogAccess(l, qa.handlePutMetadata)).Methods("PUT")
	r.HandleFunc("/query/{query}/result", LogAccess(l, qa.handleGetResults)).Methods("GET")
}

func (qa *QueryAPI) LoadTestData(obsFilename string) (int, error) {
	return qa.qc.LoadTestData(obsFilename)
}

func (qa *QueryAPI) EnableQueryLogging() {
	qa.qc.EnableQueryLogging()
}

func NewQueryAPI(config *pto3.PTOConfiguration, azr Authorizer, r *mux.Router) (*QueryAPI, error) {

	if config.QueryCacheRoot == "" {
		return nil, nil
	}

	qa := new(QueryAPI)
	qa.config = config
	qa.azr = azr

	var err error
	qa.qc, err = pto3.NewQueryCache(config)
	if err != nil {
		return nil, err
	}

	qa.addRoutes(r, config.AccessLogger())

	return qa, nil
}
