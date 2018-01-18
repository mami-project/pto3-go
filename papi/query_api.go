package papi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

type QueryAPI struct {
	config *pto3.PTOConfiguration
	qc     *pto3.QueryCache
	azr    Authorizer
}

func queryResponse(w http.ResponseWriter, status int, q *pto3.Query) {
	b, err := json.Marshal(q)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshalling query", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
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
	if !qa.azr.IsAuthorized(w, r, "list_query") {
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
	w.WriteHeader(http.StatusOK)
	w.Write(outb)
}

func (qa *QueryAPI) handleSubmit(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !qa.azr.IsAuthorized(w, r, "submit_query") {
		return
	}

	// execute query, but don't wait for it beyond the immediate wait.
	// This will give us an existing query if it's already in the cache.
	if err := r.ParseForm(); err != nil {
		http.Error(w, "error parsing form", http.StatusBadRequest)
	}

	q, _, err := qa.qc.ExecuteQueryFromForm(r.Form, make(chan struct{}))
	if err != nil {
		pto3.HandleErrorHTTP(w, "parsing query", err)
		return
	}

	queryResponse(w, http.StatusOK, q)
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

	queryResponse(w, http.StatusOK, q)
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

	queryResponse(w, http.StatusOK, q)
}

func (qa *QueryAPI) handleGetResults(w http.ResponseWriter, r *http.Request) {
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
	}

	if page > 1 {
		prevLink, _ := qa.config.LinkTo(fmt.Sprintf("/query/%s/result?page=%d", q.Identifier, page-1))
		robj["prev"] = prevLink
	}

	outb, err := json.Marshal(robj)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshaling result", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(outb)
}

func (qa *QueryAPI) addRoutes(r *mux.Router, l *log.Logger) {
	r.HandleFunc("/query", LogAccess(l, qa.handleList)).Methods("GET")
	r.HandleFunc("/query/submit", LogAccess(l, qa.handleSubmit)).Methods("GET", "POST")
	r.HandleFunc("/query/{query}", LogAccess(l, qa.handleGetMetadata)).Methods("GET")
	r.HandleFunc("/query/{query}", LogAccess(l, qa.handlePutMetadata)).Methods("PUT")
	r.HandleFunc("/query/{query}/data", LogAccess(l, qa.handleGetResults)).Methods("GET")
}
