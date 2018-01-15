package papi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

type QueryAPI struct {
	qc  pto3.QueryCache
	azr Authorizer
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

func (qa *QueryAPI) handleList(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func (qa *QueryAPI) handleSubmit(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !qa.azr.IsAuthorized(w, r, "submit_query") {
		return
	}

	// Submit the query; this will give us an existing query if it's already in the cache
	if err := r.ParseForm(); err != nil {
		http.Error(w, "error parsing form", http.StatusBadRequest)
	}

	q, isnew, err := qa.qc.SubmitQueryFromForm(r.Form)
	if err != nil {
		pto3.HandleErrorHTTP(w, "parsing query", err)
	}

	// Serialize query
	status := http.StatusOK
	if isnew {
		status = http.StatusAccepted
	}

	queryResponse(w, status, q)
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
	}

	queryResponse(w, http.StatusOK, q)
}

func (qa *QueryAPI) handlePutMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

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
	}

	if err := q.FlushMetadata(); err != nil {
		pto3.HandleErrorHTTP(w, "writing query metadata", err)
	}

	queryResponse(w, http.StatusOK, q)
}

func (qa *QueryAPI) handleGetResults(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)

}

func (qa *QueryAPI) addRoutes(r *mux.Router, l *log.Logger) {
	r.HandleFunc("/query", LogAccess(l, qa.handleList)).Methods("GET")
	r.HandleFunc("/query/submit", LogAccess(l, qa.handleSubmit)).Methods("POST")
	r.HandleFunc("/query/{query}", LogAccess(l, qa.handleGetMetadata)).Methods("GET")
	r.HandleFunc("/query/{query}", LogAccess(l, qa.handlePutMetadata)).Methods("PUT")
	r.HandleFunc("/query/{query}/data", LogAccess(l, qa.handleGetResults)).Methods("GET")
}
