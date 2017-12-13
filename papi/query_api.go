package papi

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

type QueryAPI struct {
	qc pto3.QueryCache
}

func (qa *QueryAPI) handleList(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func (qa *QueryAPI) handleSubmit(w http.ResponseWriter, r *http.Request) {
	// Create a query specifier

	// Stick it in queue

	// Q: do we need a context here?
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func (qa *QueryAPI) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)

}

func (qa *QueryAPI) handlePutMetadata(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)

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
