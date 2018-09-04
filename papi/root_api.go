package papi

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

type RootAPI struct {
	config *pto3.PTOConfiguration
}

func (ra *RootAPI) additionalHeaders(w http.ResponseWriter) {
	if ra.config.AllowOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", ra.config.AllowOrigin)
	}
}

func (ra *RootAPI) handleRoot(w http.ResponseWriter, r *http.Request) {

	links := make(map[string]string)

	links["banner"] = "This is an instance of the MAMI Path Transparency Observatory. See https://github.com/mami-project/pto3-go for more information."

	if ra.config.RawRoot != "" {
		links["raw"], _ = ra.config.LinkTo("raw")
	}

	if ra.config.ObsDatabase.Database != "" {
		links["obs"], _ = ra.config.LinkTo("obs")
	}

	if ra.config.QueryCacheRoot != "" {
		links["query"], _ = ra.config.LinkTo("query")
	}

	linksj, err := json.Marshal(links)

	if err != nil {
		pto3.HandleErrorHTTP(w, "marshaling root link list", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	ra.additionalHeaders(w)
	w.WriteHeader(http.StatusOK)
	w.Write(linksj)
}

func (ra *RootAPI) noMoreCORS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Request-Headers", "Authorization")
	w.Header().Set("Access-Control-Request-Method", "POST")
	w.Header().Set("Access-Control-Allow-Origin", ra.config.AllowOrigin)
	w.WriteHeader(http.StatusOK)
}

func (ra *RootAPI) addRoutes(r *mux.Router, l *log.Logger) {
	r.HandleFunc("/", LogAccess(l, ra.handleRoot)).Methods("GET")
	r.PathPrefix("/").Methods("OPTIONS").HandlerFunc(LogAccess(l, ra.noMoreCORS))
}

func NewRootAPI(config *pto3.PTOConfiguration, azr Authorizer, r *mux.Router) *RootAPI {
	ra := new(RootAPI)
	ra.config = config
	ra.addRoutes(r, config.AccessLogger())
	return ra
}
