package papi

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
)

func (config *PTOConfiguration) HandleRoot(w http.ResponseWriter, r *http.Request) {

	links := make(map[string]string)

	if config.RawRoot != "" {
		rawrel, _ := url.Parse("raw")
		links["raw"] = config.baseURL.ResolveReference(rawrel).String()
	}

	if config.ObsDatabase.Database != "" {
		obsrel, _ := url.Parse("obs")
		links["obs"] = config.baseURL.ResolveReference(obsrel).String()
	}

	linksj, err := json.Marshal(links)

	if err != nil {
		LogInternalServerError(w, "marshaling root link list", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(linksj)
}

func (config *PTOConfiguration) AddRoutes(r *mux.Router) {
	r.HandleFunc("/", LogAccess(config.accessLogger, config.HandleRoot)).Methods("GET")
}
