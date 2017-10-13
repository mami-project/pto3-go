package pto3

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/go-pg/pg"
	"github.com/gorilla/mux"
)

type ObservationStore struct {
	config *PTOServerConfig
	azr    *Authorizer
	db     *pg.DB
}

func NewObservationStore(config *PTOServerConfig, azr *Authorizer) (*ObservationStore, error) {
	osr := ObservationStore{config: config, azr: azr}

	// Connect to database
	osr.db = pg.Connect(&config.ObsDatabase)

	return &osr, nil
}

func (osr *ObservationStore) initDB() error {
	err := osr.db.CreateTable(&ObservationSet{}, nil)
	if err != nil {
		return err
	}

	err = osr.db.CreateTable(&Observation{}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (osr *ObservationStore) HandleListSets(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

// HandleCreateSet handles POST /obs/create. It requires a JSON object with
// observation set metadata in the request. It echoes back the metadata as a
// JSON object in the response, with a link to the created object in the __link
// metadata key.
func (osr *ObservationStore) HandleCreateSet(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !osr.azr.IsAuthorized(w, r, "write_obs") {
		return
	}

	// fail if not JSON
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, fmt.Sprintf("Content-type for metadata must be application/json; got %s instead",
			r.Header.Get("Content-Type")), http.StatusUnsupportedMediaType)
		return
	}

	// fill in an observation set from supplied metadata
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var set ObservationSet
	if err := json.Unmarshal(b, &set); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// now insert the set in the database
	err = osr.db.RunInTransaction(func(t *pg.Tx) error {
		return set.Insert(t, true)
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// compute a link for it
	set.LinkVia(osr.config.BaseURL)

	// and echo back the set, including the link
	b, err = json.Marshal(&set)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write(b)
}

// HandleCreateSet handles POST /obs/create. It requires a JSON object with
// observation set metadata in the request. It echoes back the metadata as a
// JSON object in the response, with a link to the created object in the __link
// metadata key.

func (osr *ObservationStore) HandleGetMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// get set ID
	setid, err := strconv.ParseInt(vars["set"], 16, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad or missing set ID %s: %s", vars["set"], err.Error()), http.StatusBadRequest)
		return
	}

	set := ObservationSet{ID: int(setid)}
	if err := osr.db.Select(&set); err != nil {
		// FIXME distinguish not found from some other database error
	}

	// compute a link for the set
	set.LinkVia(osr.config.BaseURL)

	// and send it on
	b, err := json.Marshal(&set)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func (osr *ObservationStore) HandlePutMetadata(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

func (osr *ObservationStore) HandleUpload(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

func (osr *ObservationStore) HandleDownload(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

func (osr *ObservationStore) AddRoutes(r *mux.Router) {
	r.HandleFunc("/obs", osr.HandleListSets).Methods("GET")
	r.HandleFunc("/obs/create", osr.HandleCreateSet).Methods("POST")
	r.HandleFunc("/obs/{set}", osr.HandleGetMetadata).Methods("GET")
	r.HandleFunc("/obs/{set}", osr.HandlePutMetadata).Methods("PUT")
	r.HandleFunc("/obs/{set}/data", osr.HandleDownload).Methods("GET")
	r.HandleFunc("/obs/{set}/data", osr.HandleUpload).Methods("PUT")
}
