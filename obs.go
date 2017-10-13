package pto3

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

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

	// FIXME echo back the inserted set
	w.WriteHeader(http.StatusOK)
}

func (osr *ObservationStore) HandleGetMetadata(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
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