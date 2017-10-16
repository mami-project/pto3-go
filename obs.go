package pto3

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

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

func (osr *ObservationStore) writeMetadataResponse(w http.ResponseWriter, set *ObservationSet, status int) {
	// compute a link for the observation set
	set.LinkVia(osr.config.BaseURL)

	// now write it to the response
	b, err := json.Marshal(&set)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	w.Write(b)
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

	osr.writeMetadataResponse(w, &set, http.StatusCreated)
}

// HandleGetMetadata handles Get /obs/<set>. It writes a JSON object with
// observation set metadata in the response.
func (osr *ObservationStore) HandleGetMetadata(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !osr.azr.IsAuthorized(w, r, "read_obs") {
		return
	}

	vars := mux.Vars(r)

	// get set ID
	setid, err := strconv.ParseInt(vars["set"], 16, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad or missing set ID %s: %s", vars["set"], err.Error()), http.StatusBadRequest)
		return
	}

	set := ObservationSet{ID: int(setid)}
	if err := osr.db.Select(&set); err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	osr.writeMetadataResponse(w, &set, http.StatusOK)
}

// HandlePutMetadata handles POST /obs/create. It requires a JSON object with
// observation set metadata in the request. It echoes back the metadata as a
// JSON object in the response,
func (osr *ObservationStore) HandlePutMetadata(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !osr.azr.IsAuthorized(w, r, "write_obs") {
		return
	}

	vars := mux.Vars(r)

	// fill in set ID from URL
	setid, err := strconv.ParseInt(vars["set"], 16, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad or missing set ID %s: %s", vars["set"], err.Error()), http.StatusBadRequest)
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
	set.ID = int(setid)

	// now update
	err = osr.db.RunInTransaction(func(t *pg.Tx) error {
		return set.Update(t)
	})
	if err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	osr.writeMetadataResponse(w, &set, http.StatusCreated)
}

func (osr *ObservationStore) HandleDownload(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

// HandleUpload handles PUT /obs/create/data. It requires a newline-delimited
// JSON stream (of content-type application/vnd.mami.ndjson) in observation set
// file format. Set IDs in the input are ignored. It writes a response
// containing the set's metadata.

func (osr *ObservationStore) HandleUpload(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !osr.azr.IsAuthorized(w, r, "write_obs") {
		return
	}

	vars := mux.Vars(r)

	// fill in set ID from URL
	setid, err := strconv.ParseInt(vars["set"], 16, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad or missing set ID %s: %s", vars["set"], err.Error()), http.StatusBadRequest)
		return
	}

	// retrieve set metadata
	set := ObservationSet{ID: int(setid)}
	if err := osr.db.Select(&set); err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// fail if observations exist
	if set.CountObservations(osr.db) != 0 {
		http.Error(w, fmt.Sprintf("Observation set %s already uploaded", vars["set"]), http.StatusBadRequest)
	}

	// now scan the input looking for observations
	in := bufio.NewScanner(r.Body)
	var obs Observation

	err = osr.db.RunInTransaction(func(t *pg.Tx) error {
		for in.Scan() {
			if err := json.Unmarshal([]byte(in.Text()), &obs); err != nil {
				return err
			}
			if err = obs.InsertInSet(t, &set); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// now update observation count
	set.CountObservations(osr.db)

	// and write
	osr.writeMetadataResponse(w, &set, http.StatusCreated)
}

func (osr *ObservationStore) CreateTables() error {
	return CreateTables(osr.db)
}

func (osr *ObservationStore) DropTables() error {
	return DropTables(osr.db)
}

func (osr *ObservationStore) EnableQueryLogging() {
	osr.db.OnQueryProcessed(func(event *pg.QueryProcessedEvent) {
		query, err := event.FormattedQuery()
		if err != nil {
			panic(err)
		}

		log.Printf("%s %s", time.Since(event.StartTime), query)
	})
}

func (osr *ObservationStore) AddRoutes(r *mux.Router) {
	r.HandleFunc("/obs", osr.HandleListSets).Methods("GET")
	r.HandleFunc("/obs/create", osr.HandleCreateSet).Methods("POST")
	r.HandleFunc("/obs/{set}", osr.HandleGetMetadata).Methods("GET")
	r.HandleFunc("/obs/{set}", osr.HandlePutMetadata).Methods("PUT")
	r.HandleFunc("/obs/{set}/data", osr.HandleDownload).Methods("GET")
	r.HandleFunc("/obs/{set}/data", osr.HandleUpload).Methods("PUT")
}
