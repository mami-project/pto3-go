package papi

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-pg/pg"
	"github.com/gorilla/mux"
)


// FIXME observation store, here, is a
type ObservationStore struct {
	config *PTOConfiguration
	azr    Authorizer
	db     *pg.DB
}

func NewObservationStore(config *PTOConfiguration, azr Authorizer) (*ObservationStore, error) {
	osr := ObservationStore{config: config, azr: azr}

	// Connect to database
	osr.db = pg.Connect(&config.ObsDatabase)

	return &osr, nil
}

func (osr *ObservationStore) writeMetadataResponse(w http.ResponseWriter, set *ObservationSet, status int) {
	// compute a link for the observation set
	set.LinkVia(osr.config.baseURL)

	// now write it to the response
	b, err := json.Marshal(&set)
	if err != nil {
		LogInternalServerError(w, "marshaling metadata", err)
		return
	}
	w.WriteHeader(status)
	w.Write(b)
}

type setList struct {
	Sets []string `json:"sets"`
}

// HandleListSets handles GET /obs.
// It returns a JSON object with links to current observation sets in the sets key.
func (osr *ObservationStore) HandleListSets(w http.ResponseWriter, r *http.Request) {
	var setIds []int

	// select set IDs into an array
	// FIXME this should go into model.go
	if err := osr.db.Model(&ObservationSet{}).ColumnExpr("array_agg(id)").Select(pg.Array(&setIds)); err != nil && err != pg.ErrNoRows {
		LogInternalServerError(w, "listing set IDs", err)
		return
	}

	// linkify them
	sets := setList{make([]string, len(setIds))}
	for i, id := range setIds {
		sets.Sets[i] = LinkForSetID(osr.config.baseURL, id)
	}

	// FIXME pagination goes here
	outb, err := json.Marshal(sets)
	if err != nil {
		LogInternalServerError(w, "marshaling set list", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(outb)
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
		// then insert the set itself
		return set.Insert(t, true)
	})
	if err != nil {
		log.Print(err)
		LogInternalServerError(w, "inserting set record", err)
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
	setid, err := strconv.ParseUint(vars["set"], 16, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad or missing set ID %s: %s", vars["set"], err.Error()), http.StatusBadRequest)
		return
	}

	set := ObservationSet{ID: int(setid)}
	if err = set.SelectByID(osr.db); err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			LogInternalServerError(w, "retrieving set", err)
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
	setid, err := strconv.ParseUint(vars["set"], 16, 64)
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
			LogInternalServerError(w, "updating set metadata", err)
		}
		return
	}

	osr.writeMetadataResponse(w, &set, http.StatusCreated)
}

// HandleDownload handles GET /obs/<set>/data. It requires  Set IDs in the input are ignored. It writes a response
// containing the all the observations in the set as a newline-delimited
// JSON stream (of content-type application/vnd.mami.ndjson) in observation set
// file format.

func (osr *ObservationStore) HandleDownload(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !osr.azr.IsAuthorized(w, r, "write_obs") {
		return
	}

	vars := mux.Vars(r)

	// fill in set ID from URL
	setid, err := strconv.ParseUint(vars["set"], 16, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad or missing set ID %s: %s", vars["set"], err.Error()), http.StatusBadRequest)
		return
	}

	// retrieve set metadata
	set := ObservationSet{ID: int(setid)}
	if err = set.SelectByID(osr.db); err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			LogInternalServerError(w, "retrieving set", err)
		}
		return
	}

	// fail if no observations exist
	if set.CountObservations(osr.db) == 0 {
		http.Error(w, fmt.Sprintf("Observation set %s has no observations", vars["set"]), http.StatusNotFound)
		return
	}

	// now select all the observations
	// FIXME this sucks the whole obset into RAM, which is fast but probably not great.
	// Figure out how to stream this. Might require another library

	// FIXME shouldn't this funtionality be in model.go?

	obsdat, err := ObservationsBySetID(osr.db, int(setid))
	if err != nil {
		LogInternalServerError(w, "retrieving observation set", err)
		return
	}

	w.Header().Set("Content-type", "application/vnd.mami.ndjson")
	w.WriteHeader(http.StatusOK)

	if err := WriteObservations(obsdat, w); err != nil {
		LogInternalServerError(w, "writing observation set on download", err)
		w.Write([]byte("\"error during download\"\n"))
		return
	}
}

// HandleUpload handles PUT /obs/<set>/data. It requires a newline-delimited
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
	setid, err := strconv.ParseUint(vars["set"], 16, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("bad or missing set ID %s: %s", vars["set"], err.Error()), http.StatusBadRequest)
		return
	}

	// retrieve set metadata
	set := ObservationSet{ID: int(setid)}
	if err := set.SelectByID(osr.db); err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			LogInternalServerError(w, "retrieving set metadata", err)
		}
		return
	}

	// fail if observations exist
	if set.CountObservations(osr.db) != 0 {
		http.Error(w, fmt.Sprintf("Observation set %s already uploaded", vars["set"]), http.StatusBadRequest)
		return
	}

	// now scan the input looking for observations, streaming them into the database
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
		LogInternalServerError(w, "inserting observation data", err)
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
	l := osr.config.accessLogger
	r.HandleFunc("/obs", LogAccess(l, osr.HandleListSets)).Methods("GET")
	r.HandleFunc("/obs/create", LogAccess(l, osr.HandleCreateSet)).Methods("POST")
	r.HandleFunc("/obs/{set}", LogAccess(l, osr.HandleGetMetadata)).Methods("GET")
	r.HandleFunc("/obs/{set}", LogAccess(l, osr.HandlePutMetadata)).Methods("PUT")
	r.HandleFunc("/obs/{set}/data", LogAccess(l, osr.HandleDownload)).Methods("GET")
	r.HandleFunc("/obs/{set}/data", LogAccess(l, osr.HandleUpload)).Methods("PUT")
}
