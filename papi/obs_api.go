package papi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-pg/pg"
	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

type ObsAPI struct {
	config *pto3.PTOConfiguration
	azr    Authorizer
	db     *pg.DB
}

func (oa *ObsAPI) writeMetadataResponse(w http.ResponseWriter, set *pto3.ObservationSet, status int) {
	// compute a link for the observation set
	set.LinkVia(oa.config.baseURL)

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

// handleListSets handles GET /obs.
// It returns a JSON object with links to current observation sets in the sets key.
func (oa *ObsAPI) handleListSets(w http.ResponseWriter, r *http.Request) {
	var setIds []int

	// select set IDs into an array
	// FIXME this should go into model.go
	if err := oa.db.Model(&ObservationSet{}).ColumnExpr("array_agg(id)").Select(pg.Array(&setIds)); err != nil && err != pg.ErrNoRows {
		LogInternalServerError(w, "listing set IDs", err)
		return
	}

	// linkify them
	sets := setList{make([]string, len(setIds))}
	for i, id := range setIds {
		sets.Sets[i] = LinkForSetID(oa.config.baseURL, id)
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

// handleCreateSet handles POST /obs/create. It requires a JSON object with
// observation set metadata in the request. It echoes back the metadata as a
// JSON object in the response, with a link to the created object in the __link
// metadata key.
func (oa *ObsAPI) handleCreateSet(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !oa.azr.IsAuthorized(w, r, "write_obs") {
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
	err = oa.db.RunInTransaction(func(t *pg.Tx) error {
		// then insert the set itself
		return set.Insert(t, true)
	})
	if err != nil {
		log.Print(err)
		LogInternalServerError(w, "inserting set record", err)
		return
	}

	oa.writeMetadataResponse(w, &set, http.StatusCreated)
}

// handleGetMetadata handles Get /obs/<set>. It writes a JSON object with
// observation set metadata in the response.
func (oa *ObsAPI) handleGetMetadata(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !oa.azr.IsAuthorized(w, r, "read_obs") {
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
	if err = set.SelectByID(oa.db); err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			LogInternalServerError(w, "retrieving set", err)
		}
		return
	}

	oa.writeMetadataResponse(w, &set, http.StatusOK)
}

// handlePutMetadata handles POST /obs/create. It requires a JSON object with
// observation set metadata in the request. It echoes back the metadata as a
// JSON object in the response,
func (oa *ObsAPI) handlePutMetadata(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !oa.azr.IsAuthorized(w, r, "write_obs") {
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
	err = oa.db.RunInTransaction(func(t *pg.Tx) error {
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

	oa.writeMetadataResponse(w, &set, http.StatusCreated)
}

// handleDownload handles GET /obs/<set>/data. It requires  Set IDs in the input are ignored. It writes a response
// containing the all the observations in the set as a newline-delimited
// JSON stream (of content-type application/vnd.mami.ndjson) in observation set
// file format.

func (oa *ObsAPI) handleDownload(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !oa.azr.IsAuthorized(w, r, "write_obs") {
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
	set := pto3.ObservationSet{ID: int(setid)}
	if err = set.SelectByID(oa.db); err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			pto3.HandleErrorHTTP(w, "retrieving set", err)
		}
		return
	}

	// fail if no observations exist
	if set.CountObservations(oa.db) == 0 {
		http.Error(w, fmt.Sprintf("Observation set %s has no observations", vars["set"]), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-type", "application/vnd.mami.ndjson")
	w.WriteHeader(http.StatusOK)
	if err := set.CopyDataToStream(oa.db, w); err != nil {
		pto3.HandleErrorHTTP(w, "downloading observation set", err)
		w.Write([]byte("\n\"error during download\"\n"))
	}
}

// handleUpload handles PUT /obs/<set>/data. It requires a newline-delimited
// JSON stream (of content-type application/vnd.mami.ndjson) in observation set
// file format. Set IDs in the input are ignored. It writes a response
// containing the set's metadata.
func (oa *ObsAPI) handleUpload(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !oa.azr.IsAuthorized(w, r, "write_obs") {
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
	if err := set.SelectByID(oa.db); err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			LogInternalServerError(w, "retrieving set metadata", err)
		}
		return
	}

	// fail if observations exist
	if set.CountObservations(oa.db) != 0 {
		http.Error(w, fmt.Sprintf("Observation set %s already uploaded", vars["set"]), http.StatusBadRequest)
		return
	}

	// create a temporary file to hold observations
	tf, err := ioutil.TempFile("", "pto3_obs")
	if err != nil {
		pto3.HandleErrorHTTP(w, "creating temporary observation file", err)
		return
	}
	defer tf.Close()
	defer os.Remove(tf.Name())

	// copy observation data to the tempfile
	if err := pto3.StreamCopy(r.Body, tf); err != nil {
		pto3.HandleErrorHTTP(w, "uploading to temporary observation file", err)
		return
	}
	tf.Sync()

	// create condition and path caches
	cidCache, err := pto3.LoadConditionCache(oa.db)
	if err != nil {
		pto3.HandleErrorHTTP(w, "loading condition cache", err)
		return
	}
	pidCache := make(pto3.PathCache)

	// now insert the tempfile into the database
	if err := pto3.CopyDataFromObsFile(tf.Name(), oa.db, set, cidCache, pidCache); err != nil {
		pto3.HandleErrorHTTP(w, "inserting observations", err)
		return
	}

	// now update observation count
	set.CountObservations(oa.db)

	// and write
	oa.writeMetadataResponse(w, &set, http.StatusCreated)
}

func (oa *ObsAPI) CreateTables() error {
	return pto3.CreateTables(oa.db)
}

func (oa *ObsAPI) DropTables() error {
	return pto3.DropTables(oa.db)
}

func (oa *ObsAPI) EnableQueryLogging() {
	oa.db.OnQueryProcessed(func(event *pg.QueryProcessedEvent) {
		query, err := event.FormattedQuery()
		if err != nil {
			panic(err)
		}

		log.Printf("%s %s", time.Since(event.StartTime), query)
	})
}

func (oa *ObsAPI) addRoutes(r *mux.Router, l *log.Logger) {
	r.handleFunc("/obs", LogAccess(l, oa.handleListSets)).Methods("GET")
	r.handleFunc("/obs/create", LogAccess(l, oa.handleCreateSet)).Methods("POST")
	r.handleFunc("/obs/{set}", LogAccess(l, oa.handleGetMetadata)).Methods("GET")
	r.handleFunc("/obs/{set}", LogAccess(l, oa.handlePutMetadata)).Methods("PUT")
	r.handleFunc("/obs/{set}/data", LogAccess(l, oa.handleDownload)).Methods("GET")
	r.handleFunc("/obs/{set}/data", LogAccess(l, oa.handleUpload)).Methods("PUT")
}

func NewObsAPI(config *pto3.PTOConfiguration, azr Authorizer, r *mux.Router, l *log.Logger) *ObsAPI {
	if config.ObsDatabase.Database == "" {
		return nil, nil
	}

	oa := new(ObsAPI)
	oa.config = config
	oa.azr = azr
	oa.db = pg.Connect(&config.ObsDatabase)

	oa.addRoutes(r, l)

	return oa
}
