package papi

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

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
	set.LinkVia(oa.config)

	// now write it to the response
	b, err := json.Marshal(&set)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshaling metadata", err)
		return
	}
	w.WriteHeader(status)
	w.Write(b)
}

type setList struct {
	Sets       []string `json:"sets"`
	Next       string   `json:"next"`
	Prev       string   `json:"prev"`
	TotalCount int      `json:"total_count"`
}

func (sl *setList) MarshalJSON() ([]byte, error) {
	out := make(map[string]interface{})

	out["sets"] = sl.Sets

	if sl.Next != "" {
		out["next"] = sl.Next
	}

	if sl.Prev != "" {
		out["prev"] = sl.Prev
	}

	return json.Marshal(out)
}

func (oa *ObsAPI) writeSetListResponse(w http.ResponseWriter, setIds []int, pageVal string) {
	// slice the array based on page
	page64, _ := strconv.ParseInt(pageVal, 10, 64)
	page := int(page64)
	offset := page * oa.config.PageLength

	var out setList

	// paginate if we need to
	if page > 0 || len(setIds) > (page+1)*oa.config.PageLength {

		if len(setIds) > (page+1)*oa.config.PageLength {
			out.Next, _ = oa.config.LinkTo(fmt.Sprintf("/obs?page=%d", page+1))
			out.TotalCount = len(setIds)
		}

		if page > 0 {
			out.Prev, _ = oa.config.LinkTo(fmt.Sprintf("/obs?page=%d", page-1))
			out.TotalCount = len(setIds)
		}

		endOffset := offset + oa.config.PageLength
		if endOffset > len(setIds) {
			endOffset = len(setIds)
		}

		setIds = setIds[offset:endOffset]
	}

	// linkify set IDs
	out.Sets = make([]string, len(setIds))
	for i, id := range setIds {
		out.Sets[i] = pto3.LinkForSetID(oa.config, id)
	}

	outb, err := json.Marshal(&out)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshaling set list", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(outb)
}

// handleListSets handles GET /obs.
// It returns a JSON object with links to current observation sets in the sets key.
func (oa *ObsAPI) handleListSets(w http.ResponseWriter, r *http.Request) {

	// fail if not authorized
	if !oa.azr.IsAuthorized(w, r, "read_obs") {
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("error parsing form: %s", err.Error()), http.StatusBadRequest)
	}

	// select set IDs into an array
	setIds, err := pto3.AllObservationSetIDs(oa.db)
	if err != nil {
		pto3.HandleErrorHTTP(w, "listing set IDs", err)
		return
	}

	oa.writeSetListResponse(w, setIds, r.Form.Get("page"))
}

func intersectSetIds(a []int, b []int, hasSets bool) []int {
	if hasSets {
		out := make([]int, 0)
		amap := make(map[int]struct{})
		for _, id := range a {
			amap[id] = struct{}{}
		}
		for _, id := range b {
			if _, ok := amap[id]; ok {
				out = append(out, id)
			}
		}
		return out
	} else {
		return b
	}
}

// handleMetadataQuery handles GET/POST /obs/by_metadata. It requires two
// URL/form parameters: 'k', the key to search for, and 'v', the value to
// search for.

func (oa *ObsAPI) handleMetadataQuery(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !oa.azr.IsAuthorized(w, r, "read_obs") {
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("error parsing form: %s", err.Error()), http.StatusBadRequest)
	}

	setIds := make([]int, 0)
	queryActive := false

	source := r.Form.Get("source")
	if source != "" {
		// handle source query
		sourceSetIds, err := pto3.ObservationSetIDsWithSource(oa.db, source)
		if err != nil {
			pto3.HandleErrorHTTP(w, "selecting set IDs by source", err)
			return
		}
		setIds = intersectSetIds(setIds, sourceSetIds, queryActive)
		queryActive = true
	}

	analyzer := r.Form.Get("analyzer")
	if analyzer != "" {
		// handle analyzer query
		analyzerSetIds, err := pto3.ObservationSetIDsWithAnalyzer(oa.db, analyzer)
		if err != nil {
			pto3.HandleErrorHTTP(w, "selecting set IDs by analyzer", err)
			return
		}
		setIds = intersectSetIds(setIds, analyzerSetIds, queryActive)
		queryActive = true
	}

	condition := r.Form.Get("condition")
	if condition != "" {
		// create condition caches
		cidCache, err := pto3.LoadConditionCache(oa.db)
		if err != nil {
			pto3.HandleErrorHTTP(w, "loading condition cache", err)
			return
		}

		// handle condition query
		conditionSetIds, err := pto3.ObservationSetIDsWithCondition(oa.db, cidCache, condition)
		if err != nil {
			pto3.HandleErrorHTTP(w, "selecting set IDs by condition", err)
			return
		}
		setIds = intersectSetIds(setIds, conditionSetIds, queryActive)
		queryActive = true
	}

	k := r.Form.Get("k")
	if k != "" {
		v := r.Form.Get("v")
		if v != "" {
			// handle metadata key equality query
			equalitySetIds, err := pto3.ObservationSetIDsWithMetadataValue(oa.db, k, v)
			if err != nil {
				pto3.HandleErrorHTTP(w, "selecting set IDs by key equality", err)
				return
			}
			setIds = intersectSetIds(setIds, equalitySetIds, queryActive)
			queryActive = true
		} else {
			// handle metadata key presence query
			presenceSetIds, err := pto3.ObservationSetIDsWithMetadata(oa.db, k)
			if err != nil {
				pto3.HandleErrorHTTP(w, "selecting set IDs by key existance", err)
				return
			}
			setIds = intersectSetIds(setIds, presenceSetIds, queryActive)
			queryActive = true
		}
	}

	if queryActive == false {
		http.Error(w, "no query parameters given", http.StatusBadRequest)
	}

	oa.writeSetListResponse(w, setIds, r.Form.Get("page"))
}

// handleConditionQuery handles GET /obs/conditions. It requires two
// URL/form parameters: 'k', the key to search for, and 'v', the value to
// search for.

func (oa *ObsAPI) handleConditionQuery(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !oa.azr.IsAuthorized(w, r, "read_obs") {
		return
	}

	// load condition cache
	condCache, err := pto3.LoadConditionCache(oa.db)
	if err != nil {
		pto3.HandleErrorHTTP(w, "retrieving conditions", err)
		return
	}

	// dump it to JSON
	out := struct {
		C []string `json:"conditions"`
	}{C: condCache.Names()}

	outb, err := json.Marshal(&out)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshaling condition list", err)
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

	var set pto3.ObservationSet
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
		pto3.HandleErrorHTTP(w, "inserting set record", err)
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

	set := pto3.ObservationSet{ID: int(setid)}
	if err = set.SelectByID(oa.db); err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			pto3.HandleErrorHTTP(w, "retrieving set", err)
		}
		return
	}

	// force observation count
	set.CountObservations(oa.db)

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

	var set pto3.ObservationSet
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
			pto3.HandleErrorHTTP(w, "updating set metadata", err)
		}
		return
	}

	oa.writeMetadataResponse(w, &set, http.StatusCreated)
}

// handleDownload handles GET /obs/<set>/data. It requires  Set IDs in the
// input are ignored. It writes a response containing the all the observations
// in the set as a newline-delimited JSON stream (of content-type
// application/vnd.mami.ndjson) in observation set file format.

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
	set := pto3.ObservationSet{ID: int(setid)}
	if err := set.SelectByID(oa.db); err != nil {
		if err == pg.ErrNoRows {
			http.Error(w, fmt.Sprintf("Observation set %s not found", vars["set"]), http.StatusNotFound)
		} else {
			pto3.HandleErrorHTTP(w, "retrieving set metadata", err)
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
	if _, err := io.Copy(tf, r.Body); err != nil {
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
	if err := pto3.CopyDataFromObsFile(tf.Name(), oa.db, &set, cidCache, pidCache); err != nil {
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
	pto3.EnableQueryLogging(oa.db)
}

func (oa *ObsAPI) addRoutes(r *mux.Router, l *log.Logger) {
	r.HandleFunc("/obs", LogAccess(l, oa.handleListSets)).Methods("GET")
	r.HandleFunc("/obs/by_metadata", LogAccess(l, oa.handleMetadataQuery)).Methods("GET", "POST")
	r.HandleFunc("/obs/conditions", LogAccess(l, oa.handleConditionQuery)).Methods("GET")
	r.HandleFunc("/obs/create", LogAccess(l, oa.handleCreateSet)).Methods("POST")
	r.HandleFunc("/obs/{set}", LogAccess(l, oa.handleGetMetadata)).Methods("GET")
	r.HandleFunc("/obs/{set}", LogAccess(l, oa.handlePutMetadata)).Methods("PUT")
	r.HandleFunc("/obs/{set}/data", LogAccess(l, oa.handleDownload)).Methods("GET")
	r.HandleFunc("/obs/{set}/data", LogAccess(l, oa.handleUpload)).Methods("PUT")
}

func NewObsAPI(config *pto3.PTOConfiguration, azr Authorizer, r *mux.Router) *ObsAPI {
	if config.ObsDatabase.Database == "" {
		return nil
	}

	oa := new(ObsAPI)
	oa.config = config
	oa.azr = azr
	oa.db = pg.Connect(&config.ObsDatabase)

	oa.addRoutes(r, config.AccessLogger())

	return oa
}
