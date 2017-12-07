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

// HandleListCampaigns handles GET /raw, returning a list of campaigns in the
// raw data store. It writes a JSON object to the response with a single key,
// "campaigns", whose content is an array of campaign URL as strings.
func (rds *RawDataStore) HandleListCampaigns(w http.ResponseWriter, r *http.Request) {

	// fail if not authorized
	if !rds.azr.IsAuthorized(w, r, "list_raw") {
		return
	}

	// force a campaign rescan
	err := rds.scanCampaigns()
	if err != nil {
		LogInternalServerError(w, "scanning campaigns", err)
		return
	}

	// construct URLs based on the campaign
	out := campaignList{make([]string, len(rds.campaigns))}

	i := 0
	for k := range rds.campaigns {
		camurl, err := url.Parse(fmt.Sprintf("raw/%s", k))
		if err != nil {
			LogInternalServerError(w, "generating campaign link", err)
			return
		}
		out.Campaigns[i] = rds.config.baseURL.ResolveReference(camurl).String()
		i++
	}

	// FIXME pagination goes here

	outb, err := json.Marshal(out)
	if err != nil {
		LogInternalServerError(w, "marshaling campaign list", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(outb)
}

type campaignFileList struct {
	Metadata *RDSMetadata `json:"metadata"`
	Files    []string     `json:"files"`
}

// HandleGetCampaignMetadata handles GET /raw/<campaign>, returning metadata for
// a campaign. It writes a JSON object to the response containing campaign
// metadata.
func (rds *RawDataStore) HandleGetCampaignMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// get campaign name
	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", http.StatusBadRequest)
		return
	}

	// fail if not authorized
	if !rds.azr.IsAuthorized(w, r, "read_raw:"+camname) {
		return
	}

	// look up campaign
	cam, ok := rds.campaigns[camname]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), http.StatusNotFound)
		return
	}

	var out campaignFileList
	var err error
	out.Metadata, err = cam.getCampaignMetadata()
	if err != nil {
		rdsHTTPError(w, err)
		return
	}

	out.Files = make([]string, len(cam.fileMetadata))
	i := 0
	for filename := range cam.fileMetadata {
		filepath, err := url.Parse("/raw/" + filepath.Base(cam.path) + "/" + filename)
		if err != nil {
			log.Print(err)
			LogInternalServerError(w, "generating file link", err)
		}
		out.Files[i] = rds.config.baseURL.ResolveReference(filepath).String()
		i++
	}

	outb, err := json.Marshal(out)
	if err != nil {
		LogInternalServerError(w, "marshaling campaign metadata", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(outb)
}

// HandlePutCampaignMetadata handles PUT /raw/<campaign>, overwriting metadata for
// a campaign, creating it if necessary. It requires a JSON object in the
// request body containing campaign metadata. It echoes the written metadata
// back in the response.
func (rds *RawDataStore) HandlePutCampaignMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// get campaign name
	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", http.StatusBadRequest)
		return
	}

	// fail if not authorized
	if !rds.azr.IsAuthorized(w, r, "write_raw:"+camname) {
		return
	}

	// fail if not JSON
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, fmt.Sprintf("Content-type for metadata must be application/json; got %s instead",
			r.Header.Get("Content-Type")), http.StatusUnsupportedMediaType)
		return
	}

	// read metadata from request
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// unmarshal it
	var in RDSMetadata
	err = json.Unmarshal(b, &in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// now look up the campaign
	cam, ok := rds.campaigns[camname]
	if !ok {
		// Campaign doesn't exist. We have to create it.
		cam, err = rds.createCampaign(camname, &in)
		if err != nil {
			LogInternalServerError(w, fmt.Sprintf("creating campaign %s", camname), err)
			return
		}
	}

	// overwrite metadata
	err = cam.putCampaignMetadata(&in)
	if err != nil {
		rdsHTTPError(w, err)
		return
	}

	// now reflect it back
	out, err := cam.getCampaignMetadata()
	if err != nil {
		rdsHTTPError(w, err)
		return
	}

	outb, err := json.Marshal(out)
	if err != nil {
		LogInternalServerError(w, "marshalling campaign metadata", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(outb)
}

// HandleGetFileMetadata handles GET /raw/<campaign>/<file>, returning
// metadata for a file, including virtual metadata (file size and data URL) and
// any metadata inherited from the campaign. It writes a JSON object to the
// response containing file metadata.
func (rds *RawDataStore) HandleGetFileMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", http.StatusBadRequest)
		return
	}

	filename, ok := vars["file"]
	if !ok {
		http.Error(w, "missing file", http.StatusBadRequest)
		return
	}

	// fail if not authorized
	if !rds.azr.IsAuthorized(w, r, "read_raw:"+camname) {
		return
	}

	cam, ok := rds.campaigns[vars["campaign"]]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), http.StatusNotFound)
		return
	}

	out, err := cam.GetFileMetadata(filename)
	if err != nil {
		rdsHTTPError(w, err)
		return
	}

	outb, err := json.Marshal(out)
	if err != nil {
		LogInternalServerError(w, "marshalling file metadata", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(outb)
}

// HandlePutFileMetadata handles PUT /raw/<campaign>/<file>, overwriting metadata for
// a file, creating it if necessary. It requires a JSON object in the
// request body containing file metadata. It echoes the full file metadata
// back in the response, including inherited campaign metadata and any virtual metadata.
func (rds *RawDataStore) HandlePutFileMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", http.StatusBadRequest)
		return
	}

	filename, ok := vars["file"]
	if !ok {
		http.Error(w, "missing file", http.StatusBadRequest)
		return
	}

	// fail if not authorized
	if !rds.azr.IsAuthorized(w, r, "write_raw:"+camname) {
		return
	}

	// fail if not JSON
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, fmt.Sprintf("Content-type for metadata must be application/json; got %s instead",
			r.Header.Get("Content-Type")), http.StatusUnsupportedMediaType)
		return
	}

	// read metadata from request
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// unmarshal it
	var in RDSMetadata
	err = json.Unmarshal(b, &in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// now look up the campaign
	cam, ok := rds.campaigns[camname]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), http.StatusNotFound)
		return
	}

	// overwrite metadata for file
	err = cam.putFileMetadata(filename, &in)
	if err != nil {
		rdsHTTPError(w, err)
		return
	}

	// now reflect it back
	out, err := cam.GetFileMetadata(filename)
	if err != nil {
		rdsHTTPError(w, err)
		return
	}

	outb, err := json.Marshal(out)
	if err != nil {
		LogInternalServerError(w, "marshalling file metadata", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(outb) // FIXME log error here
}

// HandleDeleteFile handles DELETE /raw/<campaign>/<file>, deleting a file's
// metadata and content by marking it pending deletion in the raw data store.
// Deletion is not yet fully specified or implemented, so this just returns a
// StatusNotImplemented response for now.
func (rds *RawDataStore) HandleDeleteFile(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "delete not implemented, come back later", http.StatusNotImplemented)
}

// HandleFileDownload handles GET /raw/<campaign>/<file>/data, returning a file's
// content. It writes a response of the appropriate MIME type for the file (as
// determined by the filetypes map and the _file_type metadata key).
func (rds *RawDataStore) HandleFileDownload(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)

	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", http.StatusBadRequest)
		return
	}

	filename, ok := vars["file"]
	if !ok {
		http.Error(w, "missing file", http.StatusBadRequest)
		return
	}

	// fail if not authorized
	if !rds.azr.IsAuthorized(w, r, "read_raw:"+camname) {
		return
	}

	// now look up the campaign
	cam, ok := rds.campaigns[camname]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", camname), http.StatusNotFound)
		return
	}

	// determine MIME type
	ft := cam.getFiletype(filename)
	if ft == nil {
		LogInternalServerError(w, fmt.Sprintf("determining filetype for %s", filename), nil)
	}

	// try to open raw data file
	rawfile, err := cam.ReadFileData(filename)
	if err != nil {
		LogInternalServerError(w, "opening data file", err)
		return
	}
	defer rawfile.Close()

	// write MIME type to header
	w.Header().Set("Content-Type", ft.ContentType)
	w.WriteHeader(http.StatusOK)

	buf := make([]byte, 65536)
	for {
		n, err := rawfile.Read(buf)
		if err == nil {
			w.Write(buf[0:n]) // FIXME log error here
		} else if err == io.EOF {
			break
		} else {
			LogInternalServerError(w, "reading data file", err)
			return
		}
	}
}

// HandleFileUpload handles PUT /raw/<campaign>/<file>/data. It requires a request of the appropriate MIME type for the file (as
// determined by the filetypes map and the _file_type metadata key) whose body is the file's content. It writes a response containing the file's metadata.
func (rds *RawDataStore) HandleFileUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", http.StatusBadRequest)
		return
	}

	filename, ok := vars["file"]
	if !ok {
		http.Error(w, "missing file", http.StatusBadRequest)
		return
	}

	// fail if not authorized
	if !rds.azr.IsAuthorized(w, r, "write_raw:"+camname) {
		return
	}

	// now look up the campaign
	cam, ok := rds.campaigns[camname]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), http.StatusNotFound)
		return
	}

	// build a local filesystem path for uploading and validate it
	rawpath := filepath.Clean(filepath.Join(rds.path, camname, filename))
	if pathok, _ := filepath.Match(filepath.Join(rds.path, "*", "*"), rawpath); !pathok {
		http.Error(w, fmt.Sprintf("path %s is not ok", rawpath), http.StatusBadRequest)
		return
	}

	// fail if file exists
	_, err := os.Stat(rawpath)
	if (err == nil) || !os.IsNotExist(err) {
		http.Error(w, fmt.Sprintf("file %s/%s already exists", camname, filename), http.StatusBadRequest)
	}

	// determine and verify MIME type
	ft := cam.getFiletype(filename)
	if ft == nil {
		LogInternalServerError(w, fmt.Sprintf("getting filetype for %s", filename), nil)
		return
	}
	if ft.ContentType != r.Header.Get("Content-Type") {
		http.Error(w, fmt.Sprintf("Content-Type for %s/%s must be %s", camname, filename, ft.ContentType), http.StatusBadRequest)
	}

	// write MIME type to header
	w.Header().Set("Content-Type", ft.ContentType)

	// now stream the file from the reader on to disk
	rawfile, err := os.Create(rawpath)
	if err != nil {
		LogInternalServerError(w, "creating data file", err)
		return
	}
	defer rawfile.Close()

	reqreader, err := r.GetBody()
	if err != nil {
		LogInternalServerError(w, "reading upload data", err)
		return
	}

	buf := make([]byte, 65536)
	for {
		n, err := reqreader.Read(buf)
		if err == nil {
			_, err = rawfile.Write(buf[0:n])
			if err != nil {
				LogInternalServerError(w, "writing upload data", err)
				return
			}
		} else if err == io.EOF {
			break
		} else {
			LogInternalServerError(w, "reading upload data", err)
			return
		}
	}

	// update file metadata to reflect size
	rawfile.Sync()
	err = cam.updateFileVirtualMetadata(filename)
	if err != nil {
		LogInternalServerError(w, "updating virtual metadata", err)
		return
	}

	// and now a reply... return file metadata
	out, err := cam.GetFileMetadata(filename)
	if err != nil {
		rdsHTTPError(w, err)
		return
	}

	outb, err := json.Marshal(out)
	if err != nil {
		LogInternalServerError(w, "marshalling file metadata", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(outb)
}

func (rds *RawDataStore) AddRoutes(r *mux.Router) {
	l := rds.config.accessLogger
	r.HandleFunc("/raw", LogAccess(l, rds.HandleListCampaigns)).Methods("GET")
	r.HandleFunc("/raw/{campaign}", LogAccess(l, rds.HandleGetCampaignMetadata)).Methods("GET")
	r.HandleFunc("/raw/{campaign}", LogAccess(l, rds.HandlePutCampaignMetadata)).Methods("PUT")
	r.HandleFunc("/raw/{campaign}/{file}", LogAccess(l, rds.HandleGetFileMetadata)).Methods("GET")
	r.HandleFunc("/raw/{campaign}/{file}", LogAccess(l, rds.HandlePutFileMetadata)).Methods("PUT")
	r.HandleFunc("/raw/{campaign}/{file}", LogAccess(l, rds.HandleDeleteFile)).Methods("DELETE")
	r.HandleFunc("/raw/{campaign}/{file}/data", LogAccess(l, rds.HandleFileDownload)).Methods("GET")
	r.HandleFunc("/raw/{campaign}/{file}/data", LogAccess(l, rds.HandleFileUpload)).Methods("PUT")
}

// FIXME observation store, here, is a
type ObservationStore struct {
	config *PTOServerConfig
	azr    Authorizer
	db     *pg.DB
}

func NewObservationStore(config *PTOServerConfig, azr Authorizer) (*ObservationStore, error) {
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
