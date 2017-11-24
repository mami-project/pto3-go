package pto3

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// CampaignMetadataFilename is the name of each campaign metadata file in each campaign directory
const CampaignMetadataFilename = "__pto_campaign_metadata.json"

// FileMetadataSuffix is the suffix on each metadata file on disk
const FileMetadataSuffix = ".pto_file_metadata.json"

// DeletionTagSuffix is the suffix on a deletion tag on disk
const DeletionTagSuffix = ".pto_file_delete_me"

// DataRelativeURL is the path relative to each file metadata path for content access
var DataRelativeURL *url.URL

func init() {
	DataRelativeURL, _ = url.Parse("data")
}

type RDSMetadata struct {
	Parent    *RDSMetadata
	filetype  string
	Owner     string
	TimeStart *time.Time
	TimeEnd   *time.Time
	Metadata  map[string]string
	datalink  string
	datasize  int
}

func (md *RDSMetadata) DumpJSONObject(inherit bool) ([]byte, error) {

	jmap := make(map[string]interface{})

	// first inherit from parent
	if inherit && md.Parent != nil {
		if md.Parent.filetype != "" {
			jmap["_file_type"] = md.Parent.filetype
		}

		if md.Parent.Owner != "" {
			jmap["_owner"] = md.Parent.Owner
		}

		if md.Parent.TimeStart != nil {
			jmap["_time_start"] = md.Parent.TimeStart.Format(time.RFC3339)
		}

		if md.Parent.TimeEnd != nil {
			jmap["_time_start"] = md.Parent.TimeEnd.Format(time.RFC3339)
		}

		for k, v := range md.Parent.Metadata {
			jmap[k] = v
		}
	}

	// then overwrite with own values
	if md.filetype != "" {
		jmap["_file_type"] = md.filetype
	}

	if md.Owner != "" {
		jmap["_owner"] = md.Owner
	}

	if md.TimeStart != nil {
		jmap["_time_start"] = md.TimeStart.Format(time.RFC3339)
	}

	if md.TimeEnd != nil {
		jmap["_time_end"] = md.TimeEnd.Format(time.RFC3339)
	}

	// data link and data size are not inheritable
	if md.datalink != "" {
		jmap["__data"] = md.datalink
	}

	if md.datasize != 0 {
		jmap["__data_size"] = md.datasize
	}

	for k, v := range md.Metadata {
		jmap[k] = v
	}

	return json.Marshal(jmap)
}

func (md *RDSMetadata) MarshalJSON() ([]byte, error) {
	// by default, serialize object with all inherited information
	return md.DumpJSONObject(true)
}

func (md *RDSMetadata) UnmarshalJSON(b []byte) error {
	md.Metadata = make(map[string]string)

	var jmap map[string]interface{}

	if err := json.Unmarshal(b, &jmap); err != nil {
		return err
	}

	var err error
	for k, v := range jmap {
		if k == "_file_type" {
			md.filetype = AsString(v)
		} else if k == "_owner" {
			md.Owner = AsString(v)
		} else if k == "_time_start" {
			var t time.Time
			if t, err = AsTime(v); err != nil {
				return err
			}
			md.TimeStart = &t
		} else if k == "_time_end" {
			var t time.Time
			if t, err = AsTime(v); err != nil {
				return err
			}
			md.TimeEnd = &t
		} else if strings.HasPrefix(k, "__") {
			// Ignore all (incoming) __ keys instead of stuffing them in metadata
		} else {
			md.Metadata[k] = AsString(v)
		}
	}

	return nil
}

// WriteToFile writes this metadata object as JSON to a file, ignoring virual metadata keys
func (md *RDSMetadata) WriteToFile(pathname string) error {
	b, err := md.DumpJSONObject(false)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(pathname, b, 0644)
}

// Validate returns nil if the metadata is valid (i.e., it or its parent has all required keys), or an error if not
func (md *RDSMetadata) Validate(isCampaign bool) error {
	if md.Owner == "" && (md.Parent == nil || md.Parent.Owner == "") {
		return RDSMissingMetadataError("missing _owner")
	}

	// short circuit file-only checks
	if isCampaign {
		return nil
	}

	if md.filetype == "" && (md.Parent == nil || md.Parent.filetype == "") {
		return RDSMissingMetadataError("missing _file_type")
	}

	if md.TimeStart == nil && (md.Parent == nil || md.Parent.TimeStart == nil) {
		return RDSMissingMetadataError("missing _time_start")
	}

	if md.TimeEnd == nil && (md.Parent == nil || md.Parent.TimeEnd == nil) {
		return RDSMissingMetadataError("missing _time_end")
	}

	return nil
}

func (md *RDSMetadata) Filetype() string {
	if md.filetype == "" && md.Parent != nil {
		return md.Parent.filetype
	} else {
		return md.filetype
	}
}

func RDSMetadataFromReader(r io.Reader, parent *RDSMetadata) (*RDSMetadata, error) {
	var md RDSMetadata

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(b, &md); err != nil {
		return nil, err
	}

	// link to campaign metadata for inheritance
	md.Parent = parent
	return &md, nil
}

func RDSMetadataFromFile(pathname string, parent *RDSMetadata) (*RDSMetadata, error) {
	f, err := os.Open(pathname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return RDSMetadataFromReader(f, parent)
}

// RDSFiletype encapsulates a filetype in the raw data store FIXME not quite the right type
type RDSFiletype struct {
	// PTO filetype name
	Filetype string `json:"file_type"`
	// Associated MIME type
	ContentType string `json:"mime_type"`
}

// RDSError encapsulates a raw data store error, containing a subject
// identifying what is broken and an HTTP status code identifying how.
type RDSError struct {
	// Subject (filename, campaign, key, etc.) of the error
	subject string
	// HTTP status code
	Status int
}

func (e *RDSError) Error() string {
	switch e.Status {
	case http.StatusBadRequest:
		return fmt.Sprintf("metadata key %s", e.subject)
	case http.StatusForbidden:
		return fmt.Sprintf("operation forbidden on %s", e.subject)
	case http.StatusNotFound:
		return fmt.Sprintf("%s not found", e.subject)
	case http.StatusUnsupportedMediaType:
		return fmt.Sprintf("wrong Content-Type: %s required", e.subject)
	}

	return fmt.Sprintf("unknown error %d: %s is not ok", e.Status, e.subject)
}

// RDSNotFoundError returns a 404 error for a missing campaign and/or file
func RDSNotFoundError(name string) error {
	return &RDSError{name, http.StatusNotFound}
}

// RDSMissingMetadataError returns a 400 error for a missing metadata key in upload
func RDSMissingMetadataError(key string) error {
	return &RDSError{key, http.StatusBadRequest}
}

// RDSCampaign encapsulates a single campaign in a raw data store,
// and caches metadata for the campaign and files within it.
type RDSCampaign struct {
	// application configuration
	config *PTOServerConfig

	// path to campaign directory
	path string

	// requires metadata reload
	stale bool

	// campaign metadata cache
	campaignMetadata *RDSMetadata

	// file metadata cache; keys of this define known filenames
	fileMetadata map[string]*RDSMetadata

	// lock on metadata structures
	lock sync.RWMutex
}

// NewRDSCampaign creates a new campaign object bound the path of a directory on
// disk containing the campaign's files.
func NewRDSCampaign(config *PTOServerConfig, path string) *RDSCampaign {
	cam := RDSCampaign{
		config:       config,
		path:         path,
		stale:        true,
		fileMetadata: make(map[string]*RDSMetadata),
	}

	return &cam
}

// reloadMetadata reloads the metadata for this campaign and its files from disk
func (cam *RDSCampaign) reloadMetadata(force bool) error {
	var err error

	cam.lock.Lock()
	defer cam.lock.Unlock()

	// skip if not stale
	if !force && !cam.stale {
		return nil
	}

	// load the campaign metadata file
	cam.campaignMetadata, err = RDSMetadataFromFile(filepath.Join(cam.path, CampaignMetadataFilename), nil)
	if err != nil {
		return err
	}

	// now scan directory and load each metadata file
	direntries, err := ioutil.ReadDir(cam.path)
	for _, direntry := range direntries {
		metafilename := direntry.Name()
		if strings.HasSuffix(metafilename, FileMetadataSuffix) {
			linkname := metafilename[0 : len(metafilename)-len(FileMetadataSuffix)]
			cam.fileMetadata[linkname], err =
				RDSMetadataFromFile(filepath.Join(cam.path, metafilename), cam.campaignMetadata)
			if err != nil {
				return err
			}
		}
	}

	// everything loaded, mark not stale and return no error
	cam.stale = false
	return nil
}

// unloadMetadata allows a campaign's metadata to be garbage-collected, requiring reload on access.
func (cam *RDSCampaign) unloadMetadata() {
	cam.lock.Lock()
	defer cam.lock.Unlock()

	cam.campaignMetadata = nil
	cam.fileMetadata = nil
	cam.stale = true
}

func (cam *RDSCampaign) getCampaignMetadata() (*RDSMetadata, error) {
	// reload if stale
	err := cam.reloadMetadata(false)
	if err != nil {
		return nil, err
	}

	return cam.campaignMetadata, nil
}

func (cam *RDSCampaign) creat(md *RDSMetadata) error {
	if err := os.Mkdir(cam.path, 0755); err != nil {
		return err
	}

	// make sure campaign metadata is ok
	if err := md.Validate(true); err != nil {
		return err
	}

	// write it directly to the file
	if err := md.WriteToFile(filepath.Join(cam.path, CampaignMetadataFilename)); err != nil {
		return err
	}

	// and force a rescan
	if err := cam.reloadMetadata(true); err != nil {
		return err
	}

	return nil
}

func (cam *RDSCampaign) putCampaignMetadata(md *RDSMetadata) error {
	cam.lock.Lock()
	defer cam.lock.Unlock()

	// make sure campaign metadata is ok
	if err := md.Validate(true); err != nil {
		return err
	}

	// write to campaign metadata file
	if err := md.WriteToFile(filepath.Join(cam.path, CampaignMetadataFilename)); err != nil {
		return err
	}

	// update metadata cache
	cam.campaignMetadata = md
	return nil
}

func (cam *RDSCampaign) getFileMetadata(filename string) (*RDSMetadata, error) {
	// reload if stale
	err := cam.reloadMetadata(false)
	if err != nil {
		return nil, err
	}

	// check for file metadata
	filemd, ok := cam.fileMetadata[filename]
	if !ok {
		return nil, RDSNotFoundError(filename)
	}

	return filemd, nil
}

// updateFileVirtualMetadata fills in the __data and __data_size virtual metadata
// for a file. Not concurrency safe: caller must hold the campaign write lock.
func (cam *RDSCampaign) updateFileVirtualMetadata(filename string) error {

	// get file metadata
	md, ok := cam.fileMetadata[filename]
	if !ok {
		return RDSNotFoundError(filename)
	}

	// get file size
	datafi, err := os.Stat(filepath.Join(cam.path, filename))
	if err == nil {
		md.datasize = int(datafi.Size())
	} else if os.IsNotExist(err) {
		md.datasize = 0
	} else {
		return err
	}

	// generate data path
	datarel, err := url.Parse("/raw/" + filepath.Base(cam.path) + "/" + filename + "/data")
	if err != nil {
		return err
	}
	md.datalink = cam.config.baseURL.ResolveReference(datarel).String()

	return nil
}

func (cam *RDSCampaign) putFileMetadata(filename string, md *RDSMetadata) error {
	cam.lock.Lock()
	defer cam.lock.Unlock()

	// inherit from campaign
	md.Parent = cam.campaignMetadata

	// ensure we have a filetype
	if md.Filetype() == "" {
		return RDSMissingMetadataError("_file_type")
	}

	// write to file metadata file
	err := md.WriteToFile(filepath.Join(cam.path, filename+FileMetadataSuffix))
	if err != nil {
		return err
	}

	// update metadata cache
	cam.fileMetadata[filename] = md

	// and update virtuals
	return cam.updateFileVirtualMetadata(filename)
}

func (cam *RDSCampaign) getFiletype(filename string) *RDSFiletype {
	// reload if stale
	err := cam.reloadMetadata(false)
	if err != nil {
		return nil
	}

	md, ok := cam.fileMetadata[filename]
	if !ok {
		return nil
	}

	ftname := md.Filetype()
	ctype, ok := cam.config.ContentTypes[ftname]
	if !ok {
		return nil
	}

	return &RDSFiletype{ftname, ctype}
}

// A RawDataStore encapsulates a pile of PTO data and metadata files as a set of
// campaigns.
type RawDataStore struct {
	// application configuration
	config *PTOServerConfig

	// authorizer
	azr *Authorizer

	// base path
	path string

	// lock on campaign cache
	lock sync.RWMutex

	// campaign cache
	campaigns map[string]*RDSCampaign
}

func rdsHTTPError(w http.ResponseWriter, err error) {
	switch ev := err.(type) {
	case *RDSError:
		http.Error(w, ev.Error(), ev.Status)
	default:
		LogInternalServerError(w, "", err)
	}
}

// scanCampaigns updates the RawDataStore to reflect the current state of the
// files on disk.
func (rds *RawDataStore) scanCampaigns() error {
	rds.lock.Lock()
	defer rds.lock.Unlock()

	rds.campaigns = make(map[string]*RDSCampaign)

	direntries, err := ioutil.ReadDir(rds.path)

	if err != nil {
		return err
	}

	for _, direntry := range direntries {
		if direntry.IsDir() {

			// look for a metadata file
			mdpath := filepath.Join(rds.path, direntry.Name(), CampaignMetadataFilename)
			_, err := os.Stat(mdpath)
			if err != nil {
				if os.IsNotExist(err) {
					log.Printf("Missing campaign metadata file %s", mdpath)
					continue // no metadata file means we don't care about this directory
				} else {
					return err // something else broke. die.
				}
			}

			// create a new (stale) campaign
			rds.campaigns[direntry.Name()] = NewRDSCampaign(rds.config,
				filepath.Join(rds.path, direntry.Name()))
		}
	}

	return nil
}

// FIXME rethink how to bootstrap new campaigns...
func (rds *RawDataStore) createCampaign(camname string, md *RDSMetadata) (*RDSCampaign, error) {

	campath := filepath.Join(rds.path, camname)
	cam := NewRDSCampaign(rds.config, campath)
	err := cam.creat(md)
	if err != nil {
		return nil, err
	}

	err = cam.putCampaignMetadata(md)
	if err != nil {
		return nil, err
	}

	rds.lock.Lock()
	rds.campaigns[camname] = cam
	rds.lock.Unlock()

	return cam, nil
}

type campaignList struct {
	Campaigns []string `json:"campaigns"`
}

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

	out, err := cam.getFileMetadata(filename)
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
	out, err := cam.getFileMetadata(filename)
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

	// build a local filesystem path for downloading and validate it
	rawpath := filepath.Clean(filepath.Join(rds.path, camname, filename))
	if pathok, _ := filepath.Match(filepath.Join(rds.path, "*", "*"), rawpath); !pathok {
		http.Error(w, fmt.Sprintf("path %s is not ok", rawpath), http.StatusBadRequest)
		return
	}

	// write MIME type to header
	w.Header().Set("Content-Type", ft.ContentType)

	// now stream the file to the writer
	rawfile, err := os.Open(rawpath)
	if err != nil {
		LogInternalServerError(w, "opening data file", err)
		return
	}
	defer rawfile.Close()

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
	out, err := cam.getFileMetadata(filename)
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

// Ensure that the directories backing the data store exist. Used for testing.
func (rds *RawDataStore) CreateDirectories() error {
	return os.Mkdir(rds.path, 0755)
}

// Remove the directories backing the data store incluing all their contents.
// Used for testing.
func (rds *RawDataStore) RemoveDirectories() error {
	return os.RemoveAll(rds.path)
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

// NewRawDataStore encapsulates a raw data store, given a pathname of a
// directory containing its files.
func NewRawDataStore(config *PTOServerConfig, azr *Authorizer) (*RawDataStore, error) {
	rds := RawDataStore{config: config, azr: azr, path: config.RawRoot}

	// scan the directory for campaigns
	rds.scanCampaigns()

	return &rds, nil
}
