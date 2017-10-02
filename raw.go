package gopto

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gorilla/mux"
)

// CampaignMetadataFilename is the name of each campaign metadata file in each campaign directory
const CampaignMetadataFilename = "ptocampaign_metadata.json"

// FileMetadataSuffix is the suffix on each metadata file on disk
const FileMetadataSuffix = ".ptofile_metadata.json"

// DeletionTagSuffix is the suffix on a deletion tag on disk
const DeletionTagSuffix = ".ptofile_delete_me"

var DataRelativeURL *url.URL

func init() {
	var err error
	DataRelativeURL, err = url.Parse("data")
	if err != nil {
		panic("relative URL parse invariant violation")
	}
}

// RDSMetadata represents metadata about a file or a campaign
type RDSMetadata map[string]interface{}

func ReadRDSMetadata(pathname string) (out RDSMetadata, err error) {
	rmd, err := ioutil.ReadFile(pathname)
	if err == nil {
		err = json.Unmarshal(rmd, out)
	}
	return
}

// WriteToFile writes this metadata object as JSON to a file, ignoring virual metadata keys
func (md *RDSMetadata) WriteToFile(pathname string) error {
	return nil
}

// RDSFiletype encapsulates a filetype in the raw data store
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
	case 400:
		return fmt.Sprintf("metadata key %s", e.subject)
	case 403:
		return fmt.Sprintf("operation forbidden on %s", e.subject)
	case 404:
		return fmt.Sprintf("%s not found", e.subject)
	case 415:
		return fmt.Sprintf("wrong Content-Type: %s required", e.subject)
	}

	return fmt.Sprintf("unknown error %d: %s is not ok", e.Status, e.subject)
}

// RDSNotFoundError returns a 404 error for a missing campaign and/or file
func RDSNotFoundError(name string) error {
	return &RDSError{name, 404}
}

// RDSMissingMetadataError returns a 400 error for a missing metadata key in upload
func RDSMissingMetadataError(key string) error {
	return &RDSError{key, 400}
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
	campaignMetadata RDSMetadata

	// file metadata cache; keys of this define known filenames
	fileMetadata map[string]RDSMetadata

	// lock on metadata structures
	lock sync.RWMutex
}

// reloadMetadata reloads the metadata for this campaign and its files from disk
func (cam *RDSCampaign) reloadMetadata(force bool) error {
	var err error

	cam.lock.Lock()
	defer cam.lock.Unlock()

	// skip if not stale
	if !cam.stale {
		return nil
	}

	// load the campaign metadata file
	cam.campaignMetadata, err = ReadRDSMetadata(filepath.Join(cam.path, CampaignMetadataFilename))
	if err != nil {
		return err
	}

	// now scan directory and load each metadata file
	// FIXME check for deletion file as well
	direntries, err := ioutil.ReadDir(cam.path)
	for _, direntry := range direntries {
		if strings.HasSuffix(direntry.Name(), FileMetadataSuffix) {
			cam.fileMetadata[direntry.Name()], err = ReadRDSMetadata(filepath.Join(cam.path, direntry.Name()))
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

	cam.lock.RLock()
	defer cam.lock.RUnlock()

	// copy metadata from cache
	out := make(RDSMetadata, len(cam.campaignMetadata))
	for k, v := range cam.campaignMetadata {
		out[k] = v
	}

	// and we're done
	return &out, nil
}

func (cam *RDSCampaign) putCampaignMetadata(md *RDSMetadata) error {
	cam.lock.Lock()
	defer cam.lock.Unlock()

	// copy metadata from input, ignoring virtuals
	out := make(RDSMetadata, len(*md))
	for k, v := range *md {
		if !strings.HasPrefix(k, "__") {
			out[k] = v
		}
	}

	// write to campaign metadata file
	err := out.WriteToFile(filepath.Join(cam.path, CampaignMetadataFilename))
	if err != nil {
		return err
	}

	// update metadata cache
	cam.campaignMetadata = out
	return nil
}

func (cam *RDSCampaign) getFileMetadata(filename string) (*RDSMetadata, error) {
	// reload if stale
	err := cam.reloadMetadata(false)
	if err != nil {
		return nil, err
	}

	cam.lock.RLock()
	defer cam.lock.RUnlock()

	// check for file metadata
	filemd, ok := cam.fileMetadata[filename]
	if !ok {
		return nil, RDSNotFoundError(filename)
	}

	// copy campaign metadata from cache
	out := make(RDSMetadata, len(cam.fileMetadata))
	for k, v := range cam.campaignMetadata {
		out[k] = v
	}

	// and override it with file metadata
	for k, v := range filemd {
		out[k] = v
	}

	return &out, nil
}

func (cam *RDSCampaign) putFileMetadata(filename string, md *RDSMetadata) error {
	cam.lock.Lock()
	defer cam.lock.Unlock()

	// verify that we have a filetype somewhere
	if _, ok := cam.campaignMetadata["_file_type"]; !ok {
		if _, ok := (*md)["_file_type"]; !ok {
			return RDSMissingMetadataError("_file_type")
		}
	}

	// copy metadata from input, ignoring virtuals
	out := make(RDSMetadata, len(*md))
	for k, v := range *md {
		if !strings.HasPrefix(k, "__") {
			out[k] = v
		}
	}

	// write to file metadata file
	err := out.WriteToFile(filepath.Join(cam.path, filename+FileMetadataSuffix))
	if err != nil {
		return err
	}

	// FIXME refactor this into its own function, upload needs to call it too
	// fill in __data and __data_size virtuals
	var datasize int64
	datafi, err := os.Stat(filepath.Join(cam.path, filename))
	if err == nil {
		datasize = datafi.Size()
	} else if os.IsNotExist(err) {
		datasize = 0
	} else {
		return err
	}

	out["__data_size"] = datasize
	out["__data"] = cam.config.BaseURL.ResolveReference(DataRelativeURL).String()

	// and update metadata cache
	cam.fileMetadata[filename] = out
	return nil
}

func (cam *RDSCampaign) getFiletype(filename string) (*RDSFiletype, error) {
	// FIXME
	return nil, nil
}

// NewRDSCampaign creates a new campaign object bound the path of a directory on
// disk containing the campaign's files.
func NewRDSCampaign(config *PTOServerConfig, path string) *RDSCampaign {
	cam := RDSCampaign{config: config, path: path, stale: true}

	return &cam
}

// A RawDataStore encapsulates a pile of PTO data and metadata files as a set of
// campaigns.
type RawDataStore struct {
	// application configuration
	config *PTOServerConfig

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
		http.Error(w, fmt.Sprintf("internal server error: %s", err.Error()), 500)
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

type campaignList struct {
	Campaigns []string `json:"campaigns"`
}

func (rds *RawDataStore) HandleListCampaigns(w http.ResponseWriter, r *http.Request) {

	// fail if not authorized
	if !IsAuthorized(w, r, "list_raw") {
		return
	}

	// construct URLs based on the campaign
	out := campaignList{make([]string, len(rds.campaigns))}

	for k, _ := range rds.campaigns {
		camurl, err := url.Parse(k)
		if err != nil {
			http.Error(w, fmt.Sprintf("raw data store error: campaign %s not ok", k), 500)
			return
		}
		out.Campaigns = append(out.Campaigns, camurl.String())
	}

	outb, err := json.Marshal(out)
	if err != nil {
		http.Error(w, "raw data store error: cannot marshal campaign list", 500)
		return
	}

	// FIXME pagination goes here

	w.Header()["Content-Type"] = []string{"application/json"}
	w.Write(outb)
}

func (rds *RawDataStore) HandleGetCampaignMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// get campaign name
	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", 400)
		return
	}

	// fail if not authorized
	if !IsAuthorized(w, r, "read_raw:"+camname) {
		return
	}

	// look up campaign
	cam, ok := rds.campaigns[camname]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), 404)
		return
	}

	out, err := cam.getCampaignMetadata()
	if err != nil {
		rdsHTTPError(w, err)
		return
	}

	outb, err := json.Marshal(out)
	if err != nil {
		http.Error(w, "raw data store error: cannot marshal campaign metadata", 500)
		return
	}

	w.Header()["Content-Type"] = []string{"application/json"}
	w.Write(outb)
}

func (rds *RawDataStore) HandlePutCampaignMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// get campaign name
	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", 400)
		return
	}

	// fail if not authorized
	if !IsAuthorized(w, r, "write_raw:"+camname) {
		return
	}

	// read metadata from request
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// unmarshal it
	var in RDSMetadata
	err = json.Unmarshal(b, in)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// now look up the campaign
	cam, ok := rds.campaigns[camname]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), 404)
		return
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
		http.Error(w, "raw data store error: cannot marshal campaign metadata", 500)
		return
	}

	w.Header()["Content-Type"] = []string{"application/json"}
	w.Write(outb)
}

func (rds *RawDataStore) HandleGetFileMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", 400)
		return
	}

	filename, ok := vars["file"]
	if !ok {
		http.Error(w, "missing file", 400)
		return
	}

	// fail if not authorized
	if !IsAuthorized(w, r, "read_raw:"+camname) {
		return
	}

	cam, ok := rds.campaigns[vars["campaign"]]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), 404)
		return
	}

	out, err := cam.getFileMetadata(filename)
	if err != nil {
		rdsHTTPError(w, err)
		return
	}

	outb, err := json.Marshal(out)
	if err != nil {
		http.Error(w, "raw data store error: cannot marshal file metadata", 500)
		return
	}

	w.Header()["Content-Type"] = []string{"application/json"}
	w.Write(outb)
}

func (rds *RawDataStore) HandlePutFileMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", 400)
		return
	}

	filename, ok := vars["file"]
	if !ok {
		http.Error(w, "missing file", 400)
		return
	}

	// fail if not authorized
	if !IsAuthorized(w, r, "write_raw:"+camname) {
		return
	}

	// read metadata from request
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// unmarshal it
	var in RDSMetadata
	err = json.Unmarshal(b, in)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	// now look up the campaign
	cam, ok := rds.campaigns[camname]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), 404)
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
		http.Error(w, "raw data store error: cannot marshal file metadata", 500)
		return
	}

	w.Header()["Content-Type"] = []string{"application/json"}
	w.Write(outb) // FIXME log error here
}

func (rds *RawDataStore) HandleDeleteFile(w http.ResponseWriter, r *http.Request) {

}

func (rds *RawDataStore) HandleFileDownload(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)

	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", 400)
		return
	}

	filename, ok := vars["file"]
	if !ok {
		http.Error(w, "missing file", 400)
		return
	}

	// fail if not authorized
	if !IsAuthorized(w, r, "write_raw:"+camname) {
		return
	}

	// now look up the campaign
	cam, ok := rds.campaigns[camname]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), 404)
		return
	}

	// determine MIME type
	ft, err := cam.getFiletype(filename)

	// build a local filesystem path for downloading and validate it
	rawpath := filepath.Clean(filepath.Join(rds.path, camname, filename))
	if pathok, _ := filepath.Match(filepath.Join(rds.path, "*", "*"), rawpath); !pathok {
		http.Error(w, fmt.Sprintf("path %s is not ok", rawpath), 400)
		return
	}

	// write MIME type to header
	w.Header()["Content-Type"] = []string{ft.ContentType}

	// now stream the file to the writer
	rawfile, err := os.Open(rawpath)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	buf := make([]byte, 65536)
	for {
		n, err := rawfile.Read(buf)
		if err == nil {
			if n == 0 {
				break
			}
			w.Write(buf[0:n]) // FIXME log error here
		} else {
			http.Error(w, err.Error(), 500)
			return
		}
	}
}

func (rds *RawDataStore) HandleFileUpload(w http.ResponseWriter, r *http.Request) {

}

// NewRawDataStore encapsulates a raw data store, given a pathname of a
// directory containing its files.
func NewRawDataStore(config *PTOServerConfig, path string) (*RawDataStore, error) {
	rds := RawDataStore{config: config, path: path}

	// now scan the directory for campaigns
	rds.scanCampaigns()

	return &rds, nil
}
