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

func (cam *RDSCampaign) GetFileMetadata(filename string) (*RDSMetadata, error) {
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

func (cam *RDSCampaign) ReadFileData(filename string) (io.ReadCloser, error) {

	// build a local filesystem path and validate it
	rawpath := filepath.Clean(filepath.Join(cam.path, filename))
	if pathok, _ := filepath.Match(filepath.Join(cam.path, "*"), rawpath); !pathok {
		return nil, fmt.Errorf("path %s is not ok", rawpath)
	}

	// open the file
	return os.Open(rawpath)
}

// A RawDataStore encapsulates a pile of PTO data and metadata files as a set of
// campaigns.
type RawDataStore struct {
	// application configuration
	config *PTOServerConfig

	// authorizer
	azr Authorizer

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

// Ensure that the directories backing the data store exist. Used for testing.
func (rds *RawDataStore) CreateDirectories() error {
	return os.Mkdir(rds.path, 0755)
}

// Remove the directories backing the data store incluing all their contents.
// Used for testing.
func (rds *RawDataStore) RemoveDirectories() error {
	return os.RemoveAll(rds.path)
}

func (rds *RawDataStore) CampaignForName(camname string) (*RDSCampaign, error) {
	// force a campaign rescan
	err := rds.scanCampaigns()
	if err != nil {
		return nil, err
	}

	// die if campaign not found
	cam, ok := rds.campaigns[camname]
	if !ok {
		return nil, fmt.Errorf("campaign %s does not exist")
	}

	return cam, nil
}

// NewRawDataStore encapsulates a raw data store, given a pathname of a
// directory containing its files.
func NewRawDataStore(config *PTOServerConfig, azr Authorizer) (*RawDataStore, error) {
	rds := RawDataStore{config: config, azr: azr, path: config.RawRoot}

	// scan the directory for campaigns
	if err := rds.scanCampaigns(); err != nil {
		return nil, err
	}

	return &rds, nil
}
