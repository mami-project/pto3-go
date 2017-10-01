package gopto

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// CampaignMetadataFilename is the name of each campaign metadata file in each campaign directory
const CampaignMetadataFilename = "ptocampaign_metadata.json"

// FileMetadataSuffix is the suffix on each metadata file
const FileMetadataSuffix = ".ptofile_metadata.json"

// RDSMetadata
type RDSMetadata map[string]interface{}

func ReadRDSMetadata(pathname string) (out RDSMetadata, err error) {
	rmd, err := ioutil.ReadFile(pathname)
	if err == nil {
		err = json.Unmarshal(rmd, out)
	}
	return
}

// RDSCampaign encapsulates a single campaign in a raw data store,
// and caches metadata for the campaign and files within it.
type RDSCampaign struct {
	// path to campaign directory
	path string

	// requires metadata reload
	stale bool

	// campaign metadata cache
	campaignMetadata RDSMetadata

	// file metadata cache; keys of this define known filenames
	fileMetadata map[string]RDSMetadata
}

// reloadMetadata reloads the metadata for this campaign and its files from disk
func (cam *RDSCampaign) reloadMetadata() error {
	var err error

	// load the campaign metadata file
	cam.campaignMetadata, err = ReadRDSMetadata(filepath.Join(cam.path, CampaignMetadataFilename))
	if err != nil {
		return err
	}

	// now scan directory and load each metadata file
	direntries, err := ioutil.ReadDir(cam.path)
	for _, direntry := range direntries {
		if strings.HasSuffix(direntry.Name(), FileMetadataSuffix) {
			cam.fileMetadata[direntry.Name()], err = ReadRDSMetadata(filepath.Join(cam.path, direntry.Name()))
			if err != nil {
				return err
			}
		}
	}

	// everything loaded fine, mark not stale
	cam.stale = false
	return nil
}

func (cam *RDSCampaign) getCampaignMetadata() (RDSMetadata, error) {

	// WORK POINTER

}

func (cam *RDSCampaign) putCampaignMetadata(md RDSMetadata) error {

	return nil
}

func (cam *RDSCampaign) getFileMetadata() (RDSMetadata, error) {

	return nil, nil
}

func (cam *RDSCampaign) putFileMetadata(filename string, md RDSMetadata) error {

	return nil
}

func NewRDSCampaign(path string) *RDSCampaign {
	cam := RDSCampaign{path: path, stale: true}

	return &cam
}

// A RawDataStore encapsulates a pile of PTO data and metadata files as a set of
// campaigns.
type RawDataStore struct {
	// base path
	path string

	// campaign cache
	campaigns map[string]*RDSCampaign
}

// scanCampaigns updates the RawDataStore to reflect the current state of the files on disk
func (rds *RawDataStore) scanCampaigns() error {

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
				continue // no metadata file means we don't care about this directory
			}

			// create a new campaign
			rds.campaigns[direntry.Name()] = NewRDSCampaign(filepath.Join(rds.path, direntry.Name()))
		}
	}

	return nil
}

type CampaignList struct {
	Campaigns []string `json:"campaigns"`
}

func (rds *RawDataStore) HandleListCampaigns(w http.ResponseWriter, r *http.Request) {

	// fail if not authorized
	if !IsAuthorized(w, r, "list_raw") {
		return
	}

	// construct URLs based on the campaign
}

func (rds *RawDataStore) HandleGetCampaignMetadata(w http.ResponseWriter, r *http.Request) {

}

func (rds *RawDataStore) HandlePutCampaignMetadata(w http.ResponseWriter, r *http.Request) {

}

func (rds *RawDataStore) HandleGetFileMetadata(w http.ResponseWriter, r *http.Request) {

}

func (rds *RawDataStore) HandlePutFileMetadata(w http.ResponseWriter, r *http.Request) {

}

func (rds *RawDataStore) HandleFileDownload(w http.ResponseWriter, r *http.Request) {

}

func (rds *RawDataStore) HandleFileUpload(w http.ResponseWriter, r *http.Request) {

}

// NewRawDataStore encapsulates a raw data store, given a pathname of a
// directory containing its files.
func NewRawDataStore(path string) (*RawDataStore, error) {
	rds := RawDataStore{path: path}

	// now scan the directory for campaigns
	rds.scanCampaigns()

	return &rds, nil
}
