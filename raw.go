package gopto

import (
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
)

// CampaignMetadataFilename is the name of each campaign metadata file in each campaign directory
const CampaignMetadataFilename = "ptocampaign_metadata.json"

// FileMetadataSuffix is the suffix on each metadata file
const FileMetadataSuffix = ".ptofile_metadata.json"

// RDSCampaign encapsulates a single campaign in a raw data store,
// and caches metadata for the campaign and files within it.
type RDSCampaign struct {
	// campaign path
	path string

	// campaign metadata cache
	campaignMetadata map[string]string

	// file metadata cache
	fileMetadata map[string]map[string]string
}

func NewRDSCampaign(path string) *RDSCampaign {
	return &RDSCampaign{
		campaignMetadata: make(map[string]string),
		fileMetadata:     make(map[string]map[string]string),
	}
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

	// make a new campaign map
	rds.campaigns = make(map[string]*RDSCampaign)

	direntries, err := ioutil.ReadDir(rds.path)

	if err != nil {
		return err
	}

	for _, direntry := range direntries {
		if direntry.IsDir() {
			mdpath := filepath.Join(rds.path, direntry.Name(), CampaignMetadataFilename)
			_, err := os.Stat(mdpath)
			if err != nil {
				continue // this isn't a directory that we care about, skip it silently
			}
			rds.campaigns[direntry.Name()] = NewRDSCampaign(filepath.Join(rds.path, direntry.Name()))
		}
	}

	return nil
}

type CampaignList struct {
	Campaigns []string `json:"campaigns"`
}

func (rds *RawDataStore) HandleListCampaigns(w http.ResponseWriter, r *http.Request) {

	if !IsAuthorized(w, r, "list_raw") {
		return
	}

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
