package pto3

import (
	"encoding/json"
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

// RawMetadata represents metadata for a raw data object (file or campaign)
type RawMetadata struct {
	// Parent metadata object (campaign metadata, for files)
	Parent *RawMetadata
	// Name of filetype
	filetype string
	// Owner identifier
	Owner string
	// Start time for records in the file
	TimeStart *time.Time
	// End time for records in the file
	TimeEnd *time.Time
	// Arbitrary metadata
	Metadata map[string]string
	// Link to data object
	datalink string
	// Size of data object
	datasize int
}

// DumpJSONObject serializes a RawMetadata object to JSON. If inherit is true,
// this inherits data and metadata items from the parent; if false, it only
// dumps information in this object itself.
func (md *RawMetadata) DumpJSONObject(inherit bool) ([]byte, error) {
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

// MarshalJSON serializes a RawMetadata object to JSON. All values inherited
// from the parent, if present, are also serialized see DumpJSONObject for
// control over inheritance.
func (md *RawMetadata) MarshalJSON() ([]byte, error) {
	// by default, serialize object with all inherited information
	return md.DumpJSONObject(true)
}

// UnmarshalJSON fills in a RawMetadata object from JSON.
func (md *RawMetadata) UnmarshalJSON(b []byte) error {
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

// writeToFile writes this RawMetadata object as JSON to a file.
func (md *RawMetadata) writeToFile(pathname string) error {
	b, err := md.DumpJSONObject(false)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(pathname, b, 0644)
}

// validate returns nil if the metadata is valid (i.e., it or its parent has all required keys), or an error if not
func (md *RawMetadata) validate(isCampaign bool) error {
	if md.Owner == "" && (md.Parent == nil || md.Parent.Owner == "") {
		return PTOMissingMetadataError("_owner")
	}

	// short circuit file-only checks
	if isCampaign {
		return nil
	}

	if md.filetype == "" && (md.Parent == nil || md.Parent.filetype == "") {
		return PTOMissingMetadataError("_file_type")
	}

	if md.TimeStart == nil && (md.Parent == nil || md.Parent.TimeStart == nil) {
		return PTOMissingMetadataError("_time_start")
	}

	if md.TimeEnd == nil && (md.Parent == nil || md.Parent.TimeEnd == nil) {
		return PTOMissingMetadataError("_time_end")
	}

	return nil
}

// Filetype returns the filetype associated with a given metadata object, or inherited from its parent.
func (md *RawMetadata) Filetype() string {
	if md.filetype == "" && md.Parent != nil {
		return md.Parent.filetype
	}

	return md.filetype
}

// RawMetadataFromReader reads metadata for a raw data file from a stream. It
// creates a new RawMetadata object bound to an optional parent.
func RawMetadataFromReader(r io.Reader, parent *RawMetadata) (*RawMetadata, error) {
	var md RawMetadata

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

// RawMetadataFromFile reads metadata for a raw data file from a file. It
// creates a new RawMetadata object bound to an optional parent.
func RawMetadataFromFile(pathname string, parent *RawMetadata) (*RawMetadata, error) {
	f, err := os.Open(pathname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return RawMetadataFromReader(f, parent)
}

// RawFiletype encapsulates a filetype in the raw data store FIXME not quite the right type
type RawFiletype struct {
	// PTO filetype name
	Filetype string `json:"file_type"`
	// Associated MIME type
	ContentType string `json:"mime_type"`
}

// Campaign encapsulates a single campaign in a raw data store,
// and caches metadata for the campaign and files within it.
type Campaign struct {
	// application configuration
	config *PTOConfiguration

	// path to campaign directory
	path string

	// requires metadata reload
	stale bool

	// campaign metadata cache
	campaignMetadata *RawMetadata

	// file metadata cache; keys of this define known filenames
	fileMetadata map[string]*RawMetadata

	// lock on metadata structures
	lock sync.RWMutex
}

// newCampaign creates a new campaign object bound the path of a directory on
// disk containing the campaign's files. If a pointer to metadata is given, it
// creates a new campaign directory on disk with the given metadata. Error can
// be ignored if metadata is nil.
func newCampaign(config *PTOConfiguration, name string, md *RawMetadata) (*Campaign, error) {

	cam := &Campaign{
		config:       config,
		path:         filepath.Join(config.RawRoot, name),
		stale:        true,
		fileMetadata: make(map[string]*RawMetadata),
	}

	// metadata means try to create new campaign
	if md != nil {

		// okay, we're trying to make a new campaign. first, make sure campaign metadata is ok
		if err := md.validate(true); err != nil {
			return nil, err
		}

		// then check to see if the campaign directory exists
		_, err := os.Stat(cam.path)
		if (err == nil) || !os.IsNotExist(err) {
			return nil, PTOExistsError("campaign", name)
		}

		// create directory
		if err := os.Mkdir(cam.path, 0755); err != nil {
			return nil, err
		}

		// write metadata to campaign metadata file
		if err := md.writeToFile(filepath.Join(cam.path, CampaignMetadataFilename)); err != nil {
			return nil, err
		}

		// and force a rescan
		if err := cam.reloadMetadata(true); err != nil {
			return nil, err
		}

	}

	return cam, nil

}

// reloadMetadata reloads the metadata for this campaign and its files from disk
func (cam *Campaign) reloadMetadata(force bool) error {
	var err error

	cam.lock.Lock()
	defer cam.lock.Unlock()

	// skip if not stale
	if !force && !cam.stale {
		return nil
	}

	// load the campaign metadata file
	cam.campaignMetadata, err = RawMetadataFromFile(filepath.Join(cam.path, CampaignMetadataFilename), nil)
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
				RawMetadataFromFile(filepath.Join(cam.path, metafilename), cam.campaignMetadata)
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
func (cam *Campaign) unloadMetadata() {
	cam.lock.Lock()
	defer cam.lock.Unlock()

	cam.campaignMetadata = nil
	cam.fileMetadata = nil
	cam.stale = true
}

// GetCampaignMetadata returns the metadata for this campaign.
func (cam *Campaign) GetCampaignMetadata() (*RawMetadata, error) {
	// reload if stale
	err := cam.reloadMetadata(false)
	if err != nil {
		return nil, err
	}

	return cam.campaignMetadata, nil
}

// PutCampaignMetadata overwrites the metadata for this campaign with the given metadata.
func (cam *Campaign) PutCampaignMetadata(md *RawMetadata) error {
	cam.lock.Lock()
	defer cam.lock.Unlock()

	// make sure campaign metadata is ok
	if err := md.validate(true); err != nil {
		return err
	}

	// write to campaign metadata file
	if err := md.writeToFile(filepath.Join(cam.path, CampaignMetadataFilename)); err != nil {
		return err
	}

	// update metadata cache
	cam.campaignMetadata = md
	return nil
}

// FileNames returns a list of filenames currently in the campaign.
func (cam *Campaign) FileNames() ([]string, error) {
	// reload if stale
	err := cam.reloadMetadata(false)
	if err != nil {
		return nil, err
	}

	cam.lock.RLock()
	defer cam.lock.RUnlock()
	out := make([]string, len(cam.fileMetadata))
	i := 0
	for filename := range cam.fileMetadata {
		out[i] = filename
		i++
	}

	return out, nil
}

// FileLinks returns a list of links to files currently in the campaign.
func (cam *Campaign) FileLinks() ([]string, error) {
	// reload if stale
	err := cam.reloadMetadata(false)
	if err != nil {
		return nil, err
	}

	cam.lock.RLock()
	defer cam.lock.RUnlock()
	out := make([]string, len(cam.fileMetadata))
	i := 0
	for filename := range cam.fileMetadata {
		out[i], _ = cam.config.LinkTo("raw/" + filepath.Base(cam.path) + "/" + filename)
		i++
	}

	return out, nil
}

// GetFileMetadata retrieves metadata for a file in this campaign given a file name.
func (cam *Campaign) GetFileMetadata(filename string) (*RawMetadata, error) {
	// reload if stale
	err := cam.reloadMetadata(false)
	if err != nil {
		return nil, err
	}

	// check for file metadata
	filemd, ok := cam.fileMetadata[filename]
	if !ok {
		return nil, PTONotFoundError("file", filename)
	}

	return filemd, nil
}

// updateFileVirtualMetadata fills in the __data and __data_size virtual metadata
// for a file. Not concurrency safe: caller must hold the campaign lock.
func (cam *Campaign) updateFileVirtualMetadata(filename string) error {
	// get file metadata
	md, ok := cam.fileMetadata[filename]
	if !ok {
		return PTONotFoundError("file", filename)
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
	md.datalink, err = cam.config.LinkTo("raw/" + filepath.Base(cam.path) + "/" + filename + "/data")
	if err != nil {
		return err
	}

	return nil
}

// PutFileMetadata overwrites the metadata in this campaign with the given metadata.
func (cam *Campaign) PutFileMetadata(filename string, md *RawMetadata) error {
	cam.lock.Lock()
	defer cam.lock.Unlock()

	// inherit from campaign
	md.Parent = cam.campaignMetadata

	// ensure we have a filetype
	if md.Filetype() == "" {
		return PTOMissingMetadataError("_file_type")
	}

	// write to file metadata file
	err := md.writeToFile(filepath.Join(cam.path, filename+FileMetadataSuffix))
	if err != nil {
		return err
	}

	// update metadata cache
	cam.fileMetadata[filename] = md

	// and update virtuals
	return cam.updateFileVirtualMetadata(filename)
}

// GetFiletype returns the filetype associated with a given file in this campaign.
func (cam *Campaign) GetFiletype(filename string) *RawFiletype {
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

	return &RawFiletype{ftname, ctype}
}

// ReadFileData opens and returns the data file associated with a filename on this campaign for reading.
func (cam *Campaign) ReadFileData(filename string) (*os.File, error) {
	// build a local filesystem path and validate it
	rawpath := filepath.Clean(filepath.Join(cam.path, filename))
	if pathok, _ := filepath.Match(filepath.Join(cam.path, "*"), rawpath); !pathok {
		return nil, PTOErrorf("path %s is not ok", rawpath).StatusIs(http.StatusInternalServerError)
	}

	// open the file
	return os.Open(rawpath)
}

// ReadFileDataToStream copies data from the data file associated with a
// filename on this campaign to a given writer.
func (cam *Campaign) ReadFileDataToStream(filename string, out io.Writer) error {
	in, err := cam.ReadFileData(filename)
	if err != nil {
		return err
	}
	defer in.Close()

	// now copy to the writer until EOF
	if err := StreamCopy(in, out); err != nil {
		return err
	}

	return nil
}

// WriteDataFile creates, open and returns the data file associated with a
// filename on this campaign for writing.If force is true, replaces the data
// file if it exists; otherwise, returns an error if the data file exists.
func (cam *Campaign) WriteFileData(filename string, force bool) (*os.File, error) {
	// build a local filesystem path and validate it
	rawpath := filepath.Clean(filepath.Join(cam.path, filename))
	if pathok, _ := filepath.Match(filepath.Join(cam.path, "*"), rawpath); !pathok {
		return nil, PTOErrorf("path %s is not ok", rawpath).StatusIs(http.StatusInternalServerError)
	}

	// ensure file isn't there unless we're forcing overwrite
	if !force {
		_, err := os.Stat(rawpath)
		if (err == nil) || !os.IsNotExist(err) {
			return nil, PTOExistsError("file", filename)
		}
	}

	// create file to write to
	return os.Create(rawpath)
}

// WriteFileDataFromStream copies data from a given reader to the data file
// associated with a filename on this campaign. If force is true, replaces the
// data file if it exists; otherwise, returns an error if the data file exists.
func (cam *Campaign) WriteFileDataFromStream(filename string, force bool, in io.Reader) error {
	out, err := cam.WriteFileData(filename, force)
	if err != nil {
		return err
	}
	defer out.Close()

	// now copy from the reader until EOF
	if err := StreamCopy(in, out); err != nil {
		return err
	}

	// update virtual metadata, as the underlying file size will have changed
	if err := out.Sync(); err != nil {
		return err
	}
	cam.lock.Lock()
	defer cam.lock.Unlock()
	return cam.updateFileVirtualMetadata(filename)
}

// A RawDataStore encapsulates a pile of PTO data and metadata files as a set of
// campaigns.
type RawDataStore struct {
	// application configuration
	config *PTOConfiguration

	// base path
	path string

	// lock on campaign cache
	lock sync.RWMutex

	// campaign cache
	campaigns map[string]*Campaign
}

// ScanCampaigns updates the campaign cache in RawDataStore to reflect the
// current state of the files on disk.
func (rds *RawDataStore) ScanCampaigns() error {
	rds.lock.Lock()
	defer rds.lock.Unlock()

	rds.campaigns = make(map[string]*Campaign)

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
			cam, _ := newCampaign(rds.config, filepath.Join(rds.path, direntry.Name()), nil)
			rds.campaigns[direntry.Name()] = cam
		}
	}

	return nil
}

// CreateCampaign creates a new campaign given a campaign name and initial metadata for the new campaign.
func (rds *RawDataStore) CreateCampaign(camname string, md *RawMetadata) (*Campaign, error) {
	cam, err := newCampaign(rds.config, camname, md)
	if err != nil {
		return nil, err
	}

	err = cam.PutCampaignMetadata(md)
	if err != nil {
		return nil, err
	}

	rds.lock.Lock()
	rds.campaigns[camname] = cam
	rds.lock.Unlock()

	return cam, nil
}

// CampaignForName returns a campaign object for a given name.
func (rds *RawDataStore) CampaignForName(camname string) (*Campaign, error) {
	// die if campaign not found
	cam, ok := rds.campaigns[camname]
	if !ok {
		return nil, PTONotFoundError("campaign", camname)
	}

	return cam, nil
}

func (rds *RawDataStore) CampaignNames() []string {
	// return list of names
	rds.lock.RLock()
	defer rds.lock.RUnlock()
	out := make([]string, len(rds.campaigns))
	i := 0
	for k := range rds.campaigns {
		out[i] = k
		i++
	}
	return out
}

// NewRawDataStore encapsulates a raw data store, given a configuration object
// pointing to a directory containing data and metadata organized into campaigns.
func NewRawDataStore(config *PTOConfiguration) (*RawDataStore, error) {
	rds := RawDataStore{config: config, path: config.RawRoot}

	// scan the directory for campaigns
	if err := rds.ScanCampaigns(); err != nil {
		return nil, err
	}

	return &rds, nil
}

// StreamCopy copies bytes from in to out until EOF.
func StreamCopy(in io.Reader, out io.Writer) error {
	buf := make([]byte, 65536)
	for {
		n, err := in.Read(buf)
		if err == nil {
			if _, err = out.Write(buf[0:n]); err != nil {
				return err
			}
		} else if err == io.EOF {
			return nil
		} else {
			return err
		}
	}
}
