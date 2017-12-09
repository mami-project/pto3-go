package papi

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

	"github.com/mami-project/pto3-go"

	"github.com/gorilla/mux"
)

type RawAPI struct {
	config *pto3.PTOConfiguration
	rds    *pto3.RawDataStore
	azr    Authorizer
}

// handleListCampaigns handles GET /raw, returning a list of campaigns in the
// raw data store. It writes a JSON object to the response with a single key,
// "campaigns", whose content is an array of campaign URL as strings.
func (ra *RawAPI) handleListCampaigns(w http.ResponseWriter, r *http.Request) {

	// fail if not authorized
	if !ra.azr.IsAuthorized(w, r, "list_raw") {
		return
	}

	// force a campaign rescan
	err := ra.ra.rds.ScanCampaigns()
	if err != nil {
		LogInternalServerError(w, "scanning campaigns", err)
		return
	}

	// construct URLs based on the campaign
	out := campaignList{make([]string, len(ra.rds.campaigns))}

	i := 0
	for k := range ra.ra.rds.campaigns {
		camurl, err := url.Parse(fmt.Sprintf("raw/%s", k))
		if err != nil {
			LogInternalServerError(w, "generating campaign link", err)
			return
		}
		out.Campaigns[i] = ra.config.baseURL.ResolveReference(camurl).String()
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
	Metadata *pto3.RawMetadata `json:"metadata"`
	Files    []string          `json:"files"`
}

// handleGetCampaignMetadata handles GET /raw/<campaign>, returning metadata for
// a campaign. It writes a JSON object to the response containing campaign
// metadata.
func (ra *RawAPI) handleGetCampaignMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// get campaign name
	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", http.StatusBadRequest)
		return
	}

	// fail if not authorized
	if !ra.azr.IsAuthorized(w, r, "read_raw:"+camname) {
		return
	}

	// look up campaign
	cam, ok := ra.rds.campaigns[camname]
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
		out.Files[i] = ra.config.baseURL.ResolveReference(filepath).String()
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

// handlePutCampaignMetadata handles PUT /raw/<campaign>, overwriting metadata for
// a campaign, creating it if necessary. It requires a JSON object in the
// request body containing campaign metadata. It echoes the written metadata
// back in the response.
func (ra *RawAPI) handlePutCampaignMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// get campaign name
	camname, ok := vars["campaign"]
	if !ok {
		http.Error(w, "missing campaign", http.StatusBadRequest)
		return
	}

	// fail if not authorized
	if !ra.azr.IsAuthorized(w, r, "write_raw:"+camname) {
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
	var in RawMetadata
	err = json.Unmarshal(b, &in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// now look up the campaign
	cam, ok := ra.rds.campaigns[camname]
	if !ok {
		// Campaign doesn't exist. We have to create it.
		cam, err = ra.rds.CreateCampaign(camname, &in)
		if err != nil {
			LogInternalServerError(w, fmt.Sprintf("creating campaign %s", camname), err)
			return
		}
	}

	// overwrite metadata
	err = cam.PutCampaignMetadata(&in)
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

// handleGetFileMetadata handles GET /raw/<campaign>/<file>, returning
// metadata for a file, including virtual metadata (file size and data URL) and
// any metadata inherited from the campaign. It writes a JSON object to the
// response containing file metadata.
func (ra *RawAPI) handleGetFileMetadata(w http.ResponseWriter, r *http.Request) {
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
	if !ra.azr.IsAuthorized(w, r, "read_raw:"+camname) {
		return
	}

	cam, ok := ra.rds.campaigns[vars["campaign"]]
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

// handlePutFileMetadata handles PUT /raw/<campaign>/<file>, overwriting metadata for
// a file, creating it if necessary. It requires a JSON object in the
// request body containing file metadata. It echoes the full file metadata
// back in the response, including inherited campaign metadata and any virtual metadata.
func (ra *RawAPI) handlePutFileMetadata(w http.ResponseWriter, r *http.Request) {
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
	if !ra.azr.IsAuthorized(w, r, "write_raw:"+camname) {
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
	var in RawMetadata
	err = json.Unmarshal(b, &in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// now look up the campaign
	cam, ok := ra.rds.campaigns[camname]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), http.StatusNotFound)
		return
	}

	// overwrite metadata for file
	err = cam.PutFileMetadata(filename, &in)
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

// handleDeleteFile handles DELETE /raw/<campaign>/<file>, deleting a file's
// metadata and content by marking it pending deletion in the raw data store.
// Deletion is not yet fully specified or implemented, so this just returns a
// StatusNotImplemented response for now.
func (ra *RawAPI) handleDeleteFile(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "delete not implemented, come back later", http.StatusNotImplemented)
}

// handleFileDownload handles GET /raw/<campaign>/<file>/data, returning a file's
// content. It writes a response of the appropriate MIME type for the file (as
// determined by the filetypes map and the _file_type metadata key).
func (ra *RawAPI) handleFileDownload(w http.ResponseWriter, r *http.Request) {

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
	if !ra.azr.IsAuthorized(w, r, "read_raw:"+camname) {
		return
	}

	// now look up the campaign
	cam, ok := ra.rds.campaigns[camname]
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

// handleFileUpload handles PUT /raw/<campaign>/<file>/data. It requires a request of the appropriate MIME type for the file (as
// determined by the filetypes map and the _file_type metadata key) whose body is the file's content. It writes a response containing the file's metadata.
func (ra *RawAPI) handleFileUpload(w http.ResponseWriter, r *http.Request) {
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
	if !ra.azr.IsAuthorized(w, r, "write_raw:"+camname) {
		return
	}

	// now look up the campaign
	cam, ok := ra.rds.campaigns[camname]
	if !ok {
		http.Error(w, fmt.Sprintf("campaign %s not found", vars["campaign"]), http.StatusNotFound)
		return
	}

	// build a local filesystem path for uploading and validate it
	rawpath := filepath.Clean(filepath.Join(ra.rds.path, camname, filename))
	if pathok, _ := filepath.Match(filepath.Join(ra.rds.path, "*", "*"), rawpath); !pathok {
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

func (ra *RawAPI) addRoutes(r *mux.Router) {
	l := ra.config.accessLogger
	r.handleFunc("/raw", LogAccess(l, ra.rds.handleListCampaigns)).Methods("GET")
	r.handleFunc("/raw/{campaign}", LogAccess(l, ra.rds.handleGetCampaignMetadata)).Methods("GET")
	r.handleFunc("/raw/{campaign}", LogAccess(l, ra.rds.handlePutCampaignMetadata)).Methods("PUT")
	r.handleFunc("/raw/{campaign}/{file}", LogAccess(l, ra.rds.handleGetFileMetadata)).Methods("GET")
	r.handleFunc("/raw/{campaign}/{file}", LogAccess(l, ra.rds.handlePutFileMetadata)).Methods("PUT")
	r.handleFunc("/raw/{campaign}/{file}", LogAccess(l, ra.rds.handleDeleteFile)).Methods("DELETE")
	r.handleFunc("/raw/{campaign}/{file}/data", LogAccess(l, ra.rds.handleFileDownload)).Methods("GET")
	r.handleFunc("/raw/{campaign}/{file}/data", LogAccess(l, ra.rds.handleFileUpload)).Methods("PUT")
}

func NewRawAPI(config *pto3.PTOConfiguration, azr Authorizer, r *mux.Router, l *log.Logger) (*RawAPI, error) {
	var err error

	if config.RawRoot == "" {
		return nil, nil
	}

	ra := new(RawAPI)
	ra.config = config
	ra.azr = azr
	if ra.rds, err = pto3.NewRawDataStore(config); err != nil {
		return nil, err
	}

	ra.addRoutes(r, l)

	return ra, nil
}
