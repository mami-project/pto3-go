package papi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/mami-project/pto3-go"

	"github.com/gorilla/mux"
)

type RawAPI struct {
	config *pto3.PTOConfiguration
	rds    *pto3.RawDataStore
	azr    Authorizer
}

func rawMetadataResponse(w http.ResponseWriter, status int, cam *pto3.Campaign, filename string) {
	var md *pto3.RawMetadata
	var err error
	if filename == "" {
		md, err = cam.GetCampaignMetadata()
	} else {
		md, err = cam.GetFileMetadata(filename)
	}
	if err != nil {
		pto3.HandleErrorHTTP(w, "retrieving metadata", err)
		return
	}

	b, err := json.Marshal(md)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshalling metadata", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(b)
}

type campaignList struct {
	Campaigns []string `json:"campaigns"`
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
	err := ra.rds.ScanCampaigns()
	if err != nil {
		pto3.HandleErrorHTTP(w, "scanning campaigns", err)
		return
	}

	// construct URLs based on the campaign
	camnames := ra.rds.CampaignNames()
	out := campaignList{Campaigns: make([]string, len(camnames))}
	for i, camname := range camnames {
		out.Campaigns[i], _ = ra.config.LinkTo(fmt.Sprintf("raw/%s", camname))
	}

	// FIXME pagination goes here

	outb, err := json.Marshal(out)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshaling campaign list", err)
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
	cam, err := ra.rds.CampaignForName(camname)
	if err != nil {
		pto3.HandleErrorHTTP(w, "retrieving campaign", err)
		return
	}

	var out campaignFileList
	out.Metadata, err = cam.GetCampaignMetadata()
	if err != nil {
		pto3.HandleErrorHTTP(w, "getting campaign metadata", err)
		return
	}

	out.Files, err = cam.FileLinks()
	if err != nil {
		pto3.HandleErrorHTTP(w, "listing campaign files", err)
		return
	}

	outb, err := json.Marshal(out)
	if err != nil {
		pto3.HandleErrorHTTP(w, "marshaling campaign metadata", err)
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
	var in pto3.RawMetadata
	err = json.Unmarshal(b, &in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// now look up the campaig and create if necessary.
	cam, err := ra.rds.CampaignForName(camname)
	didCreateCampaign := false
	if err != nil {
		switch ev := err.(type) {
		case *pto3.PTOError:
			if ev.Status() == http.StatusNotFound {
				// Campaign doesn't exist. We have to create it.
				cam, err = ra.rds.CreateCampaign(camname, &in)
				if err != nil {
					pto3.HandleErrorHTTP(w, "creating campaign", err)
					return
				}
				didCreateCampaign = true
			} else {
				pto3.HandleErrorHTTP(w, "retrieving campaign", err)
				return
			}
		default:
			pto3.HandleErrorHTTP(w, "retrieving campaign", err)
			return
		}
	}

	// overwrite metadata unless we created the campaign
	if !didCreateCampaign {
		err = cam.PutCampaignMetadata(&in)
		if err != nil {
			pto3.HandleErrorHTTP(w, "writing metadata", err)
			return
		}
	}

	rawMetadataResponse(w, http.StatusCreated, cam, "")
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

	cam, err := ra.rds.CampaignForName(camname)
	if err != nil {
		pto3.HandleErrorHTTP(w, "retrieving campaign", err)
		return
	}

	rawMetadataResponse(w, http.StatusOK, cam, filename)
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
	var in pto3.RawMetadata
	err = json.Unmarshal(b, &in)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// now look up the campaign
	cam, err := ra.rds.CampaignForName(camname)
	if err != nil {
		pto3.HandleErrorHTTP(w, "retrieving campaign", err)
		return
	}

	// overwrite metadata for file
	err = cam.PutFileMetadata(filename, &in)
	if err != nil {
		pto3.HandleErrorHTTP(w, "writing file metadata", err)
		return
	}

	rawMetadataResponse(w, http.StatusCreated, cam, filename)
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
	cam, err := ra.rds.CampaignForName(camname)
	if err != nil {
		pto3.HandleErrorHTTP(w, "retrieving campaign", err)
		return
	}

	// determine MIME type
	ft := cam.GetFiletype(filename)
	if ft == nil {
		pto3.HandleErrorHTTP(w, fmt.Sprintf("determining filetype for %s", filename), nil)
		return
	}

	// write MIME type to header
	w.Header().Set("Content-Type", ft.ContentType)
	w.WriteHeader(http.StatusOK)

	// and copy the file
	if err := cam.ReadFileDataToStream(filename, w); err != nil {
		pto3.HandleErrorHTTP(w, "downloading data file", err)
		w.Write([]byte("\n\"error during download\"\n"))

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
	cam, err := ra.rds.CampaignForName(camname)
	if err != nil {
		pto3.HandleErrorHTTP(w, "retrieving campaign", err)
		return
	}

	// determine and verify MIME type
	ft := cam.GetFiletype(filename)
	if ft == nil {
		// fixme another way to do this?
		pto3.HandleErrorHTTP(w, fmt.Sprintf("getting filetype for %s", filename), nil)
		return
	}
	if ft.ContentType != r.Header.Get("Content-Type") {
		http.Error(w, fmt.Sprintf("Content-Type for %s/%s must be %s", camname, filename, ft.ContentType), http.StatusBadRequest)
		return
	}

	reqreader, err := r.GetBody()
	if err != nil {
		pto3.HandleErrorHTTP(w, "reading upload data", err)
		return
	}

	if err := cam.WriteFileDataFromStream(filename, false, reqreader); err != nil {
		pto3.HandleErrorHTTP(w, "writing uploaded data", err)
		return
	}

	// and now a reply... return file metadata
	rawMetadataResponse(w, http.StatusCreated, cam, filename)
}

func (ra *RawAPI) addRoutes(r *mux.Router, l *log.Logger) {
	r.HandleFunc("/raw", LogAccess(l, ra.handleListCampaigns)).Methods("GET")
	r.HandleFunc("/raw/{campaign}", LogAccess(l, ra.handleGetCampaignMetadata)).Methods("GET")
	r.HandleFunc("/raw/{campaign}", LogAccess(l, ra.handlePutCampaignMetadata)).Methods("PUT")
	r.HandleFunc("/raw/{campaign}/{file}", LogAccess(l, ra.handleGetFileMetadata)).Methods("GET")
	r.HandleFunc("/raw/{campaign}/{file}", LogAccess(l, ra.handlePutFileMetadata)).Methods("PUT")
	r.HandleFunc("/raw/{campaign}/{file}", LogAccess(l, ra.handleDeleteFile)).Methods("DELETE")
	r.HandleFunc("/raw/{campaign}/{file}/data", LogAccess(l, ra.handleFileDownload)).Methods("GET")
	r.HandleFunc("/raw/{campaign}/{file}/data", LogAccess(l, ra.handleFileUpload)).Methods("PUT")
}

func NewRawAPI(config *pto3.PTOConfiguration, azr Authorizer, r *mux.Router) (*RawAPI, error) {
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

	ra.addRoutes(r, config.AccessLogger())

	return ra, nil
}
