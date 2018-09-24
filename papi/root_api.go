package papi

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gorilla/mux"
	pto3 "github.com/mami-project/pto3-go"
)

type RootAPI struct {
	config *pto3.PTOConfiguration
}

var staticMimeTypeTable = map[string]string{
	"jpg":  "image/jpeg",
	"jpeg": "image/jpeg",
	"png":  "image/png",
	"gif":  "image/gif",
	"htm":  "text/html",
	"html": "text/html",
	"css":  "text/css",
	"txt":  "text/plain",
	"json": "application/json",
	"js":   "application/javascript",
}

func (ra *RootAPI) mimeTypeForFilename(filename string) string {

	nameparts := strings.Split(filename, ".")
	suffix := strings.ToLower(nameparts[len(nameparts)-1])

	mimeType := staticMimeTypeTable[suffix]
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	return mimeType
}

func (ra *RootAPI) additionalHeaders(w http.ResponseWriter) {
	if ra.config.AllowOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", ra.config.AllowOrigin)
	}
}

func (ra *RootAPI) handleRootLinks(w http.ResponseWriter, r *http.Request) {

	links := make(map[string]string)

	links["banner"] = "This is an instance of the MAMI Path Transparency Observatory. See https://github.com/mami-project/pto3-go for more information."

	if ra.config.RawRoot != "" {
		links["raw"], _ = ra.config.LinkTo("raw")
	}

	if ra.config.ObsDatabase.Database != "" {
		links["obs"], _ = ra.config.LinkTo("obs")
	}

	if ra.config.QueryCacheRoot != "" {
		links["query"], _ = ra.config.LinkTo("query")
	}

	linksj, err := json.Marshal(links)

	if err != nil {
		pto3.HandleErrorHTTP(w, "marshaling root link list", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	ra.additionalHeaders(w)
	w.WriteHeader(http.StatusOK)
	w.Write(linksj)
}

func (ra *RootAPI) handleRootFile(w http.ResponseWriter, r *http.Request) {

	mimeType := ra.mimeTypeForFilename(ra.config.RootFile)

	file, err := os.Open(ra.config.RootFile)
	if err != nil {
		pto3.HandleErrorHTTP(w, "serving static root page", err)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", mimeType)
	ra.additionalHeaders(w)
	w.WriteHeader(http.StatusOK)
	io.Copy(w, file)
}

func (ra *RootAPI) handleStaticFile(w http.ResponseWriter, r *http.Request) {

	path := r.URL.Path
	if !strings.HasPrefix(path, "/static/") {
		pto3.HandleErrorHTTP(w, "serving static page", fmt.Errorf("internal error, unexpected static URL %v", r.URL))
		return
	}

	if strings.Contains(path, "/../") || strings.HasSuffix(path, "/..") {
		pto3.HandleErrorHTTP(w, "serving static page", fmt.Errorf("internal error, unexpected .. in url %v", r.URL))
		return
	}

	filename := filepath.Join(ra.config.StaticRoot, path[len("/static/"):len(path)])
	mimeType := ra.mimeTypeForFilename(filename)

	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "URL not found", http.StatusNotFound)
		} else {
			pto3.HandleErrorHTTP(w, "serving static content", err)
		}
	}
	defer file.Close()

	w.Header().Set("Content-Type", mimeType)
	ra.additionalHeaders(w)
	w.WriteHeader(http.StatusOK)
	io.Copy(w, file)
}

func (ra *RootAPI) addRoutes(r *mux.Router, l *log.Logger) {
	if ra.config.RootFile == "" {
		r.HandleFunc("/", LogAccess(l, ra.handleRootLinks)).Methods("GET")
	} else {
		r.HandleFunc("/", LogAccess(l, ra.handleRootFile)).Methods("GET")
	}

	if ra.config.StaticRoot != "" {
		r.PathPrefix("/static/").Methods("GET").HandlerFunc(LogAccess(l, ra.handleStaticFile))
	}
}

func NewRootAPI(config *pto3.PTOConfiguration, azr Authorizer, r *mux.Router) *RootAPI {
	ra := new(RootAPI)
	ra.config = config
	ra.addRoutes(r, config.AccessLogger())
	return ra
}
