// Path Transparency Observatory Server

package main

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
	"github.com/mami-project/gopto"
)

type PTOServerConfiguration struct {
	BaseURL *url.URL
	RawRoot string
}

func (psc *PTOServerConfiguration) HandleRoot(w http.ResponseWriter, r *http.Request) {

	rawrel, _ := url.Parse("raw")

	links := map[string]string{
		"raw": psc.BaseURL.ResolveReference(rawrel).String(),
	}

	linksj, err := json.Marshal(links)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(linksj)
}

func NewPTOServerConfiguration() *PTOServerConfiguration {
	bu, _ := url.Parse("http://localhost:8080/")

	psc := PTOServerConfiguration{BaseURL: bu}

	// FIXME read from dotfile
	return &psc
}

func main() {
	// create a RawDataStore around the given RDS path

	// now hook up routes
	r := mux.NewRouter()

	psc := NewPTOServerConfiguration()

	r.HandleFunc("/", psc.HandleRoot)

	rds := gopto.NewRawDataStore(psc.RawRoot)

	r.HandleFunc("/raw", rds.HandleListCampaigns).methods("GET")
	r.HandleFunc("/raw/{campaign}", rds.HandleGetCampaignMetadata).methods("GET")
	r.HandleFunc("/raw/{campaign}", rds.HandlePutCampaignMetadata).methods("PUT")
	r.HandleFunc("/raw/{campaign}/{file}", rds.HandleGetFileMetadata).methods("GET")
	r.HandleFunc("/raw/{campaign}/{file}", rds.HandlePutFileMetadata).methods("PUT")
	r.HandleFunc("/raw/{campaign}/{file}", rds.HandleDeleteFile).methods("DELETE")
	r.HandleFunc("/raw/{campaign}/{file}/data", rds.HandleFileDownload).methods("GET")
	r.HandleFunc("/raw/{campaign}/{file}/data", rds.HandleFileDownload).methods("PUT")

	log.Fatal(http.ListenAndServe(":8000", r))
}
