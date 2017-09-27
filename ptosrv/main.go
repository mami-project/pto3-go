// Path Transparency Observatory Server

package main

import (
	"net/http"
	"net/url"
	"log"
	"encoding/json"
	"github.com/gorilla/mux"
)

type PTOServerConfiguration struct {
	BaseURL *url.URL
	RawRoot string
}

func (psc *PTOServerConfiguration) HandleRoot(w http.ResponseWriter, r *http.Request) {

	rawrel, _ := url.Parse("raw")

	links := map[string]string {
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

type RawDataStore struct {
	path string
} 

func (rds *RawDataStore) HandleListCampaigns(w http.ResponseWriter, r *http.Request) {
}

func (rds *RawDataStore) HandleCampaignMetadata(w http.ResponseWriter, r *http.Request) {
}

func (rds *RawDataStore) HandleFileMetadata(w http.ResponseWriter, r *http.Request) {
}

func (rds *RawDataStore) HandleFileData(w http.ResponseWriter, r *http.Request) {
}

func NewRawDataStore(path string) *RawDataStore {
	rds := RawDataStore{path: path}

	// TODO list campaign directories and files

	// TODO create metadata cache

	return &rds
}


func main() {
	// create a RawDataStore around the given RDS path 

	// now hook up routes 
	r := mux.NewRouter()

	psc := NewPTOServerConfiguration()

	r.HandleFunc("/",psc.HandleRoot)

	rds := NewRawDataStore(psc.RawRoot)

	r.HandleFunc("/raw", rds.HandleListCampaigns)
	r.HandleFunc("/raw/{campaign}", rds.HandleCampaignMetadata)
	r.HandleFunc("/raw/{campaign}/{file}", rds.HandleFileMetadata)
	r.HandleFunc("/raw/{campaign}/{file}/data", rds.HandleFileData)

	log.Fatal(http.ListenAndServe(":8000", r))
}