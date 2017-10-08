package pto3

import (
	"net/http"
	"time"

	"github.com/go-pg/pg"
	"github.com/gorilla/mux"
)

type ObservationSet struct {
	Id       int64
	Sources  []string
	Analyzer string
}

type Observation struct {
	Id        int64
	Set       *ObservationSet
	Start     time.Time
	End       time.Time
	Path      string
	Condition string
	Value     int
}

type ObservationStore struct {
	config *PTOServerConfig
	azr    *Authorizer
	db     *pg.DB
}

func NewObservationStore(config *PTOServerConfig, azr *Authorizer) (*ObservationStore, error) {
	obs := ObservationStore{config: config, azr: azr}

	// Connect to database
	obs.db = pg.Connect(&config.ObsDatabase)

	return &obs, nil
}

func (obs *ObservationStore) initDB() error {
	err := obs.db.CreateTable(&ObservationSet{}, nil)
	if err != nil {
		return err
	}

	err = obs.db.CreateTable(&Observation{}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (obs *ObservationStore) HandleListSets(w http.ResponseWriter, r *http.Request) {}

func (obs *ObservationStore) HandleCreateSet(w http.ResponseWriter, r *http.Request)   {}
func (obs *ObservationStore) HandleGetMetadata(w http.ResponseWriter, r *http.Request) {}
func (obs *ObservationStore) HandlePutMetadata(w http.ResponseWriter, r *http.Request) {}
func (obs *ObservationStore) HandleUpload(w http.ResponseWriter, r *http.Request)      {}
func (obs *ObservationStore) HandleDownload(w http.ResponseWriter, r *http.Request)    {}

func (obs *ObservationStore) AddRoutes(r *mux.Router) {
	r.HandleFunc("/obs", obs.HandleListSets).Methods("GET")
	r.HandleFunc("/obs/create", obs.HandleCreateSet).Methods("POST")
	r.HandleFunc("/obs/{set}", obs.HandleGetMetadata).Methods("GET")
	r.HandleFunc("/obs/{set}", obs.HandlePutMetadata).Methods("PUT")
	r.HandleFunc("/obs/{set}/data", obs.HandleDownload).Methods("GET")
	r.HandleFunc("/obs/{set}/data", obs.HandleUpload).Methods("PUT")
}
