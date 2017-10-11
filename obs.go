package pto3

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/go-pg/pg"
	"github.com/gorilla/mux"
)

const ISO8601Format = "2006-01-02T15:04:05"

type ObservationSet struct {
	Id       int64
	Sources  []string
	Analyzer string
	Metadata map[string]string
}

func (set *ObservationSet) MarshalJSON() ([]byte, error) {
	jmap := make(map[string]interface{})

	jmap["_sources"] = set.Sources
	jmap["_analyzer"] = set.Analyzer

	for k, v := range set.Metadata {
		jmap[k] = v
	}

	return json.Marshal(jmap)
}

func (set *ObservationSet) UnmarshalJSON(b []byte) error {
	set.Metadata = make(map[string]string)

	var jmap map[string]interface{}
	err := json.Unmarshal(b, &jmap)
	if err != nil {
		return err
	}

	var ok bool
	for k, v := range jmap {
		switch k {
		case "_sources":
			set.Sources, ok = AsStringArray(v)
			if !ok {
				return errors.New("_sources not a string array")
			}
		case "_analyzer":
			set.Analyzer = AsString(v)
		default:
			set.Metadata[k] = AsString(v)
		}
	}

	// make sure we got values for everything
	if set.Sources == nil {
		return errors.New("ObservationSet missing _sources")
	}

	if set.Analyzer == "" {
		return errors.New("ObservationSet missing _analyzer")
	}

	return nil
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

func (obs *Observation) MarshalJSON() ([]byte, error) {
	jslice := []string{
		fmt.Sprintf("%d", obs.Set.Id),
		obs.Start.Format(ISO8601Format),
		obs.End.Format(ISO8601Format),
		obs.Path,
		obs.Condition,
	}

	if obs.Value != 0 {
		jslice = append(jslice, strconv.Itoa(obs.Value))
	}

	return json.Marshal(&jslice)
}

func (obs *Observation) UnmarshalJSON(b []byte) error {
	var jslice []string

	err := json.Unmarshal(b, &jslice)
	if err != nil {
		return err
	}

	if len(jslice) < 5 {
		return errors.New("Observation requires at least five elements")
	}

	obs.Id = 0
	obs.Start, err = time.Parse(ISO8601Format, jslice[1])
	if err != nil {
		return err
	}
	obs.End, err = time.Parse(ISO8601Format, jslice[2])
	if err != nil {
		return err
	}
	obs.Path = jslice[3]
	obs.Condition = jslice[4]

	if len(jslice) >= 6 {
		obs.Value, err = strconv.Atoi(jslice[5])
		if err != nil {
			return err
		}
	}

	return nil
}

type ObservationStore struct {
	config *PTOServerConfig
	azr    *Authorizer
	db     *pg.DB
}

func NewObservationStore(config *PTOServerConfig, azr *Authorizer) (*ObservationStore, error) {
	osr := ObservationStore{config: config, azr: azr}

	// Connect to database
	osr.db = pg.Connect(&config.ObsDatabase)

	return &osr, nil
}

func (osr *ObservationStore) initDB() error {
	err := osr.db.CreateTable(&ObservationSet{}, nil)
	if err != nil {
		return err
	}

	err = osr.db.CreateTable(&Observation{}, nil)
	if err != nil {
		return err
	}

	return nil
}

func (osr *ObservationStore) HandleListSets(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

func (osr *ObservationStore) HandleCreateSet(w http.ResponseWriter, r *http.Request) {
	// fail if not authorized
	if !osr.azr.IsAuthorized(w, r, "write_obs") {
		return
	}

	// fail if not JSON
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, fmt.Sprintf("Content-type for metadata must be application/json; got %s instead",
			r.Header.Get("Content-Type")), http.StatusUnsupportedMediaType)
		return
	}

	// fill in an observation set from supplied metadata
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var set ObservationSet
	err = json.Unmarshal(b, &set)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

func (osr *ObservationStore) HandleGetMetadata(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

func (osr *ObservationStore) HandlePutMetadata(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

func (osr *ObservationStore) HandleUpload(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

func (osr *ObservationStore) HandleDownload(w http.ResponseWriter, r *http.Request) {
	// FIXME insert into database, get ID, form URL for the newly created observation set
	http.Error(w, "not done learning go-pg yet", http.StatusNotImplemented)
}

func (osr *ObservationStore) AddRoutes(r *mux.Router) {
	r.HandleFunc("/obs", osr.HandleListSets).Methods("GET")
	r.HandleFunc("/obs/create", osr.HandleCreateSet).Methods("POST")
	r.HandleFunc("/obs/{set}", osr.HandleGetMetadata).Methods("GET")
	r.HandleFunc("/obs/{set}", osr.HandlePutMetadata).Methods("PUT")
	r.HandleFunc("/obs/{set}/data", osr.HandleDownload).Methods("GET")
	r.HandleFunc("/obs/{set}/data", osr.HandleUpload).Methods("PUT")
}
