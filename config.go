package pto3

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/go-pg/pg"
)

// PTOServerConfig contains a configuration of a PTO server
type PTOServerConfig struct {
	// Address/port to bind to
	BindTo string

	// base URL of web service
	BaseURL url.URL `json:"BaseURL,string"`

	// API key filename
	APIKeyFile string

	// base path for raw data store; empty for no RDS.
	RawRoot string

	// Filetype registry for RDS.
	ContentTypes map[string]string

	// base path for query cache data store; empty for no query cache.
	//QueryCacheRoot string

	// PostgreSQL options for connection to observation database; leave default for no OBS.
	ObsDatabase pg.Options
}

func (config *PTOServerConfig) HandleRoot(w http.ResponseWriter, r *http.Request) {

	rawrel, _ := url.Parse("raw")

	links := map[string]string{
		"raw": config.BaseURL.ResolveReference(rawrel).String(),
	}

	linksj, err := json.Marshal(links)

	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(linksj)
}

func LoadConfig(filename string) (*PTOServerConfig, error) {
	var config PTOServerConfig

	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(b, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
