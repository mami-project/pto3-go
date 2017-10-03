package gopto

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
)

// PTOServerConfig contains a configuration of a PTO server
type PTOServerConfig struct {
	// base URL of web service
	BaseURL url.URL

	// base path for raw data store; empty for no RDS.
	RawRoot string

	// Filetype registry
	ContentTypes map[string]string
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
