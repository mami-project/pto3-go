package gopto

import "net/url"

// PTOServerConfig contains a configuration of a PTO server
type PTOServerConfig struct {
	// base URL of web service
	BaseURL url.URL
}
