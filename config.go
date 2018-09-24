package pto3

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/url"
	"os"

	"github.com/go-pg/pg"
)

// PTOConfiguration contains a configuration of a PTO server
type PTOConfiguration struct {
	// Address/port to bind to
	BindTo string

	// base URL of web service
	BaseURL string
	// ...this right here is effing annoying but i'm not writing a custom unmarshaler just for that...
	baseURL *url.URL

	// Access-Control-Allow-Origin header on responses
	AllowOrigin string

	// API key file path
	APIKeyFile string

	// Certificate file path
	CertificateFile string

	// Private key file path
	PrivateKeyFile string

	// File to serve for / (empty == serve paths to enabled apps)
	RootFile string

	// Directory to serve files from for /static
	StaticRoot string

	// base path for raw data store; empty for no RDS.
	RawRoot string

	// Filetype registry for RDS.
	ContentTypes map[string]string

	// base path for query cache data store; empty for no query cache.
	QueryCacheRoot string

	// PostgreSQL options for connection to observation database; leave default for no OBS.
	ObsDatabase pg.Options

	// Page size for things that can be paginated
	PageLength int

	// Immediate query delay
	ImmediateQueryDelay int

	// Number of concurrent queries
	ConcurrentQueries int

	// Access logging file path
	AccessLogPath string
	accessLogger  *log.Logger

	// Path to configuration file
	ConfigFilePath string
}

// LinkTo creates a link to a relative URL from the configuration's base URL
func (config *PTOConfiguration) LinkTo(relative string) (string, error) {
	u, err := url.Parse(relative)
	if err != nil {
		return "", PTOWrapError(err)
	}

	return config.baseURL.ResolveReference(u).String(), nil
}

// AccessLogger returns a logger for the web API to log accesses to
func (config *PTOConfiguration) AccessLogger() *log.Logger {
	return config.accessLogger
}

func NewConfigFromJSON(b []byte) (*PTOConfiguration, error) {
	var config PTOConfiguration
	var err error

	if err := json.Unmarshal(b, &config); err != nil {
		return nil, err
	}

	config.baseURL, err = url.Parse(config.BaseURL)
	if err != nil {
		return nil, err
	}

	if config.AccessLogPath == "" {
		config.accessLogger = log.New(os.Stderr, "", log.LstdFlags)
	} else {
		accessLogFile, err := os.Open(config.AccessLogPath)
		if err != nil {
			return nil, err
		}
		config.accessLogger = log.New(accessLogFile, "access: ", log.LstdFlags)
	}

	// default page length is 1000
	if config.PageLength == 0 {
		config.PageLength = 1000
	}

	// default immediate query delay is 2s
	if config.ImmediateQueryDelay == 0 {
		config.ImmediateQueryDelay = 2000
	}

	// default query concurrency is 8
	if config.ConcurrentQueries == 0 {
		config.ConcurrentQueries = 8
	}

	return &config, nil
}

func NewConfigFromFile(filename string) (*PTOConfiguration, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	out, err := NewConfigFromJSON(b)
	if err != nil {
		return nil, err
	}

	out.ConfigFilePath = filename

	return out, nil
}

var DefaultConfigPaths = []string{"ptoconfig.json", "~/.ptoconfig.json", "/etc/pto/ptoconfig.json"}

func NewConfigWithDefault(filename string) (*PTOConfiguration, error) {

	var configPaths []string

	if filename != "" {
		configPaths = make([]string, len(DefaultConfigPaths)+1)
		copy(configPaths[1:], DefaultConfigPaths)
		configPaths[0] = filename
	} else {
		configPaths = DefaultConfigPaths
	}

	for _, configpath := range configPaths {
		_, err := os.Stat(configpath)
		if err == nil {
			return NewConfigFromFile(configpath)
		} else if !os.IsNotExist(err) {
			return nil, PTOWrapError(err)
		}
	}

	return nil, PTOErrorf("no configuration file found in %v", configPaths)

}
