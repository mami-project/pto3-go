package pto3_test

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"testing"

	"github.com/go-pg/pg"
	"github.com/mami-project/pto3-go"
	"golang.org/x/crypto/sha3"
)

const SuppressDropTables = false
const SuppressDeleteRawStore = false
const SuppressDeleteQueryCache = false

var TestConfig *pto3.PTOConfiguration
var TestRDS *pto3.RawDataStore
var TestDB *pg.DB
var TestQueryCache *pto3.QueryCache
var TestQueryCacheSetID int
var TestRC int

func setupRDS(config *pto3.PTOConfiguration) *pto3.RawDataStore {
	// create temporary RDS directory
	var err error
	config.RawRoot, err = ioutil.TempDir("", "pto3-test-rds")
	if err != nil {
		log.Fatal(err)
	}

	// prepopulate it with a test campaign to ensure scanning on startup works
	// FIXME this is UNIX only at the moment
	cp := exec.Command("cp", "-a", "testdata/test_raw_init/test0", config.RawRoot)
	if err := cp.Run(); err != nil {
		log.Fatal(err)
	}

	// create an RDS
	rds, err := pto3.NewRawDataStore(config)
	if err != nil {
		log.Fatal(err)
	}

	return rds
}

func teardownRDS(config *pto3.PTOConfiguration) {
	if SuppressDeleteRawStore {
		log.Printf("Leaving temporary raw data store at %s", config.RawRoot)
	} else if err := os.RemoveAll(config.RawRoot); err != nil {
		log.Fatal(err)
	}
}

func setupDB(config *pto3.PTOConfiguration) *pg.DB {
	// create a DB connection
	db := pg.Connect(&config.ObsDatabase)

	// log everything
	pto3.EnableQueryLogging(db)

	// create tables
	if err := pto3.CreateTables(db); err != nil {
		log.Fatal(err)
	}

	return db
}

func teardownDB(db *pg.DB) {
	// drop tables
	if !SuppressDropTables {
		if err := pto3.DropTables(db); err != nil {
			log.Fatal(err)
		}
	}
}

func setupQC(config *pto3.PTOConfiguration) *pto3.QueryCache {
	// create temporary query cache directory
	var err error
	config.QueryCacheRoot, err = ioutil.TempDir("", "pto3-test-qc")
	if err != nil {
		log.Fatal(err)
	}

	// create a query cache
	qc, err := pto3.NewQueryCache(config)
	if err != nil {
		log.Fatal(err)
	}

	// ensure the query test data is loaded and stash its set ID
	TestQueryCacheSetID, err = qc.LoadTestData("testdata/test_query.ndjson")
	if err != nil {
		log.Fatal(err)
	}

	return qc
}

func teardownQC(config *pto3.PTOConfiguration) {
	if SuppressDeleteQueryCache {
		log.Printf("Leaving temporary query cache at %s", config.QueryCacheRoot)
	} else if err := os.RemoveAll(config.QueryCacheRoot); err != nil {
		log.Fatal(err)
	}
}

// compare data files returned by ReadClosers by hashing them
func compareData(afn, bfn func() (io.ReadCloser, error)) error {

	af, err := afn()
	if err != nil {
		return err
	}
	defer af.Close()

	ab, err := ioutil.ReadAll(af)
	if err != nil {
		return err
	}

	ah := make([]byte, 64)
	sha3.ShakeSum256(ah, ab)

	bf, err := bfn()
	if err != nil {
		return err
	}
	defer bf.Close()

	bb, err := ioutil.ReadAll(bf)
	if err != nil {
		return err
	}

	bh := make([]byte, 64)
	sha3.ShakeSum256(bh, bb)

	if fmt.Sprintf("%x", ah) != fmt.Sprintf("%x", bh) {
		return errors.New("file hashes do not match")
	}

	return nil
}

func readPassword() (string, error) {
	file, err := os.Open("pto_main_password.txt")
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return "", scanner.Err()
	}

	return scanner.Text(), nil
}

func TestMain(m *testing.M) {
	// define a configuration
	testConfigJSON := []byte(`
{ 	
	"BaseURL" : "https://ptotest.mami-project.eu",
	"ContentTypes" : {
		"test" : "application/json",
		"osf" :  "applicaton/vnd.mami.ndjson"
	},
	"ObsDatabase" : {
		"Addr":     "localhost:5432",
		"User":     "ptotest",
		"Database": "ptotest"
	}
}`)

	var err error
	TestConfig, err = pto3.NewConfigFromJSON(testConfigJSON)
	if err != nil {
		log.Fatal(err)
	}

	// Original password was "helpful guide sheep train"
	TestConfig.ObsDatabase.Password, err = readPassword()
	if err != nil {
		log.Fatal(err)
	}

	// inner anon function ensures that os.Exit doesn't keep deferred teardown from running
	os.Exit(func() int {

		// build a raw data store (and prepare to clean up after it)
		TestRDS = setupRDS(TestConfig)
		defer teardownRDS(TestConfig)

		// build an observation store
		TestDB = setupDB(TestConfig)
		defer teardownDB(TestDB)

		// build a query cache (and prepare to clean up after it)
		TestQueryCache = setupQC(TestConfig)
		defer teardownQC(TestConfig)

		// run tests
		TestRC = m.Run()
		return TestRC
	}())
}
