package pto3_test

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {

	// create temporary RDS directory

	// create configuration, RDS, and router (as package vars?)

	// go!
	rv := m.Run()

	// delete temporary RDS

	// we're done
	os.Exit(rv)

}

func TestFileRoundtrip(t *testing.T) {

}
