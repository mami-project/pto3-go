// ptonorm is a command-line utility to run a specified normalizer with a
// specified raw data file, passing the observation data and metadata produced
// by the normalizer to standard output.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"

	pto3 "github.com/mami-project/pto3-go"
)

func copyData(from io.Reader, to io.WriteCloser, errchan chan error) {
	defer to.Close()
	buf := make([]byte, 65536)
	for {
		n, err := from.Read(buf)
		if err == nil {
			_, err2 := to.Write(buf[0:n])
			if err2 != nil {
				errchan <- err2
				log.Fatal(err)
				return
			}
		} else if err == io.EOF {
			break
		} else {
			log.Fatal(err)
			errchan <- err
			return
		}
	}
	errchan <- nil
}

func PtoNorm(config *pto3.PTOServerConfig, normCmd string, campaign string, filename string) error {

	// create a raw data store (no need for an authorizer)
	rds, err := pto3.NewRawDataStore(config, &pto3.NullAuthorizer{})
	if err != nil {
		return err
	}

	// retrieve the campaign
	cam, err := rds.CampaignForName(campaign)
	if err != nil {
		return err
	}

	// get metadata for the file
	md, err := cam.GetFileMetadata(filename)
	if err != nil {
		return err
	}

	// open raw data file
	rawfile, err := cam.ReadFileData(filename)
	if err != nil {
		return err
	}

	// create subprocess and pipes
	cmd := exec.Command(normCmd)

	// data file on stdtin
	datapipe, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	// pass through stdout and stderr
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// metadata on fd 3
	metapipeCmd, metapipe, err := os.Pipe()
	if err != nil {
		return err
	}

	cmd.ExtraFiles = make([]*os.File, 1)
	cmd.ExtraFiles[0] = metapipeCmd

	// start the command
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
		return err
	}

	metaerr := make(chan error, 1)
	dataerr := make(chan error, 1)
	cmderr := make(chan error, 1)

	// start a goroutine to fill the metadata pipe
	b, err := md.DumpJSONObject(true)
	if err != nil {
		return err
	}

	// start a goroutine to fill the metadata pipe
	go copyData(bytes.NewReader(b), metapipe, metaerr)

	// start a goroutine to fill the data pipe
	go copyData(rawfile, datapipe, dataerr)

	// start a goroutine to wait for the process to finish
	go func() {
		cmderr <- cmd.Wait()
	}()

	// now wait on the exit channels, return as soon as command completes
	for {
		select {
		case err := <-dataerr:
			if err != nil {
				return err
			}
		case err := <-metaerr:
			if err != nil {
				return err
			}
		case err := <-cmderr:
			return err
		}
	}
}

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "ptoconfig.json", "path to PTO configuration `file` with raw data store information")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: run a normalizer over a given raw data file\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s <flags> normalizer-command campaign filename\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if *helpFlag {
		flag.Usage()
		os.Exit(1)
	}

	config, err := pto3.NewConfigFromFile(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	args := flag.Args()

	if len(args) < 3 {
		flag.Usage()
		os.Exit(1)
	}

	if err := PtoNorm(config, args[0], args[1], args[2]); err != nil {
		log.Fatal(err)
	}

}
