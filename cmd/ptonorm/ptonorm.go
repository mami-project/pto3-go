// ptonorm is a command-line utility to run a specified normalizer with a
// specified raw data file, passing the observation data and metadata produced
// by the normalizer to standard output.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

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

func filterMetadata(from io.ReadCloser, to io.Writer, sourceurl string, errchan chan error, donechan chan struct{}) {
	defer from.Close()
	scanner := bufio.NewScanner(from)
	var lineno int
	md := make(map[string]interface{})

	for scanner.Scan() {
		lineno++
		line := strings.TrimSpace(scanner.Text())
		switch line[0] {
		case '{':
			// metadata. coalesce
			err := json.Unmarshal([]byte(line), &md)
			if err != nil {
				errchan <- err
				return
			}

		case '[':
			// data. pass.
			fmt.Fprintln(to, line)
		}
	}

	// At EOF. Add source URL to metadata and emit.
	md["_sources"] = []string{sourceurl}

	b, err := json.Marshal(md)
	if err != nil {
		errchan <- err
		return
	}
	fmt.Fprintf(to, "%s\n", b)
	errchan <- nil
	close(donechan)
}

func PtoNorm(config *pto3.PTOConfiguration, outfile io.Writer,
	normCmd string, campaign string, filename string) error {

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

	// observations on stdout
	obspipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	// pass through stderr
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
	obserr := make(chan error, 1)
	cmderr := make(chan error, 1)
	outdone := make(chan struct{})

	// get metadata
	b, err := md.DumpJSONObject(true)
	if err != nil {
		return err
	}

	// start a goroutine to fill the metadata pipe
	go copyData(bytes.NewReader(b), metapipe, metaerr)

	// start a goroutine to fill the data pipe
	go copyData(rawfile, datapipe, dataerr)

	// start a goroutine to filter metadata in output
	// and add a source URL
	sourceurl := fmt.Sprintf("%s%s/%s/%s", config.BaseURL, "raw", campaign, filename)
	go filterMetadata(obspipe, outfile, sourceurl, obserr, outdone)

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
		case err := <-obserr:
			if err != nil {
				return err
			}
		case err := <-cmderr:
			if err == nil {
				// wait on output completion
				<-outdone
				return nil
			} else {
				return err
			}
		}
	}
}

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "ptoconfig.json", "path to PTO configuration `file`")
var outFlag = flag.String("out", "", "path to output `file` [stdout if omitted]")

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

	var outfile *os.File
	if *outFlag == "" {
		outfile = os.Stdout
	} else {
		outfile, err = os.Create(*outFlag)
		if err != nil {
			log.Fatal(err)
		}
		defer outfile.Close()
	}

	args := flag.Args()

	if len(args) < 3 {
		flag.Usage()
		os.Exit(1)
	}

	if err := PtoNorm(config, outfile, args[0], args[1], args[2]); err != nil {
		log.Fatal(err)
	}

}
