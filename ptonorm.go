package pto3

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

func normalizerMetadataCopy(from io.Reader, to io.WriteCloser, errchan chan error) {
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

func normalizerMetadataFilter(from io.ReadCloser, to io.Writer, sourceurl string, errchan chan error, donechan chan struct{}) {
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

func RunNormalizer(config *PTOConfiguration, outfile io.Writer,
	normCmd string, campaign string, filename string) error {

	// create a raw data store (no need for an authorizer)
	rds, err := NewRawDataStore(config)
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

	// direct access to datafile
	cmd.Stdin = rawfile

	// metadata-filtered observations on stdout
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
	obserr := make(chan error, 1)
	outdone := make(chan struct{})

	// get metadata
	b, err := md.DumpJSONObject(true)
	if err != nil {
		return err
	}

	// start a goroutine to fill the metadata pipe
	go normalizerMetadataCopy(bytes.NewReader(b), metapipe, metaerr)

	// start a goroutine to fill the data pipe
	// go copyData(rawfile, datapipe, dataerr)

	// start a goroutine to filter metadata in output
	// and add a source URL
	var sourceurl string

	// make sure there's a slash between base and rest of source URL
	if len(config.BaseURL) == 0 || config.BaseURL[len(config.BaseURL)-1:] != "/" {
		sourceurl = fmt.Sprintf("%s/%s/%s/%s", config.BaseURL, "raw", campaign, filename)
	} else {
		sourceurl = fmt.Sprintf("%s%s/%s/%s", config.BaseURL, "raw", campaign, filename)
	}

	go normalizerMetadataFilter(obspipe, outfile, sourceurl, obserr, outdone)

	// now wait on the exit channels, return as soon as command completes
	for {
		select {
		// case err := <-dataerr:
		// 	if err != nil {
		// 		return err
		// 	}
		case err := <-metaerr:
			if err != nil {
				return err
			}
		case err := <-obserr:
			if err != nil {
				return err
			}
		case <-outdone:
			// This should not block because outdone is only ready
			// when the command has already finished
			err = cmd.Wait()
			if err != nil {
				return err
			}
			return nil
		}
	}
}
