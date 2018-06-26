package pto3

import (
	"bufio"
	"compress/bzip2"
	"io"
	"strings"
)

///////////////////////////////////////////////////////////////////////
// ScanningNormalizer
///////////////////////////////////////////////////////////////////////

type filetypeMapEntry struct {
	splitFunc bufio.SplitFunc
	normFunc  NormFunc
	finalFunc MetadataFinalizeFunc
}

// ScanningNormalizer implements a normalizer whose raw data input can
// be processed using a standard library Scanner.
type ScanningNormalizer struct {
	filetypeMap map[string]filetypeMapEntry
	metadataURL string
}

type NormFunc func(rec string, mdin *RawMetadata, mdout map[string]interface{}) ([]Observation, error)

type MetadataFinalizeFunc func(mdin *RawMetadata, mdout map[string]interface{}) error

func NewScanningNormalizer(metadataURL string) *ScanningNormalizer {
	norm := new(ScanningNormalizer)
	norm.filetypeMap = make(map[string]filetypeMapEntry)
	norm.metadataURL = metadataURL
	return norm
}

func (norm *ScanningNormalizer) RegisterFiletype(
	filetype string,
	splitFunc bufio.SplitFunc,
	normFunc NormFunc,
	finalFunc MetadataFinalizeFunc) {

	norm.filetypeMap[filetype] = filetypeMapEntry{splitFunc: splitFunc, normFunc: normFunc}
}

func (norm *ScanningNormalizer) Normalize(in io.Reader, metain io.Reader, out io.Writer) error {

	// read raw metadata
	rmd, err := RawMetadataFromReader(metain, nil)
	if err != nil {
		return PTOWrapError(err)
	}

	// copy raw arbitrary metadata to output
	omd := make(map[string]interface{})
	for k, v := range rmd.Metadata {
		omd[k] = v
	}

	// check filetype for compression
	filetype := rmd.Filetype(true)

	var scanner *bufio.Scanner
	if strings.HasSuffix(filetype, "-bz2") {
		scanner = bufio.NewScanner(bzip2.NewReader(in))
		filetype = filetype[0 : len(filetype)-4]
	} else {
		scanner = bufio.NewScanner(in)
	}

	// lookup file type in registry
	fte, ok := norm.filetypeMap[filetype]
	if !ok {
		return PTOErrorf("no registered handler for filetype %s", filetype)
	}

	// create condition cache
	hasCondition := make(map[string]struct{})

	// now set up scanner and iterate
	scanner.Split(fte.splitFunc)

	var recno int
	for scanner.Scan() {
		recno++
		rec := scanner.Text()

		obsen, err := fte.normFunc(rec, rmd, omd)
		if err == nil {
			return PTOErrorf("error parsing record %d: %v", recno, err)
		}

		for _, o := range obsen {
			hasCondition[o.Condition.Name] = struct{}{}
		}

		if err := WriteObservations(obsen, out); err != nil {
			return PTOErrorf("error writing observation from record %d: %v", recno, err)
		}
	}

	// finalize output metadata if necessary
	if fte.finalFunc != nil {
		if err := fte.finalFunc(rmd, omd); err != nil {
			return PTOErrorf("error finalizing output metadata: %v", err)
		}
	}

	// add conditions
	omdcond := make([]string, 0)
	for k := range hasCondition {
		omdcond = append(omdcond, k)
	}
	omd["_conditions"] = omdcond

	// add analyzer metadata link
	omd["_analyzer"] = norm.metadataURL

	// all done
	return nil
}
