package pto3

import (
	"bufio"
	"compress/bzip2"
	"fmt"
	"io"
	"strings"
)

// ConditionSet tracks conditions seen in analysis output by name.
type ConditionSet map[string]struct{}

// AddCondition adds a condition (by name) to this set
func (cs ConditionSet) AddCondition(condition string) {
	cs[condition] = struct{}{}
}

// HasCondition returns true if a given condition is in the condition set
func (cs ConditionSet) HasCondition(condition string) bool {
	_, ok := cs[condition]
	return ok
}

// Conditions lists all conditions in the set as a string slice,
// suitable for output as normalizer/analyzer metadata
func (cd ConditionSet) Conditions() []string {
	out := make([]string, 0)
	for k := range cd {
		out = append(out, k)
	}
	return out
}

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
	hasCondition := make(ConditionSet)

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
	omd["_conditions"] = hasCondition.Conditions()

	// add analyzer metadata link
	omd["_analyzer"] = norm.metadataURL

	// all done
	return nil
}

///////////////////////////////////////////////////////////////////////
// Analysis Utilities
///////////////////////////////////////////////////////////////////////

// AnalysisSetTable tracks observation sets by ID
type AnalysisSetTable map[int]*ObservationSet

// AddSetFrom adds an observation set from a given observation
func (st AnalysisSetTable) AddSetFrom(obs *Observation) {
	if _, ok := st[obs.SetID]; obs.SetID != 0 && !ok {
		st[obs.SetID] = obs.Set
	}
}

// MergeMetadata creates merged output metadata from the set of incoming
// observation sets, tracking sources and including all metadata keys for
// which the value is the same in each set in the table.
func (st AnalysisSetTable) MergeMetadata() map[string]interface{} {
	mdout := make(map[string]interface{})

	sources := make([]string, 0)
	conflictingKeys := make(map[string]struct{})

	for setid := range st {

		// track sources
		source := st[setid].Link()
		if source != "" {
			sources = append(sources, source)
		}

		// inherit arbitrary metadata for all keys without conflict
		for k, newval := range st[setid].Metadata {
			if _, ok := conflictingKeys[k]; ok {
				continue
			} else {
				existval, ok := mdout[k]
				if !ok {
					mdout[k] = newval
				} else if fmt.Sprintf("%v", existval) != fmt.Sprintf("%v", newval) {
					delete(mdout, k)
					conflictingKeys[k] = struct{}{}
				}
			}
		}
	}

	if len(sources) > 0 {
		mdout["_sources"] = sources
	}

	return mdout
}

// AnalyzeObservationStream reads observation set metadata and data from a
// file (as created by ptocat) and calls a provided analysis function once per
// observation. It is a convenience function for writing PTO analyzers in Go.
// It returns a table mapping set IDs to observation sets,
// from which metadata can be derived.
func AnalyzeObservationStream(in io.Reader, afn func(obs *Observation) error) (AnalysisSetTable, error) {
	// stream in observation sets
	scanner := bufio.NewScanner(in)

	var lineno int
	var currentSet *ObservationSet
	var obs *Observation

	setTable := make(AnalysisSetTable)

	for scanner.Scan() {
		lineno++
		line := strings.TrimSpace(scanner.Text())
		switch line[0] {
		case '{':
			// New observation set; cache metadata
			currentSet = new(ObservationSet)
			if err := currentSet.UnmarshalJSON(scanner.Bytes()); err != nil {
				return nil, PTOErrorf("error parsing set on input line %d: %s", lineno, err.Error())
			}
		case '[':
			// New observation; call analysis function
			obs = new(Observation)
			if err := obs.UnmarshalJSON(scanner.Bytes()); err != nil {
				return nil, PTOErrorf("error parsing observation on input line %d: %s", lineno, err.Error())
			}

			if currentSet == nil {
				return nil, PTOErrorf("observation on input line %d without current set", lineno)
			} else if currentSet.ID == 0 {
				// new current set, cache by ID
				currentSet.ID = obs.SetID
				setTable.AddSetFrom(obs)
			} else if currentSet.ID != obs.SetID {
				var ok bool
				currentSet, ok = setTable[obs.SetID]
				if !ok {
					return nil, PTOErrorf("observation on input line %d refers to uncached set %x", lineno, obs.SetID)
				}
			}

			obs.Set = currentSet
			if err := afn(obs); err != nil {
				return nil, PTOWrapError(err)
			}
		}
	}

	return setTable, nil
}
