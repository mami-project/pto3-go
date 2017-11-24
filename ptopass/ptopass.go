// ptopass is a passthrough normalizer. It reads raw data as observation
// files, producing the same observations on standard out, passing through
// metadata. It is meant to allow the storage of preprocessed observations as
// raw data in the PTO, as well as to provide for self-contained testing of
// the local analysis runtime.

package main

func main() {
	// use os.NewFile(3, ".piped_metadata.json") in the analyzer to read
	// metadata, die if not present.
}
