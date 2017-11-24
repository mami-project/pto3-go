// ptonorm is a command-line utility to run a specified normalizer with a
// specified raw data file, passing the observation data and metadata produced
// by the normalizer to standard output.

package main

func main() {
	// NOTES:
	// use os.Pipe to create a pipe, exec.Command.Extrafiles to pass one end as fd3,
	// then grab metadata from RDS and push it down the pipe.
}
