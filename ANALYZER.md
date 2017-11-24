# Local Analyzer Interface

Local analyzers are identified by the presence of an `_invocation` key in the
analyzer metadata. Local analyzers are either normalizers (consuming raw data,
types identified by the `_file_types` analyzer metadata key) or derived
analyzers (consuming observations, when no `_file_types` key is present).
Local analyzers are UNIX executables written in any language, though the PTO
provides special support for analyzers written in Go and in Python.

## Normalizer interface

Normalizers (raw data analyzers) take a single raw data file on standard input
and produce an [observation file](OBSETS.md) on standard output. Metadata for
the raw data file, including any metadata inherited from the campaign, is
passed in as a JSON object on file descriptor 3. 

## Derived analyzer interface

Derived analyzers take one or more observation sets in 
[observation file](OBSETS.md) format on standard input, and produce a single observation
file containing a single observation set on standard output. The observations on standard 
input are ordered by observation set, with each observation set preceded by its ; i.e., 
as multiple obset files concatenated together.

## Platforms

A local analyzer may optionally specify a platform, which determines how a
local analyzer runtime will set up the analyzer environment before the first
time it runs the `_invocation` command.

- `golang-1.x`: repository is a Go repo. The analyzer runtime will create a
  new `GOPATH` for the `go` tool, `go get` the repository, check out the
  appropriate tag, scan subdirectories for executables (`*.go` files contiaing
  `package main`), and `go install` all such executables, before the first
  time it runs the `_invocation` command. Subsequent runs will occur in the same `GOPATH`.
- `python-3.x`: repository contains a Python module. The analyzer runtime will
  create an appropriate Python `virtualenv`, run `setup.py install`, before
  the first time it runs the `_invocation` command. Subsequent runs will occur in the same `virtualenv`.
- `bash`: repository contains a module in some other language. The analyzer
  runtime will source the `setup.sh` script using `bash` in the repository
  root before running the `_invocation` command.

