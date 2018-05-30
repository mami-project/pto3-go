# Writing Local Analyzers

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
local analyzer runtime should set up the analyzer environment before the first
time it runs the `_invocation` command.

- `golang-1.x`: repository is a Go repo. The analyzer runtime will create a
  new `GOPATH` for the `go` tool, `go get` the repository, check out the
  appropriate tag, scan subdirectories for executables (`*.go` files contiaing
  `package main`), and `go install` all such executables, before the first
  time it runs the `_invocation` command. Subsequent runs will occur in the
  same `GOPATH`.
- `python-3.x`: repository contains a Python module. The analyzer runtime will
  create an appropriate Python `virtualenv`, run `setup.py install`, before
  the first time it runs the `_invocation` command. Subsequent runs will occur
  in the same `virtualenv`.
- `bash`: repository contains a module in some other language. The analyzer
  runtime will source the `setup.sh` script using `bash` in the repository
  root before running the `_invocation` command.

A local analyzer runtime is not yet available for the PTO; use the
command-line tools described below to invoke local analyzers manually.

# Using the Local Analyzer Command-Line Tools

The PTO comes with a set of command-line tools for running normalizers and
analyzers locally (i.e., on the same machine running `ptosrv`, or on a machine
with equivalent access to the raw filesystem and the PostgreSQL database). 

Three tools are provided:

- `ptonorm`: read data and metadata from raw data store, hadling campaign
  metadata inheritance, run a normalizer, and pipe to stdin / fd 3.
- `ptocat`: dump observation sets from the database with metadata (in
  [Observation File Format](OBSETS.md)) to stdout
- `ptoload`: read files with observation set data and metadata (in [Observation File
  Format](OBSETS.md)) and insert resulting observation sets into database

These tools can be used for normalization and analysis workflows as descibed
below.

## Running Normalizers

Local normalizers are run by `ptonorm`, which takes the following command-line
arguments:

```
ptonorm -config <path/to/config.json> <normalizer> <campaign> <file>
```

If `-config` is not given, the file `ptoconfig.json` in the current working
directory is used.

`ptonorm` launches the normalizer as a subprocess, allowing access to the raw
data file over stdin, and streaming metadata over a pipe on file descriptor 3.
It then takes the standard output, coalescing all metadata into a single
object, and writes it to standard output. When coalescing metadata, the last
write on a given metadata key wins.

The resulting observation file can be passed as input to `ptoload`, which
takes the following command-line arguments:

```
ptoload -config <path/to/config.json> <obsfile>...
```

If `-config` is not given, the file `ptoconfig.json` in the current working
directory is used. More than one observation file can be given on a single
command line, but each file given will create a new observation set.

For example, to normalize the file `quux.ndjson` with the `bar` normalizer in
the `foo` campaign into an observation set, using a local configuration file,
and load it directly into the database, deleting the cached observation file:

```
ptonorm bar foo quux.json > cached.obs && ptoload cached.obs && rm cached.obs
```

## Running Analyzers

Analyzers are simpler to run, as they take observation files on standard input
and generate observation files on standard output. To get observation files,
use `ptocat`, which takes the following command line arguments:

```
ptocat -config <path/to/config.json> <set-id>...
```

If `-config` is not given, the file `ptoconfig.json` in the current working
directory is used. Set IDs are given in hexadecimal, as in the rest of the
PTO. More than one set ID may appear; in this case, the metadata for the first
set will be followed by the data for the first set followed by the metadata
for the second set followed by the data for the second set and so on.

For example, to analyze sets 3a70 through 3a75 using the analyer `fizz` and
load it directly into the database, deleting the cached observation file:

```
ptocat 3a70 3a71 3a72 3a73 3a74 3a75 > cached.obs && ptoload cached.obs && rm cached.obs
```

# Writing Client Normalizers and Analyzers

Client analyzers are simply clients of the PTO. A normalizer interacts with
raw data through `/raw` resources and creates new observation sets by posting
to `/obs/create`. An analyzer retrieves observation sets from `/obs/` and
likewise creates new observation sets by posting to `/obs/create`

# MAMI Project developed normalizers and analyzers

Tools for normalizing and analyzing [PATHspider](https://pathspider.net) output (originally focused on ECN, with future support for other plugins) are in the [pto3-ecn](/mami-project/pto3-ecn) repository.

Tools for dealing with Tracebox output (with future support for other traceroute-like tools and data sources) are in the [pto3-trace](/mami-project/pto3-trace) repository.