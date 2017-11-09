# Local Analyzer Interface

write me

## Normalizer interface

raw data on stdin

metadata as a JSON object on file descriptor 3.

observation file on stdout, including metadata anywhere, last wins.

## Derived analyzer interface

observation file on stdout with multiple obsets, each obset preceded by its metadata

observation file on stdout with single obset, including metadata anywhere, last wins.

## Platforms

- `golang-1.x`: repository is a Go repo. will go get code, checkout appropriate
tag, go install anything with a main in a subdirectory, then run the
invocation command.
- `python-3.x`: repository contains a Python module. Will run setup.py in the
root in a virtualenv, then run the invocation command.

