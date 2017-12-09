# pto3-go

MAMI Path Transparency Observatory (PTO) version 3 [API](doc/API.md) implementation in golang.

Backed by filesystem storage (for raw data) and PostgreSQL (for observation storage and query).

To install: 

```
$ go install github.com/mami-project/pto3-go/papi/ptosrv
```

To run, create a `ptoconfig.json` file and invoke

```
$ ptosrv -config <path_to_config_file>
```

The following keys should appear the config file:

| Key             | Value                                                                    |
| --------------- | ------------------------------------------------------------------------ |
| `BindTo`        | Interface and port to bind HTTP server to e.g. `:8383`                   |
| `AccessLogPath` | Filename for access logging; log to stderr if missing or empty           |
| `BaseURL`       | Base URL for PTO for link generation                                     |
| `APIKeyFile`    | Filename of API key file; see below for format                           |
| `RawRoot`       | Filesystem root for raw data storage; disable `/raw` if missing or empty |
| `ContentTypes`  | Object mapping PTO filetype names to MIME content types                  |
| `ObsDatabase`   | Object configuring database connection as below; disable `/obs` if missing |

The ObsDatabase object should have the following keys:

| Key         | Value                                       |
| ----------- | ------------------------------------------- |
| `Addr`      | Hostname and port of PostgreSQL server      |
| `Database`  | Name of database on PostgreSQL server       |
| `User`      | Name of PostgreSQL role to use              |
| `Password`  | Password associated with role               |

The APIKeyFile is a JSON file mapping API key strings to an object mapping
permission strings to a boolean, true if the key has that permission, false
otherwise. 

## Local Analyzer Utilities

This package also contains command-line utilities for 