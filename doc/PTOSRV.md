# Path Transparency Observatory API Server (ptosrv)

ptosrv is a stand-alone Web server implementing the Path Transparency Observatory API detailed in [the API documentation](API.md).

## Installation

### Install Prerequisites

`ptosrv` uses PostgreSQL for observation storage and query execution, so a
PostgreSQL database must be available for the PTO's use, with a user
configured to be able to create tables, whose credentials will appear in the
`ptoconfig.json` file below.

### Install the Server

```
$ go install github.com/mami-project/pto3-go/papi/ptosrv
```

### Configure and Start the Server

The following keys should appear the config file:

| Key               | Value                                                                             |
| ----------------- | --------------------------------------------------------------------------------- |
| `BindTo`          | Interface and port to bind HTTP server to e.g. `:8383`; default to `:80` or `:443`| 
| `CertificateFile` | Path to X.509 certificate: support HTTP only if not present                       |
| `PrivateKeyFile`  | Path to X.509 private key: support HTTP only if not present                       |
| `BaseURL`         | Base URL of PTO, used for link generation                                         |
| `AccessLogPath`   | Filename for access logging; log to stderr if missing or empty                    |
| `ContentTypes`    | Object mapping PTO `_file_type` values to MIME content types                      |
| `APIKeyFile`      | Filename of API key file for access control; see below for details                |
| `RawRoot`         | Filesystem root for raw data storage; disable `/raw` if missing or empty          |
| `ObsDatabase`     | Object configuring database connection as below; disable `/obs` if missing        |
| `QueryCacheRoot`  | Filesystem root for query cache; disable `/query` if missing or empty             |
| `PageLength`      | Number of items to show on a single page (see [API](API.md) for more on pagination) |
| `ImmediateQueryDelay` | Time to wait (in milliseconds) for fast queries before returning a `pending` state |
| `ConcurrentQueries` | Maximum number of queries to execute concurrently                               |

The ObsDatabase object should have the following keys:

| Key         | Value                                       |
| ----------- | ------------------------------------------- |
| `Addr`      | Hostname and port of PostgreSQL server      |
| `Database`  | Name of database on PostgreSQL server       |
| `User`      | Name of PostgreSQL role to use              |
| `Password`  | Password associated with role               |

The APIKeyFile is a JSON file mapping API key strings to an object mapping
permission strings to a boolean, true if the key has that permission, false
otherwise. The following permissions are used by ptosrv:

| Permission      | Description                                           |
| --------------- | ----------------------------------------------------- |
| `list_raw`      | List campaign URLs                                    |
| `read_raw:<c>`  | Read raw data and metadata for campaign *c*           |
| `write_raw:<c>` | Write raw data and metadata for campaign *c*          |
| `read_obs`      | List observations, read observation data and metadata |
| `write_obs`     | Write observation data and metadata                   |
| `submit_query`  | Submit queries                                        |
| `read_query`    | Read query data and metadata                          |
| `update_query`  | Update query metadata                                 |

The special API key `default` allows the assignment of permissions for
requests without an `Authorization: APIKEY` header.

## Invocation

```
$ ptosrv -config <path_to_config_file>
```

If no `-config` flag is given, ptosrv searches for `ptoconfig.json` in the
current working directory.

On first invocation, the `-initdb` flag can be used to create the tables,
functions, and operators used by the PTO in the PostgreSQL database. It is
safe to use `-initdb` even on an initialized database, since it only creates
tables if they do not already exist.