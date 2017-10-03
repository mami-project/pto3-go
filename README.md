# pto3-go

MAMI Path Transparency Observatory (PTO) version 3 [API](API.md) implementation in golang.

Backed by filesystem storage (for raw data) and PostgreSQL (for observation storage and query).

To install in `GOROOT: 

```
$ go install ptosrv
```

To run, create a `ptoconfig.json` file and invoke

```
$ ptosrv
```

The following keys should appear the config file:

| Key             | Value                                              |
| --------------- | -------------------------------------------------- |
| `BaseURL`       | Base URL for PTO, for link generation              |
| `RawRoot`       | Filesystem root for raw data storage; disable `/raw` if missing or empty |
| `ContentTypes`  | Object mapping PTO filetype names to MIME content types |



