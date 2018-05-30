# pto3-go

MAMI Path Transparency Observatory (PTO) version 3 [API](doc/API.md) implementation in Go, backed by filesystem storage (for raw data) and PostgreSQL (for observation storage and query).

Installation and configuration instructions are available in [PTOSRV.md](doc/PTOSRV.md).

Once the service is running, interact with it via its API. API documentation is [here](doc/API.md). There is also a (not yet well-documented) Python client for retrieving observations and queries 

Writing and running local analyzers and normalizers is covered in [ANALYZER.md](doc/ANALYZER.md)

[INFOMODEL.md](doc/INFOMODEL.md) and [OBSET.md](doc/OBSET.md) cover the information model and observation set file format used in the API, respectively.
