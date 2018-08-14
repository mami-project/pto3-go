export GOPATH=/home/pto/go
export GOROOT=/usr/local/go
export PATH=$PATH:$GOROOT/bin

# something wrong with the packages that subdirectory 

go get github.com/mami-project/pto3-go
go get github.com/mami-project/pto3-go/papi
go get github.com/mami-project/pto3-ecn
go get github.com/mami-project/pto3-ecn/ecn_qof_normalizer
go get github.com/mami-project/pto3-trace
go get github.com/mami-project/pto3-trace/cmd/pto3-trace

go install github.com/mami-project/pto3-go
go install github.com/mami-project/pto3-go/papi
go install github.com/mami-project/pto3-go/papi/ptosrv
go install github.com/mami-project/pto3-go/cmd/ptocat
go install github.com/mami-project/pto3-go/cmd/ptoload
go install github.com/mami-project/pto3-go/cmd/ptonorm
go install github.com/mami-project/pto3-go/cmd/ptopass
go install github.com/mami-project/pto3-ecn/ecn_normalizer
go install github.com/mami-project/pto3-ecn/ecn_stabilizer
go install github.com/mami-project/pto3-ecn/ecn_pathdep
go install github.com/mami-project/pto3-ecn/ecn_qof_normalizer
go install github.com/mami-project/pto3-ecn/normalize_pathspider
go generate github.com/mami-project/pto3-trace
go install github.com/mami-project/pto3-trace/cmd/pto3-trace
go install github.com/mami-project/pto3-trace/cmd/pto3-trace-mkmeta