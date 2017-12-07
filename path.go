package pto3

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/go-pg/pg/orm"
)

type Path struct {
	ID     int
	String string
}

// PathCache maps a path string to a path ID
type PathCache map[string]int

// CacheNewPaths takes a set of path names, and adds those not already
// appearing to the cache and the underlying database. It modifies the pathSet
// to contain only those paths added.
func (cache PathCache) CacheNewPaths(db orm.DB, pathSet map[string]struct{}) error {
	// first, reduce to paths not already in the cache
	for ps := range pathSet {
		if cache[ps] > 0 {
			delete(pathSet, ps)
		}
	}

	// allocate a range of IDs in the database
	var nv struct {
		Nextval int
	}

	if _, err := db.QueryOne(&nv, "SELECT nextval('paths_id_seq')"); err != nil {
		return err
	}
	pidseq := nv.Nextval

	if _, err := db.Exec("SELECT setval('paths_id_seq', ?)", pidseq+len(pathSet)); err != nil {
		return err
	}

	// now add entries to the path cache while streaming into the database
	streamerr := make(chan error, 1)
	dbpipe, pathpipe, err := os.Pipe()
	if err != nil {
		return err
	}
	defer dbpipe.Close()

	go func() {
		out := csv.NewWriter(pathpipe)
		defer pathpipe.Close()

		for pathstring := range pathSet {
			p := []string{fmt.Sprintf("%d", pidseq), pathstring}
			cache[pathstring] = pidseq

			if err := out.Write(p); err != nil {
				streamerr <- err
			}

			pidseq++
		}

		out.Flush()
		streamerr <- nil
	}()

	// copy from the goroutine to the database
	if _, err = db.CopyFrom(dbpipe, "COPY paths (id, string) FROM STDIN WITH CSV"); err != nil {
		return err
	}

	// wait for goroutine to complete and return its error
	return <-streamerr
}

func (p *Path) InsertOnce(db orm.DB) error {
	if p.ID == 0 {
		_, err := db.Model(p).
			Column("id").
			Where("string=?string").
			Returning("id").
			SelectOrInsert()
		if err != nil {
			return err
		}
	}
	return nil
}
