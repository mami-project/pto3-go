package pgsandbox_test

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/go-pg/pg/orm"

	"github.com/go-pg/pg"
)

// Playground to figure out how go-pg is supposed to work with the observation model

const ISO8601Format = "2006-01-02T15:04:05"

type ObservationSet struct {
	ID       int
	Sources  []string `pg:",array"`
	Analyzer string
	Metadata map[string]string
}

type Condition struct {
	ID   int
	Name string
}

func (c *Condition) Ensure(db orm.DB) error {
	if c.ID == 0 {
		_, err := TestDB.Model(c).
			Column("id").
			Where("name=?name").
			Returning("id").
			SelectOrInsert()
		if err != nil {
			return err
		}
	}
	return nil
}

type Path struct {
	ID     int
	String string
}

func (p *Path) Ensure(db orm.DB) error {
	if p.ID == 0 {
		_, err := TestDB.Model(p).
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

type Observation struct {
	ID          int
	SetID       int
	Set         *ObservationSet
	Start       time.Time
	End         time.Time
	PathID      int
	Path        *Path
	ConditionID int
	Condition   *Condition
	Value       int
}

func createTables(db *pg.DB) error {

	opts := orm.CreateTableOptions{
		IfNotExists:   true,
		FKConstraints: true,
	}

	return db.RunInTransaction(func(tx *pg.Tx) error {
		err := db.CreateTable(&Condition{}, &opts)
		if err != nil {
			return err
		}

		err = db.CreateTable(&Path{}, &opts)
		if err != nil {
			return err
		}

		err = db.CreateTable(&ObservationSet{}, &opts)
		if err != nil {
			return err
		}

		err = db.CreateTable(&Observation{}, &opts)
		if err != nil {
			return err
		}

		return nil
	})
}

func dropTables(db *pg.DB) error {
	var err error

	err = db.DropTable(&Observation{}, nil)
	if err != nil {
		return err
	}

	err = db.DropTable(&ObservationSet{}, nil)
	if err != nil {
		return err
	}

	err = db.DropTable(&Condition{}, nil)
	if err != nil {
		return err
	}

	err = db.DropTable(&Path{}, nil)
	if err != nil {
		return err
	}

	return nil
}

var TestDB *pg.DB

func TestMain(m *testing.M) {

	opts := pg.Options{
		Addr:     "localhost:5432",
		User:     "ptotest",
		Database: "ptotest",
		Password: "helpful guide sheep train",
	}

	// connect to database
	TestDB = pg.Connect(&opts)

	// log everything that happens
	TestDB.OnQueryProcessed(func(event *pg.QueryProcessedEvent) {
		query, err := event.FormattedQuery()
		if err != nil {
			panic(err)
		}

		log.Printf("%s %s", time.Since(event.StartTime), query)
	})

	// now let's make us some tables
	err := createTables(TestDB)
	if err != nil {
		log.Fatal(err.Error())
	}

	// go!
	os.Exit(m.Run())

	// drop tables
	err = dropTables(TestDB)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func TestCreateObservationSet(t *testing.T) {
	// create an observation set with a single observation
	c := Condition{Name: "pto.test.works"}
	if err := c.Ensure(TestDB); err != nil {
		t.Fatal(err)
	}
	log.Printf("condition id is %d", c.ID)

	p := Path{String: "[1.2.3.4, *, 5.6.7.8]"}
	if err := p.Ensure(TestDB); err != nil {
		t.Fatal(err)
	}
	log.Printf("path id is %d", p.ID)

	s := ObservationSet{
		Sources:  []string{"http://ptotest.mami-project.org/raw/test/test001.json"},
		Analyzer: "http://ptotest.mami-project.org/",
		Metadata: map[string]string{
			"description": "a very simple test observation set",
		},
	}
	if err := TestDB.Insert(&s); err != nil {
		t.Fatal(err)
	}

	t0, _ := time.Parse(ISO8601Format, "2009-02-20T13:00:10")
	t1, _ := time.Parse(ISO8601Format, "2009-02-20T13:00:45")

	o := Observation{
		Set:         &s,
		SetID:       s.ID,
		Start:       t0,
		End:         t1,
		Path:        &p,
		PathID:      p.ID,
		Condition:   &c,
		ConditionID: c.ID,
	}

	if err := TestDB.Insert(&o); err != nil {
		t.Fatal(err)
	}
	t.Log("created observation id %d in set id %d", o.ID, o.SetID)
}
