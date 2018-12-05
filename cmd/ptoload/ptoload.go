// ptoload loads an observation file in the form produced by normalizers and
// derived analyzers into the database.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/go-pg/pg"
	pto3 "github.com/mami-project/pto3-go"
)

var helpFlag = flag.Bool("h", false, "display a help message")
var configFlag = flag.String("config", "", "path to PTO configuration `file` with DB connection information")
var initdbFlag = flag.Bool("initdb", false, "Create database tables on startup")
var nLoaders = flag.Int("nloaders", 1, "number of parallel loaders")

func namePusher(args []string, c chan string) {
	for _, a := range args {
		c <- a
	}
	close(c)
}

func progressMeter(n int, tick chan int) {
	progressed := 0

	for t := range tick {
		progressed++
		log.Printf("%d/%d (%5.2f%%) done, created observation set 0x%x",
			progressed, n, 100.0*float64(progressed)/float64(n), t)
	}
}

func loader(i int, names chan string, config *pto3.PTOConfiguration, db *pg.DB, cidCache pto3.ConditionCache, pidCache pto3.PathCache, tick chan int, loaderDone chan int) {
	for filename := range names {
		var set *pto3.ObservationSet
		set, err := pto3.CopySetFromObsFile(filename, db, cidCache, pidCache)
		if err != nil {
			log.Fatal("copying set from obs file: ", err)
		}

		set.LinkVia(config)
		tick <- set.ID
	}
	loaderDone <- i
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s: load observations from a file into a PTO database\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Usage: %s <flags> input-files\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if *helpFlag {
		flag.Usage()
		os.Exit(1)
	}

	args := flag.Args()

	if len(args) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	config, err := pto3.NewConfigWithDefault(*configFlag)
	if err != nil {
		log.Fatal(err)
	}

	db := pg.Connect(&config.ObsDatabase)
	if *initdbFlag {
		if err := pto3.CreateTables(db); err != nil {
			log.Fatal("creating database tables: ", err)
		}
	}

	// share pid and condition caches across all files
	cidCache, err := pto3.LoadConditionCache(db)
	if err != nil {
		log.Fatal("loading condition cache: ", err)
	}

	pidCache := make(pto3.PathCache)

	names := make(chan string)
	go namePusher(args, names)

	tick := make(chan int)
	go progressMeter(len(args), tick)

	loaderDone := make(chan int)
	for i := 0; i < *nLoaders; i++ {
		go loader(i, names, config, db, cidCache, pidCache, tick, loaderDone)
	}

	for i := 0; i < len(args); i++ {
		loaderID := <-loaderDone
		fmt.Printf("loader %d done\n", loaderID)
	}
}
