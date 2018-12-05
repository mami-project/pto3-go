package pto3

import (
	"net/http"
	"strings"
	"sync"

	"github.com/go-pg/pg/orm"
)

type Condition struct {
	ID      int
	Name    string
	Feature string
	Aspect  string
}

func NewCondition(name string) *Condition {
	out := new(Condition)
	out.Name = name

	if firstDot := strings.Index(name, "."); firstDot > -1 {
		out.Feature = name[0:firstDot]
	}

	if lastDot := strings.LastIndex(name, "."); lastDot > -1 {
		out.Aspect = name[0:lastDot]
	}

	return out
}

func NewConditionWithID(id int, name string) *Condition {
	out := NewCondition(name)
	out.ID = id
	return out
}

// ConditionCache maps a condition name to a condition ID
type ConditionCache map[string]int

var ccMutex = &sync.Mutex{}

// FIXME consider replacing this with a condition cache everywhere
func (c *Condition) InsertOnce(db orm.DB) error {
	ccMutex.Lock()
	defer ccMutex.Unlock()

	if c.ID == 0 {
		_, err := db.Model(c).
			Column("id").
			Where("name=?name").
			Returning("id").
			SelectOrInsert()
		if err != nil {
			return PTOWrapError(err)
		}
	}
	return nil
}

// FIXME consider replacing this with a condition cache everywhere
func (c *Condition) SelectByID(db orm.DB) error {
	ccMutex.Lock()
	defer ccMutex.Unlock()

	return db.Select(c)
}

func (cache ConditionCache) IDFromName(name string) (int, bool) {
	ccMutex.Lock()
	defer ccMutex.Unlock()

	if id, ok := cache[name]; ok {
		return id, true
	}

	return 0, false
}

// FillConditionIDsInSet ensures all the conditions in a given observation set
// have valid IDs. It keeps the condition cache synchronized with the database
// if any new conditions have been added.
func (cache ConditionCache) FillConditionIDsInSet(db orm.DB, set *ObservationSet) error {
	ccMutex.Lock()
	defer ccMutex.Unlock()

	for _, c := range set.Conditions {
		if err := cache.setConditionID(db, &c); err != nil {
			return err
		}
	}

	return nil
}

func (cache ConditionCache) setConditionID(db orm.DB, c *Condition) error {
	// check for a cache hit
	id, ok := cache[c.Name]
	if ok {
		c.ID = id
		return nil
	}

	// check to see if we need to insert or retrieve from DB
	if c.ID == 0 {
		if err := c.InsertOnce(db); err != nil {
			return err
		}
	}

	// now cache
	cache[c.Name] = c.ID
	return nil
}

// SetConditionID ensures the given condition has a valid ID. It keeps the
// condition cache synchronized with the database if the condition is new.
func (cache ConditionCache) SetConditionID(db orm.DB, c *Condition) error {
	ccMutex.Lock()
	defer ccMutex.Unlock()

	return cache.setConditionID(db, c)
}

func (cache ConditionCache) Reload(db orm.DB) error {
	ccMutex.Lock()
	defer ccMutex.Unlock()

	return cache.reload(db)
}

func (cache ConditionCache) reload(db orm.DB) error {
	var conditions []Condition

	if err := db.Model(&conditions).Select(); err != nil {
		return PTOWrapError(err)
	}

	for _, c := range conditions {
		cache[c.Name] = c.ID
	}

	return nil
}

func (cache ConditionCache) ConditionsByName(db orm.DB, conditionName string) ([]Condition, error) {
	ccMutex.Lock()
	defer ccMutex.Unlock()

	var out []Condition

	if strings.HasSuffix(conditionName, ".*") {
		// Wildcard. Reload cache and find everything that matches.
		if err := cache.reload(db); err != nil {
			return nil, err
		}
		out = make([]Condition, 0)
		for cachedName := range cache {
			if strings.HasPrefix(cachedName, conditionName[:len(conditionName)-1]) {
				out = append(out, *NewConditionWithID(cache[cachedName], cachedName))
			}
		}
	} else {
		// No wildcard, just look up by name.
		if cache[conditionName] == 0 {
			if err := cache.reload(db); err != nil {
				return nil, err
			}
		}
		if cache[conditionName] == 0 {
			return nil, PTOErrorf("unknown condition %s", conditionName).StatusIs(http.StatusBadRequest)
		}
		out = make([]Condition, 1)
		out[0] = *NewConditionWithID(cache[conditionName], conditionName)
	}

	return out, nil
}

func (cache ConditionCache) Names() []string {
	ccMutex.Lock()
	defer ccMutex.Unlock()

	names := make([]string, len(cache))

	i := 0
	for k := range cache {
		names[i] = k
		i++
	}

	return names
}

// LoadConditionCache creates a new condition cache with all the conditions in a given database.
func LoadConditionCache(db orm.DB) (ConditionCache, error) {
	ccMutex.Lock()
	defer ccMutex.Unlock()

	cache := make(ConditionCache)

	if err := cache.reload(db); err != nil {
		return nil, err
	}

	return cache, nil

}
