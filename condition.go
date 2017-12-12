package pto3

import "github.com/go-pg/pg/orm"

type Condition struct {
	ID   int
	Name string
}

// FIXME we probably should replace this with a condition cache everywhere
func (c *Condition) InsertOnce(db orm.DB) error {
	if c.ID == 0 {
		_, err := db.Model(c).
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

// FIXME we probably should replace this with a condition cache everywhere
func (c *Condition) SelectByID(db orm.DB) error {
	return db.Select(c)
}

// ConditionCache maps a condition name to a condition ID
type ConditionCache map[string]int

// FillConditionIDsInSet ensures all the conditions in a given observation set
// have valid IDs. It keeps the condition cache synchronized with the database
// if any new conditions have been added.
func (cache ConditionCache) FillConditionIDsInSet(db orm.DB, set *ObservationSet) error {

	for _, c := range set.Conditions {
		if err := cache.SetConditionID(db, &c); err != nil {
			return err
		}
	}

	return nil
}

// SetConditionID ensures the given condition has a valid ID. It keeps the
// condition cache synchronized with the database if the condition is new.
func (cache ConditionCache) SetConditionID(db orm.DB, c *Condition) error {
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

// LoadConditionCache creates a new condition cache with all the conditions in a given database.
func LoadConditionCache(db orm.DB) (ConditionCache, error) {

	cache := make(ConditionCache)
	var conditions []Condition

	if err := db.Model(&conditions).Select(); err != nil {
		return nil, err
	}

	for _, c := range conditions {
		cache[c.Name] = c.ID
	}

	return cache, nil
}
