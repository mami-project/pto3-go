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

// SetConditionIDsInSet ensures all the conditions in a given observation set
// have valid IDs. It keeps the condition cache synchronized with the database
// if any new conditions have been added.
func (cache *ConditionCache) SetConditionIDsInSet(db orm.DB, set *ObservationSet) error {
	cidCache := make(map[string]int)

	for _, c := range set.Conditions {
		if err := c.InsertOnce(db); err != nil {
			return nil, err
		}

		cidCache[c.Name] = c.ID
	}

	return cidCache, nil
}

// SetConditionID ensures the given condition has a valid ID. It keeps the
// condition cache synchronized with the database if the condition is new.
func (cache *ConditionCache) SetConditionID(db orm.DB, c *Condition) error {
	cidCache := make(map[string]int)

	for _, c := range set.Conditions {
		if err := c.InsertOnce(db); err != nil {
			return nil, err
		}

		cidCache[c.Name] = c.ID
	}

	return cidCache, nil
}

// LoadConditionCache creates a new condition cache with all the conditions in a given database.
func LoadConditionCache(db orm.DB) (*ConditionCache, error) {
	// FIXME write me
	panic("LoadConditionCache not yet implemented.")
}
