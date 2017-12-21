package pto3

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"
)

// AsString tries to typeswitch an interface to a string, printing its value if not.
func AsString(v interface{}) string {
	switch cv := v.(type) {
	case string:
		return cv
	default:
		return fmt.Sprintf("%v", v)
	}
}

// AsStringArray tries to typeswitch an interface to a string or a string array
func AsStringArray(v interface{}) ([]string, bool) {
	switch cv := v.(type) {
	case string:
		return []string{cv}, true
	case []string:
		return cv, true
	case []interface{}:
		out := make([]string, len(cv))
		for i, iv := range cv {
			out[i] = AsString(iv)
		}
		return out, true
	default:
		log.Printf("tried to call AsStringArray on %v of type %T", cv, cv)
		return nil, false
	}
}

// ParseTimeString takes a string and attempts to parse it as an ISO, PostgreSQL, or Unix epoch second string
func ParseTime(s string) (time.Time, error) {
	var t time.Time
	var err error

	// ISO
	t, err = time.Parse(time.RFC3339, s)
	if err == nil {
		return t, nil
	}

	// ISO Date
	t, err = time.Parse("2006-01-02", s)
	if err == nil {
		return t, nil
	}

	// PostgreSQL
	t, err = time.Parse("2006-01-02 15:04:05-07", s)
	if err == nil {
		return t, nil
	}

	// epoch seconds
	t64, err := strconv.ParseFloat(s, 64)
	if err == nil {
		s, ns := math.Modf(t64)
		return time.Unix(int64(s), int64(ns*1e9)), nil
	}

	return time.Time{}, PTOErrorf("%s not parseable as time", s).StatusIs(http.StatusBadRequest)
}

// AsTime tries to typeswitch an interface to a time.Time.
func AsTime(v interface{}) (time.Time, error) {
	switch cv := v.(type) {
	case time.Time:
		return cv, nil
	case string:
		return ParseTime(cv)
	case int64:
		return time.Unix(cv, 0), nil
	case int:
		return time.Unix(int64(cv), 0), nil
	default:
		return ParseTime(AsString(cv))
	}
}

func AsInt(v interface{}) int {
	switch cv := v.(type) {
	case int:
		return cv
	case int64:
		return int(cv)
	case uint64:
		return int(cv)
	case int32:
		return int(cv)
	case uint32:
		return int(cv)
	case int16:
		return int(cv)
	case uint16:
		return int(cv)
	case int8:
		return int(cv)
	case uint8:
		return int(cv)
	default:
		i, _ := strconv.ParseInt(AsString(cv), 10, 64)
		return int(i)
	}
}
