package pto3

import "fmt"

// AsStringArray tries to typeswitch an interface to a string or a string array
func AsStringArray(v interface{}) ([]string, bool) {
	switch cv := v.(type) {
	case string:
		return []string{cv}, true
	case []string:
		return cv, true
	default:
		return nil, false
	}
}

// AsString tries to typeswitch an interface to a string, printing its value if not.
func AsString(v interface{}) string {
	switch cv := v.(type) {
	case string:
		return cv
	default:
		return fmt.Sprintf("%v", v)
	}
}
