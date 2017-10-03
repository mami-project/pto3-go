// Path Transparency Observatory JWT-based authorization

package pto3

import "net/http"

// For now, all capabilities are authorized.
// By deployment, this will check a JWT for a valid signature and compare capabilities against it

// IsAuthorized checks a HTTP request for auth tokens, determines whether they
// are authorized for a given capability. If so, return true. If not, return false and fill in a 403 response.
func IsAuthorized(w http.ResponseWriter, r *http.Request, capability string) bool {
	return true
}
