// Path Transparency Observatory JWT-based authorization

package pto3

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// For now, all capabilities are authorized.
// By deployment, this will check a JWT for a valid signature and compare capabilities against it

// IsAuthorized checks a HTTP request for auth tokens and determines whether they
// are authorized for a given permission.
//
// If so, return true.
// If not, return false and fill in a 403 response.
//

type Authorizer interface {
	IsAuthorized(http.ResponseWriter, *http.Request, string) bool
}

type APIKeyAuthorizer struct {
	// Map of API key strings to maps of permission strings to boolean permissions
	APIKeys map[string]map[string]bool
}

func (azr *APIKeyAuthorizer) IsAuthorized(w http.ResponseWriter, r *http.Request, permission string) bool {
	authstr := strings.Fields(r.Header.Get("Authorization"))

	if len(authstr) < 2 {
		http.Error(w, "missing or malformed Authorization header", http.StatusBadRequest)
		return false
	}

	if authstr[0] == "APIKEY" {
		perms := azr.APIKeys[authstr[1]]
		if perms == nil {
			http.Error(w, "presented API key not authorized", http.StatusForbidden)
			return false
		}

		if perms[permission] {
			return true
		} else {
			http.Error(w, fmt.Sprintf("presented API key not authorized for %s", permission), http.StatusForbidden)
			return false
		}

	} else if authstr[0] == "JWT" {
		http.Error(w, "JWT not yet implemented", http.StatusNotImplemented)
		return false
	} else {
		http.Error(w, fmt.Sprintf("unsupported authorization type %s", authstr[0]), http.StatusBadRequest)
		return false
	}
}

func LoadAPIKeys(filename string) (*APIKeyAuthorizer, error) {
	var azr APIKeyAuthorizer

	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(b, &azr.APIKeys)
	if err != nil {
		return nil, err
	}

	return &azr, nil
}

type NullAuthorizer struct{}

func (azr *NullAuthorizer) IsAuthorized(w http.ResponseWriter, r *http.Request, permission string) bool {
	return false
}
