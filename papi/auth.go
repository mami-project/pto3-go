// Path Transparency Observatory JWT-based authorization

package papi

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
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

	// load defaults from apikeys if present
	perms := map[string]bool{}

	defperms := azr.APIKeys["default"]
	if defperms != nil {
		for k, v := range defperms {
			perms[k] = v
		}
	}
	log.Printf("default authorization is %+v", perms)

	// look for an authorization header
	authhdr := r.Header.Get("Authorization")

	if authhdr != "" {

		authfield := strings.Fields(authhdr)

		if len(authfield) < 2 {
			http.Error(w, fmt.Sprintf("malformed Authorization header: %v", authhdr), http.StatusBadRequest)
			return false
		} else if authfield[0] == "APIKEY" {
			keyperms := azr.APIKeys[authfield[1]]
			if keyperms != nil {
				// update permissions with those for the presented key
				for k, v := range keyperms {
					perms[k] = v
				}
				log.Printf("inherited authorization for %s is %+v", authfield[1], perms)

			}
		} else {
			http.Error(w, fmt.Sprintf("unsupported authorization type %s", authfield[0]), http.StatusBadRequest)
			return false
		}
	}

	if perms[permission] {
		return true
	} else {
		http.Error(w, fmt.Sprintf("not authorized for %s", permission), http.StatusForbidden)
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
