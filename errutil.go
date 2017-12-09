package pto3

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

// PTOError represents an error with an associated status code (usually an HTTP status code)
type PTOError struct {
	e string
	s int
}

// PTOErrorf creates a new PTOError with a given format string and arguments
func PTOErrorf(format string, args ...interface{}) *PTOError {
	out := new(PTOError)
	out.s = http.StatusInternalServerError
	out.e = fmt.Sprintf(format, args...)
	return out
}

// StatusIs sets the status of a PTOError, returning the error.
func (e *PTOError) StatusIs(status int) *PTOError {
	e.s = status
	return e
}

// Error returns the error string associated with a PTOError
func (e *PTOError) Error() string {
	return e.e
}

// Status returns the status associated with a PTOError
func (e *PTOError) Status() int {
	return e.s
}

// PTONotFoundError returns an error for a subject of a given type that does not exist
func PTONotFoundError(kind string, subject string) *PTOError {
	return PTOErrorf("%s %s not found", kind, subject).StatusIs(http.StatusNotFound)
}

// PTOExistsError returns an error for a subject of a given kind that already exists
func PTOExistsError(kind string, subject string) *PTOError {
	return PTOErrorf("%s %s already exists", kind, subject).StatusIs(http.StatusBadRequest)
}

// PTOMediaTypeError returns an error for an unsupported MIME type for a given subject
func PTOMediaTypeError(subject string) *PTOError {
	return PTOErrorf("media type %s not supported", subject).StatusIs(http.StatusUnsupportedMediaType)
}

// PTOMissingMetadataError returns an error for a missing metadata key in upload.
func PTOMissingMetadataError(subject string) *PTOError {
	return PTOErrorf("missing key %s in metadata", subject).StatusIs(http.StatusBadRequest)
}

func logtoken() string {
	return fmt.Sprintf("%016x", time.Now().UTC().UnixNano())
}

func handleInternalServerErrorHTTP(w http.ResponseWriter, during string, errmsg string) {
	token := logtoken()
	log.Printf("internal error %s %s: %s\n", during, token, errmsg)
	http.Error(w, fmt.Sprintf("internal error %s: refer to %s in server log", during, token),
		http.StatusInternalServerError)
}

// HandleErrorHTTP writes an appropriate error message to an HTTP response
// writer. It automatically determines whether a PTOError was returned, and if
// so, it extracts the status code therefrom. For internal server errors, it
// writes the error along with a token to the server log.
func HandleErrorHTTP(w http.ResponseWriter, during string, err error) {
	switch ev := err.(type) {
	case *PTOError:
		m := ev.Error()
		s := ev.Status()
		if s == http.StatusInternalServerError {
			handleInternalServerErrorHTTP(w, during, m)
		} else {
			http.Error(w, m, s)
		}
	default:
		if err == nil {
			handleInternalServerErrorHTTP(w, during, "[nil]")
		} else {
			handleInternalServerErrorHTTP(w, during, err.Error())
		}
	}

}
