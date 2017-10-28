package pto3

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

func logtoken() string {
	return fmt.Sprintf("%016x", time.Now().UTC().UnixNano())
}

func LogInternalServerError(w http.ResponseWriter, msg string, err error) {
	token := logtoken()
	log.Printf(token, err)
	http.Error(w, fmt.Sprintf("internal error %s: refer to %s in server log", msg, token), http.StatusInternalServerError)
}
