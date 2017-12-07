package papi

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

type LoggingResponseWriter struct {
	w      http.ResponseWriter
	status int
	length int
}

func (lw *LoggingResponseWriter) Header() http.Header {
	return lw.w.Header()
}

func (lw *LoggingResponseWriter) WriteHeader(status int) {
	lw.status = status
	lw.w.WriteHeader(status)
}

func (lw *LoggingResponseWriter) Write(b []byte) (int, error) {
	written, err := lw.w.Write(b)
	if err == nil {
		lw.length += written
	}
	return written, err
}

type HandlerFunc func(http.ResponseWriter, *http.Request)

func LogAccess(l *log.Logger, handler HandlerFunc) HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lw := LoggingResponseWriter{w: w}
		start := time.Now()
		handler(&lw, r)
		duration := time.Since(start)
		l.Printf("%s %s %d %d %v", r.Method, r.URL.String(), lw.length, lw.status, duration)
	}
}
