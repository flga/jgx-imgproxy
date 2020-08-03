package prom

import (
	"fmt"
	"net/http"
	"time"

	"github.com/felixge/httpsnoop"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	httpRequestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_request_count",
			Help: "Total count of http requests served.",
		}, []string{"path", "method", "status"})

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Http request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"path", "method"})
)

func init() {
	prometheus.MustRegister(httpRequestCount)
	prometheus.MustRegister(httpRequestDuration)
}

// Handler is HTTP middleware that collects stats about the request.
// It collects request counts by path, method and status, and request duration
// by path and method.
//
// It gets the path via the request context, you must provide it with
// handlers.WithRoute() before this handler is called. Failure to do so will
// result in a panic.
//
// We do not have any default behaviour when that occurs because we have no
// idea of the cardinality of the request path. High cardinality is something we
// must avoid at all costs.
//
// A typical setup is to expose the path template, as defined in your router of,
// choice. This will result in capturing metrics for /users/{id} instead of
// /users/42, /users/43, etc.
func Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		muxRoute := mux.CurrentRoute(r)
		var route string
		if name := muxRoute.GetName(); name != "" {
			route = name
		} else {
			name, err := muxRoute.GetPathTemplate()
			if err != nil {
				panic(err)
			}
			route = name
		}

		start := time.Now()
		defer func() {
			if err := recover(); err != nil {
				Observe(r.Method, route, http.StatusInternalServerError, time.Since(start))
				panic(err)
			}
		}()

		result := httpsnoop.CaptureMetrics(next, w, r)
		Observe(r.Method, route, result.Code, time.Since(start))
	})
}

// HandlerFunc is like Prometheus but takes n http.HandlerFunc, for convenience.
func HandlerFunc(next http.HandlerFunc) http.Handler {
	return Handler(next)
}

// Observe records n HTTP request.
//
// It collects request counts by path, method and status, and request duration
// by path and method.
//
// Be careful with the path you pass in, high cardinality is something we
// must avoid at all costs.
//
// A typical setup is to use the path template, as defined in your router of
// choice. This will result in capturing metrics for /users/{id} instead of
// /users/42, /users/43, etc.
func Observe(method, path string, status int, dur time.Duration) {
	httpRequestCount.With(prometheus.Labels{
		"path":   path,
		"method": method,
		"status": fmt.Sprintf("%d", status),
	}).Inc()

	httpRequestDuration.With(prometheus.Labels{
		"path":   path,
		"method": method,
	}).Observe(dur.Seconds())
}
