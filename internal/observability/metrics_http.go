package observability

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartMetricsHTTPServer exposes /metrics for the provided registry.
func StartMetricsHTTPServer(addr string, reg *prometheus.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() { _ = srv.ListenAndServe() }()

	return srv
}

func ShutdownHTTPServer(ctx context.Context, srv *http.Server) error {
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}
