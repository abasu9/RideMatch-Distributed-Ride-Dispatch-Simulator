package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// MustMetricsRegistry configures a Prometheus registry with process + runtime collectors and common gRPC observers.
func MustMetricsRegistry(serviceName string) (*prometheus.Registry, *prometheus.HistogramVec, *prometheus.CounterVec) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	hist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ridematch",
		Subsystem: "grpc_server",
		Name:      "handling_seconds",
		Help:      "Histogram of unary gRPC handling time (seconds) on each microservice.",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 18),
	}, []string{"service_name", "grpc_method", "grpc_code"})

	ctr := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ridematch",
		Subsystem: "grpc_server",
		Name:      "requests_total",
		Help:      "Unary gRPC request counter labeled by semantic code.",
	}, []string{"service_name", "grpc_method", "grpc_code"})

	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace:   "ridematch",
			Subsystem:   "build",
			Name:        "service_info",
			Help:        "Constant gauge with value=1 labeled by service name.",
			ConstLabels: prometheus.Labels{"service_name": serviceName},
		}, func() float64 { return 1 }),
	)

	reg.MustRegister(hist, ctr)

	return reg, hist, ctr
}

// InFlightGauge returns a gauge meant to approximate concurrent unary work (inc/dec via interceptors).
func InFlightGauge(serviceName string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace:   "ridematch",
		Subsystem:   "grpc_server",
		Name:        "in_flight_requests",
		Help:        "Number of unary gRPC requests currently executing.",
		ConstLabels: prometheus.Labels{"service_name": serviceName},
	})
}
