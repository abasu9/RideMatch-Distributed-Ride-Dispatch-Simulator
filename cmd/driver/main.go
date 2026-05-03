package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/abasu9/ridematch/internal/driver"
	"github.com/abasu9/ridematch/internal/kafkax"
	"github.com/abasu9/ridematch/internal/observability"
	protoschema "github.com/abasu9/ridematch/internal/proto"
	"github.com/abasu9/ridematch/internal/redisstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func main() {
	rootCtx := context.Background()

	schema := protoschema.MustLoadCompiled()

	log := observability.InitZerolog("driver", getenv("LOG_LEVEL", "info"))

	tp, err := observability.SetupTracing(rootCtx, "driver", getenv("OTEL_EXPORTER_OTLP_ENDPOINT", ""))
	if err != nil {
		panic(err)
	}
	if tp != nil {
		defer func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tp.Shutdown(stopCtx)
		}()
	}

	reg, hist, ctr := observability.MustMetricsRegistry("driver")
	inflight := observability.InFlightGauge("driver")
	reg.MustRegister(inflight)

	rdb := redis.NewClient(&redis.Options{Addr: getenv("REDIS_ADDR", "redis:6379")})
	pingCtx, cancelPing := context.WithTimeout(rootCtx, 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancelPing()
		panic(err)
	}
	cancelPing()

	active := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   "ridematch",
		Subsystem:   "drivers",
		Name:        "active",
		Help:        "Drivers currently registered in Redis set ridematch:drivers:active.",
		ConstLabels: prometheus.Labels{"service_name": "driver"},
	}, func() float64 {
		n, err := rdb.SCard(context.Background(), redisstore.ActiveDriversKey()).Result()
		if err != nil {
			return 0
		}
		return float64(n)
	})
	reg.MustRegister(active)

	brokers := splitCSV(getenv("KAFKA_BROKERS", "redpanda:9092"))
	topic := getenv("KAFKA_TOPIC", "driver-locations")
	initCtx, cancelInit := context.WithTimeout(rootCtx, 30*time.Second)
	if err := kafkax.EnsureTopic(initCtx, brokers, topic); err != nil {
		cancelInit()
		panic(err)
	}
	cancelInit()

	pub := kafkax.NewLocationPublisher(brokers, topic)

	grpcSrv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			observability.UnaryServerMetrics("driver", hist, ctr, inflight),
			observability.UnaryServerRequestID(log),
		),
	)

	impl := &driver.Service{RDB: rdb, Pub: pub, Log: log}
	driver.RegisterGRPC(grpcSrv, schema, impl)

	listener, err := net.Listen("tcp", getenv("GRPC_ADDR", ":50051"))
	if err != nil {
		panic(err)
	}

	httpSrv := observability.StartMetricsHTTPServer(getenv("METRICS_HTTP_ADDR", ":9464"), reg)

	go func() {
		if err := grpcSrv.Serve(listener); err != nil {
			panic(err)
		}
	}()

	shutdown := func() error {
		grpcSrv.GracefulStop()
		_ = pub.Close()
		_ = rdb.Close()
		stopHTTP, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return observability.ShutdownHTTPServer(stopHTTP, httpSrv)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	_ = shutdown()
}

func getenv(k, def string) string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	return v
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return []string{strings.TrimSpace(raw)}
	}
	return out
}

