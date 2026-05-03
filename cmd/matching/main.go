package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/abasu9/ridematch/internal/matching"
	"github.com/abasu9/ridematch/internal/observability"
	protoschema "github.com/abasu9/ridematch/internal/proto"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func main() {
	rootCtx := context.Background()

	schema := protoschema.MustLoadCompiled()

	log := observability.InitZerolog("matching", getenv("LOG_LEVEL", "info"))

	tp, err := observability.SetupTracing(rootCtx, "matching", getenv("OTEL_EXPORTER_OTLP_ENDPOINT", ""))
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

	reg, hist, ctr := observability.MustMetricsRegistry("matching")
	inflight := observability.InFlightGauge("matching")
	reg.MustRegister(inflight)

	rdb := redis.NewClient(&redis.Options{Addr: getenv("REDIS_ADDR", "redis:6379")})
	pingCtx, cancelPing := context.WithTimeout(rootCtx, 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancelPing()
		panic(err)
	}
	cancelPing()

	grpcSrv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			observability.UnaryServerMetrics("matching", hist, ctr, inflight),
			observability.UnaryServerRequestID(log),
		),
	)

	impl := &matching.Service{RDB: rdb, Log: log}
	matching.RegisterGRPC(grpcSrv, schema, impl)

	listener, err := net.Listen("tcp", getenv("GRPC_ADDR", ":50053"))
	if err != nil {
		panic(err)
	}

	httpSrv := observability.StartMetricsHTTPServer(getenv("METRICS_HTTP_ADDR", ":9466"), reg)

	go func() {
		if err := grpcSrv.Serve(listener); err != nil {
			panic(err)
		}
	}()

	shutdown := func() error {
		grpcSrv.GracefulStop()
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
