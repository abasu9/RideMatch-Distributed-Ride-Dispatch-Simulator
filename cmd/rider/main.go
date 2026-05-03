package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/abasu9/ridematch/internal/db"
	"github.com/abasu9/ridematch/internal/observability"
	protoschema "github.com/abasu9/ridematch/internal/proto"
	"github.com/abasu9/ridematch/internal/rider"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	rootCtx := context.Background()

	schema := protoschema.MustLoadCompiled()

	log := observability.InitZerolog("rider", getenv("LOG_LEVEL", "info"))

	tp, err := observability.SetupTracing(rootCtx, "rider", getenv("OTEL_EXPORTER_OTLP_ENDPOINT", ""))
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

	reg, hist, ctr := observability.MustMetricsRegistry("rider")
	inflight := observability.InFlightGauge("rider")
	reg.MustRegister(inflight)

	pool, err := db.Connect(rootCtx, getenv("POSTGRES_DSN", ""))
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	migrateCtx, cancelMigrate := context.WithTimeout(rootCtx, 30*time.Second)
	if err := db.Migrate(migrateCtx, pool); err != nil {
		cancelMigrate()
		panic(err)
	}
	cancelMigrate()

	dialCtx, cancelDial := context.WithTimeout(rootCtx, 15*time.Second)
	matchConn, err := grpc.DialContext(
		dialCtx,
		getenv("MATCHING_GRPC_ADDR", "matching:50053"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(observability.UnaryClientPropagateRequestID()),
	)
	cancelDial()
	if err != nil {
		panic(err)
	}
	defer matchConn.Close()

	grpcSrv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			observability.UnaryServerMetrics("rider", hist, ctr, inflight),
			observability.UnaryServerRequestID(log),
		),
	)

	impl := &rider.Service{
		Log:         log,
		PG:          pool,
		MatchClient: matchConn,
		Schema:      schema,
	}
	rider.RegisterGRPC(grpcSrv, schema, impl)

	listener, err := net.Listen("tcp", getenv("GRPC_ADDR", ":50052"))
	if err != nil {
		panic(err)
	}

	httpSrv := observability.StartMetricsHTTPServer(getenv("METRICS_HTTP_ADDR", ":9465"), reg)

	go func() {
		if err := grpcSrv.Serve(listener); err != nil {
			panic(err)
		}
	}()

	shutdown := func() error {
		grpcSrv.GracefulStop()
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
