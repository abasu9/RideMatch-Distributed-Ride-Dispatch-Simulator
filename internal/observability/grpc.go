package observability

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// UnaryServerRequestID injects a request ID into the context and structured logger.
func UnaryServerRequestID(logger zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, id := EnsureIncomingRequestID(ctx)
		log := logger.With().Timestamp().Str("request_id", id).Logger()
		ctx = log.WithContext(ctx)
		return handler(ctx, req)
	}
}

// UnaryClientPropagateRequestID forwards the request ID from the incoming server context to outgoing calls.
func UnaryClientPropagateRequestID() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		id := strings.TrimSpace(RequestID(ctx))
		if id == "" {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		ctx = grpcmetadata.AppendToOutgoingContext(ctx, "x-request-id", id)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// UnaryServerMetrics records latency histograms/counters labeled by semantic gRPC code.
func UnaryServerMetrics(serviceName string, hist *prometheus.HistogramVec, ctr *prometheus.CounterVec, inflight prometheus.Gauge) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if inflight != nil {
			inflight.Inc()
			defer inflight.Dec()
		}

		start := time.Now()
		resp, err := handler(ctx, req)

		code := codes.OK
		if err != nil {
			if st, ok := status.FromError(err); ok {
				code = st.Code()
			} else {
				code = codes.Unknown
			}
		}

		hist.WithLabelValues(serviceName, info.FullMethod, code.String()).Observe(time.Since(start).Seconds())
		ctr.WithLabelValues(serviceName, info.FullMethod, code.String()).Inc()

		return resp, err
	}
}
