package observability

import (
	"context"
	"strings"

	"github.com/google/uuid"
	grpcmetadata "google.golang.org/grpc/metadata"
)

type ctxKeyRequestID struct{}

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID{}, id)
}

func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyRequestID{}).(string)
	return id
}

func IncomingRequestID(ctx context.Context) string {
	md, ok := grpcmetadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("x-request-id")
	if len(vals) == 0 {
		return ""
	}
	if strings.TrimSpace(vals[0]) == "" {
		return ""
	}
	return vals[0]
}


func EnsureIncomingRequestID(ctx context.Context) (context.Context, string) {
	id := strings.TrimSpace(IncomingRequestID(ctx))
	if id != "" {
		return WithRequestID(ctx, id), id
	}

	id = uuid.NewString()
	md, ok := grpcmetadata.FromIncomingContext(ctx)
	if !ok {
		md = grpcmetadata.MD{}
	} else {
		md = md.Copy()
	}
	md.Set("x-request-id", id)
	ctx = grpcmetadata.NewIncomingContext(ctx, md)
	return WithRequestID(ctx, id), id
}
