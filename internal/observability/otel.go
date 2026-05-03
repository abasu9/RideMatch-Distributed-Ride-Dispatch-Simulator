package observability

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SetupTracing configures OTLP exporting and W3C trace context propagation unless endpoint is blank.
//
// Typical endpoint formats: host:4317 (docker network) or localhost:4317.
func SetupTracing(ctx context.Context, serviceName, otlpEndpoint string) (*sdktrace.TracerProvider, error) {
	ep := strings.TrimSpace(otlpEndpoint)
	if ep == "" {
		return nil, nil
	}

	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimPrefix(ep, "https://")

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(ep),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String("service.name", serviceName)),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, nil
}
