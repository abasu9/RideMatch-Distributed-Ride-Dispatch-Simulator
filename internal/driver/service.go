package driver

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/abasu9/ridematch/internal/dynfield"
	"github.com/abasu9/ridematch/internal/grpcx"
	"github.com/abasu9/ridematch/internal/h3index"
	"github.com/abasu9/ridematch/internal/kafkax"
	protoschema "github.com/abasu9/ridematch/internal/proto"
	"github.com/abasu9/ridematch/internal/redisstore"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	driverAvailabilityAvailable   protoreflect.EnumNumber = 2
	driverAvailabilityUnspecified protoreflect.EnumNumber = 0
)

type Service struct {
	RDB *redis.Client
	Pub *kafkax.LocationPublisher
	Log zerolog.Logger
}

func RegisterGRPC(gs *grpc.Server, schema *protoschema.Schema, impl *Service) {
	grpcx.RegisterUnaryService(gs, schema.DriverDescriptor, "DriverService", impl, map[string]grpcx.DynUnary{
		"RegisterDriver":    impl.registerDriver,
		"UpdateLocation":    impl.updateLocation,
		"GetDriverStatus":   impl.getDriverStatus,
	})
}

func (s *Service) registerDriver(ctx context.Context, req *dynamicpb.Message, resp *dynamicpb.Message) error {
	id := strings.TrimSpace(dynfield.GetString(req, "driver_id"))
	if id == "" {
		return status.Error(codes.InvalidArgument, "driver_id is required")
	}

	driverKey := redisstore.DriverKey(id)
	activeKey := redisstore.ActiveDriversKey()

	pipe := s.RDB.TxPipeline()
	pipe.SAdd(ctx, activeKey, id)
	pipe.HSetNX(ctx, driverKey, "availability", strconv.FormatInt(int64(driverAvailabilityAvailable), 10))
	pipe.HSetNX(ctx, driverKey, "lat", "0")
	pipe.HSetNX(ctx, driverKey, "lng", "0")
	pipe.HSetNX(ctx, driverKey, "h3_cell", "")
	pipe.HSetNX(ctx, driverKey, "updated_at_unix_nano", strconv.FormatInt(time.Now().UnixNano(), 10))
	if _, err := pipe.Exec(ctx); err != nil {
		return status.Errorf(codes.Internal, "redis driver register failed: %v", err)
	}

	s.Log.Info().Ctx(ctx).Str("driver_id", id).Msg("driver registered")

	return nil
}

func (s *Service) updateLocation(ctx context.Context, req *dynamicpb.Message, resp *dynamicpb.Message) error {
	id := strings.TrimSpace(dynfield.GetString(req, "driver_id"))
	if id == "" {
		return status.Error(codes.InvalidArgument, "driver_id is required")
	}
	lat := dynfield.GetDouble(req, "lat")
	lng := dynfield.GetDouble(req, "lng")
	if lat < -90 || lat > 90 || lng < -180 || lng > 180 {
		return status.Error(codes.InvalidArgument, "lat/lng out of bounds")
	}

	cell, err := h3index.Cell(lat, lng)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "h3 indexing failed: %v", err)
	}
	cellKey := h3index.CellKey(cell)

	now := time.Now()
	tsNano := now.UnixNano()
	// NOTE: Redis ZSET scores are float64 and cannot precisely represent Unix ns timestamps.
	// We use microseconds for ordering and keep ns precision separately in the driver hash.
	tsMicro := now.UnixMicro()

	driverKey := redisstore.DriverKey(id)
	cellZKey := redisstore.CellKey(cellKey)

	oldCell, err := s.RDB.HGet(ctx, driverKey, "h3_cell").Result()
	if err != nil && err != redis.Nil {
		return status.Errorf(codes.Internal, "redis hget failed: %v", err)
	}

	pipe := s.RDB.TxPipeline()
	if strings.TrimSpace(oldCell) != "" && oldCell != cellKey {
		pipe.ZRem(ctx, redisstore.CellKey(oldCell), id)
	}

	pipe.ZAdd(ctx, cellZKey, redis.Z{
		Score:  float64(tsMicro),
		Member: id,
	})
	pipe.HSet(ctx, driverKey, map[string]any{
		"lat":                    fmt.Sprintf("%.7f", lat),
		"lng":                    fmt.Sprintf("%.7f", lng),
		"h3_cell":                cellKey,
		"availability":           strconv.FormatInt(int64(driverAvailabilityAvailable), 10),
		"updated_at_unix_nano":   strconv.FormatInt(tsNano, 10),
	})
	pipe.SAdd(ctx, redisstore.ActiveDriversKey(), id)

	if _, err := pipe.Exec(ctx); err != nil {
		return status.Errorf(codes.Internal, "redis location update failed: %v", err)
	}

	evt := kafkax.DriverLocationEvent{
		DriverID:   id,
		Lat:        lat,
		Lng:        lng,
		H3Cell:     cellKey,
		TsUnixNano: tsNano,
	}

	if err := s.Pub.PublishDriverLocation(ctx, evt); err != nil {
		return status.Errorf(codes.Internal, "kafka publish failed: %v", err)
	}

	s.Log.Info().Ctx(ctx).Str("driver_id", id).Str("h3_cell", cellKey).Msg("driver location updated")

	return nil
}

func (s *Service) getDriverStatus(ctx context.Context, req *dynamicpb.Message, resp *dynamicpb.Message) error {
	id := strings.TrimSpace(dynfield.GetString(req, "driver_id"))
	if id == "" {
		return status.Error(codes.InvalidArgument, "driver_id is required")
	}

	driverKey := redisstore.DriverKey(id)
	values, err := s.RDB.HGetAll(ctx, driverKey).Result()
	if err != nil {
		return status.Errorf(codes.Internal, "redis hgetall failed: %v", err)
	}
	if len(values) == 0 {
		return status.Error(codes.NotFound, "unknown driver")
	}

	lat, _ := strconv.ParseFloat(values["lat"], 64)
	lng, _ := strconv.ParseFloat(values["lng"], 64)

	avail := driverAvailabilityUnspecified
	if v, ok := values["availability"]; ok {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			avail = protoreflect.EnumNumber(n)
		}
	}
	ts, _ := strconv.ParseInt(values["updated_at_unix_nano"], 10, 64)

	dynfield.SetString(resp, "driver_id", id)
	dynfield.SetDouble(resp, "lat", lat)
	dynfield.SetDouble(resp, "lng", lng)
	dynfield.SetString(resp, "h3_cell", values["h3_cell"])
	dynfield.SetEnumNumber(resp, "availability", avail)
	dynfield.SetInt64(resp, "updated_at_unix_nano", ts)

	return nil
}
