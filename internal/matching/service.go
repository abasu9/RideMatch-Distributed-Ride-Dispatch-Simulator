package matching

import (
	"context"
	"strconv"
	"strings"

	"github.com/abasu9/ridematch/internal/dynfield"
	"github.com/abasu9/ridematch/internal/geo"
	"github.com/abasu9/ridematch/internal/grpcx"
	"github.com/abasu9/ridematch/internal/h3index"
	protoschema "github.com/abasu9/ridematch/internal/proto"
	"github.com/abasu9/ridematch/internal/redisstore"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/dynamicpb"
)

const availabilityAvailable int64 = 2

type Service struct {
	RDB *redis.Client
	Log zerolog.Logger
}

func RegisterGRPC(gs *grpc.Server, schema *protoschema.Schema, impl *Service) {
	grpcx.RegisterUnaryService(gs, schema.MatchingDescriptor, "MatchingService", impl, map[string]grpcx.DynUnary{
		"FindNearestDriver": impl.findNearestDriver,
	})
}

func (s *Service) findNearestDriver(ctx context.Context, req *dynamicpb.Message, resp *dynamicpb.Message) error {
	pLat := dynfield.GetDouble(req, "pickup_lat")
	pLng := dynfield.GetDouble(req, "pickup_lng")
	rider := strings.TrimSpace(dynfield.GetString(req, "rider_id"))
	if rider == "" {
		return status.Error(codes.InvalidArgument, "rider_id is required")
	}
	if pLat < -90 || pLat > 90 || pLng < -180 || pLng > 180 {
		return status.Error(codes.InvalidArgument, "pickup coords out of bounds")
	}

	originCell, err := h3index.Cell(pLat, pLng)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "h3 pickup indexing failed: %v", err)
	}

	disks, err := h3index.GridDiskK1(originCell)
	if err != nil {
		return status.Errorf(codes.Internal, "h3 neighborhood failed: %v", err)
	}

	candidates := make(map[string]struct{})
	for _, c := range disks {
		if c == 0 {
			continue
		}
		zkey := redisstore.CellKey(h3index.CellKey(c))

		driverIDs, err := s.RDB.ZRange(ctx, zkey, 0, -1).Result()
		if err != nil {
			return status.Errorf(codes.Internal, "redis zrange failed: %v", err)
		}
		for _, id := range driverIDs {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			candidates[id] = struct{}{}
		}
	}

	type scored struct {
		id   string
		dist float64
		lat  float64
		lng  float64
	}

	var best scored
	found := false

	for driverID := range candidates {
		hkey := redisstore.DriverKey(driverID)
		fields, err := s.RDB.HGetAll(ctx, hkey).Result()
		if err != nil {
			return status.Errorf(codes.Internal, "redis driver fetch failed: %v", err)
		}

		latRaw := strings.TrimSpace(fields["lat"])
		lngRaw := strings.TrimSpace(fields["lng"])
		if latRaw == "" || lngRaw == "" {
			continue
		}

		lat, err1 := strconv.ParseFloat(latRaw, 64)
		lng, err2 := strconv.ParseFloat(lngRaw, 64)
		if err1 != nil || err2 != nil {
			continue
		}

		// Exclude drivers without a usable location ping (fresh registration placeholders).
		if lat == 0 && lng == 0 {
			continue
		}

		avail, _ := strconv.ParseInt(fields["availability"], 10, 32)
		if avail != availabilityAvailable {
			continue
		}

		cell := strings.TrimSpace(fields["h3_cell"])
		if cell == "" {
			continue
		}

		dist := geo.DistanceMeters(pLat, pLng, lat, lng)
		if !found || dist < best.dist || (dist == best.dist && driverID < best.id) {
			found = true
			best = scored{id: driverID, dist: dist, lat: lat, lng: lng}
		}
	}

	if !found {
		dynfield.SetBool(resp, "found", false)
		dynfield.SetString(resp, "driver_id", "")
		dynfield.SetDouble(resp, "lat", 0)
		dynfield.SetDouble(resp, "lng", 0)
		dynfield.SetDouble(resp, "distance_meters", 0)
		s.Log.Info().Ctx(ctx).Str("rider_id", rider).Msg("no driver found in neighborhood")
		return nil
	}

	dynfield.SetBool(resp, "found", true)
	dynfield.SetString(resp, "driver_id", best.id)
	dynfield.SetDouble(resp, "lat", best.lat)
	dynfield.SetDouble(resp, "lng", best.lng)
	dynfield.SetDouble(resp, "distance_meters", best.dist)

	s.Log.Info().Ctx(ctx).
		Str("rider_id", rider).
		Str("driver_id", best.id).
		Float64("distance_meters", best.dist).
		Msg("nearest driver selected")

	return nil
}
