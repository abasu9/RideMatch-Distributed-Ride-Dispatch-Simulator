package rider

import (
	"context"
	"strings"

	"github.com/abasu9/ridematch/internal/db"
	"github.com/abasu9/ridematch/internal/dynfield"
	"github.com/abasu9/ridematch/internal/grpcx"
	protoschema "github.com/abasu9/ridematch/internal/proto"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Service struct {
	Log         zerolog.Logger
	PG          *pgxpool.Pool
	MatchClient *grpc.ClientConn
	Schema      *protoschema.Schema
}

func RegisterGRPC(gs *grpc.Server, schema *protoschema.Schema, impl *Service) {
	grpcx.RegisterUnaryService(gs, schema.RiderDescriptor, "RiderService", impl, map[string]grpcx.DynUnary{
		"RequestRide":   impl.requestRide,
		"GetRideStatus": impl.getRideStatus,
	})
}

func (s *Service) requestRide(ctx context.Context, req *dynamicpb.Message, resp *dynamicpb.Message) error {
	riderID := strings.TrimSpace(dynfield.GetString(req, "rider_id"))
	if riderID == "" {
		return status.Error(codes.InvalidArgument, "rider_id is required")
	}

	pLat := dynfield.GetDouble(req, "pickup_lat")
	pLng := dynfield.GetDouble(req, "pickup_lng")
	dLat := dynfield.GetDouble(req, "dropoff_lat")
	dLng := dynfield.GetDouble(req, "dropoff_lng")

	matchSvc := s.Schema.Service(s.Schema.MatchingDescriptor, "MatchingService")
	matchMd := s.Schema.Method(matchSvc, "FindNearestDriver")

	matchReq := s.Schema.NewMessage(matchMd.Input())
	matchResp := s.Schema.NewMessage(matchMd.Output())

	dynfield.SetDouble(matchReq, "pickup_lat", pLat)
	dynfield.SetDouble(matchReq, "pickup_lng", pLng)
	dynfield.SetString(matchReq, "rider_id", riderID)

	if err := grpcx.UnaryCall(ctx, s.MatchClient, s.Schema.MatchingFindNearestGRPCPath, matchReq, matchResp); err != nil {
		return status.Errorf(codes.Unavailable, "matching call failed: %v", err)
	}

	ok := dynfield.GetBool(matchResp, "found")
	driverID := strings.TrimSpace(dynfield.GetString(matchResp, "driver_id"))

	rideStatus := "ASSIGNED"
	if !ok || driverID == "" {
		rideStatus = "NO_DRIVER"
		driverID = ""
	}

	rideID, err := db.InsertRide(ctx, s.PG, db.Ride{
		RiderID:    riderID,
		DriverID:   driverID,
		Status:     rideStatus,
		PickupLat:  pLat,
		PickupLng:  pLng,
		DropoffLat: dLat,
		DropoffLng: dLng,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "postgres insert ride failed: %v", err)
	}

	dynfield.SetBool(resp, "assigned", rideStatus == "ASSIGNED")
	dynfield.SetString(resp, "ride_id", rideID.String())
	dynfield.SetString(resp, "driver_id", driverID)
	dynfield.SetString(resp, "status", rideStatus)

	s.Log.Info().Ctx(ctx).
		Str("rider_id", riderID).
		Str("ride_id", rideID.String()).
		Str("status", rideStatus).
		Msg("ride requested")

	return nil
}

func (s *Service) getRideStatus(ctx context.Context, req *dynamicpb.Message, resp *dynamicpb.Message) error {
	idStr := strings.TrimSpace(dynfield.GetString(req, "ride_id"))
	if idStr == "" {
		return status.Error(codes.InvalidArgument, "ride_id is required")
	}

	rideUUID, err := uuid.Parse(idStr)
	if err != nil {
		return status.Error(codes.InvalidArgument, "ride_id must be a UUID")
	}

	ride, err := db.GetRide(ctx, s.PG, rideUUID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return status.Error(codes.NotFound, "unknown ride")
		}
		return status.Errorf(codes.Internal, "postgres query failed: %v", err)
	}

	dynfield.SetString(resp, "ride_id", ride.ID.String())
	dynfield.SetString(resp, "rider_id", ride.RiderID)
	dynfield.SetString(resp, "driver_id", ride.DriverID)
	dynfield.SetString(resp, "status", ride.Status)
	dynfield.SetInt64(resp, "created_at_unix_nano", ride.CreatedAt.UTC().UnixNano())

	return nil
}
