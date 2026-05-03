package db

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Ride struct {
	ID         uuid.UUID
	RiderID    string
	DriverID   string
	Status     string
	PickupLat  float64
	PickupLng  float64
	DropoffLat float64
	DropoffLng float64
	CreatedAt  time.Time
}

func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.HealthCheckPeriod = 5 * time.Second
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS rides (
	ride_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	rider_id text NOT NULL,
	driver_id text NOT NULL DEFAULT '',
	pickup_lat double precision NOT NULL,
	pickup_lng double precision NOT NULL,
	dropoff_lat double precision NOT NULL,
	dropoff_lng double precision NOT NULL,
	status text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS rides_rider_id_idx ON rides (rider_id);
CREATE INDEX IF NOT EXISTS rides_status_idx ON rides (status);
`)

	return err
}

func InsertRide(ctx context.Context, pool *pgxpool.Pool, r Ride) (uuid.UUID, error) {
	row := pool.QueryRow(ctx, `
INSERT INTO rides (rider_id, driver_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, status)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING ride_id`,
		r.RiderID,
		r.DriverID,
		r.PickupLat,
		r.PickupLng,
		r.DropoffLat,
		r.DropoffLng,
		r.Status,
	)

	var id uuid.UUID
	if err := row.Scan(&id); err != nil {
		return uuid.Nil, fmt.Errorf("insert ride: %w", err)
	}
	return id, nil
}

func GetRide(ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) (Ride, error) {
	row := pool.QueryRow(ctx, `
SELECT ride_id,
       rider_id,
       COALESCE(driver_id, ''),
       status,
       pickup_lat,
       pickup_lng,
       dropoff_lat,
       dropoff_lng,
       created_at
FROM rides
WHERE ride_id = $1`, id)

	var scanned Ride
	var created time.Time
	if err := row.Scan(&scanned.ID, &scanned.RiderID, &scanned.DriverID, &scanned.Status, &scanned.PickupLat, &scanned.PickupLng, &scanned.DropoffLat, &scanned.DropoffLng, &created); err != nil {
		return Ride{}, err
	}

	scanned.CreatedAt = created
	return scanned, nil
}
