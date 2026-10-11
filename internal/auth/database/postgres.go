package database

import (
	"context"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool parses databaseURL and initializes a pgx connection pool for PostgreSQL 17 with OpenTelemetry tracing and metrics.
func NewPool(ctx context.Context, databaseURL string, opts ...otelpgx.Option) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	poolConfig.ConnConfig.Tracer = otelpgx.NewTracer(opts...)

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("initialize postgres pool: %w", err)
	}

	_ = otelpgx.RecordStats(pool)

	return pool, nil
}

// Ping checks if the PostgreSQL 17 database is reachable within the given timeout.
func Ping(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) error {
	if pool == nil {
		return fmt.Errorf("postgres pool is nil")
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return pool.Ping(pingCtx)
}
