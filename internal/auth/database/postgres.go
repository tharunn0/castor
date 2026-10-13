package database

import (
	"context"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tharunn0/castor/internal/auth/config"
)

// NewPool parses databaseURL and initializes a pgx connection pool for PostgreSQL 17 with OpenTelemetry tracing and metrics.
func NewPool(ctx context.Context, databaseURL string, opts ...otelpgx.Option) (*pgxpool.Pool, error) {
	return NewPoolWithConfig(ctx, config.Config{DatabaseURL: databaseURL}, opts...)
}

// NewPoolWithConfig initializes a pgx connection pool with database sizing and timeout configurations.
func NewPoolWithConfig(ctx context.Context, cfg config.Config, opts ...otelpgx.Option) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	if cfg.DBMaxConns > 0 {
		poolConfig.MaxConns = cfg.DBMaxConns
	}
	if cfg.DBMinConns > 0 {
		poolConfig.MinConns = cfg.DBMinConns
	}
	if cfg.DBMaxConnLifetime > 0 {
		poolConfig.MaxConnLifetime = cfg.DBMaxConnLifetime
	}
	if cfg.DBMaxConnIdleTime > 0 {
		poolConfig.MaxConnIdleTime = cfg.DBMaxConnIdleTime
	}
	if cfg.DBHealthCheckPeriod > 0 {
		poolConfig.HealthCheckPeriod = cfg.DBHealthCheckPeriod
	}
	if cfg.DBConnectTimeout > 0 {
		poolConfig.ConnConfig.ConnectTimeout = cfg.DBConnectTimeout
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
