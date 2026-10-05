package database

import (
	"context"
	"testing"
	"time"
)

func TestNewPool_InvalidURL(t *testing.T) {
	_, err := NewPool(context.Background(), "://invalid-dsn")
	if err == nil {
		t.Fatal("expected error for invalid dsn, got nil")
	}
}

func TestNewPool_ValidConfig(t *testing.T) {
	pool, err := NewPool(context.Background(), "postgres://user:pass@localhost:5432/castor_auth?sslmode=disable")
	if err != nil {
		t.Fatalf("unexpected error initializing pool: %v", err)
	}
	defer pool.Close()

	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}

func TestPing_NilPool(t *testing.T) {
	err := Ping(context.Background(), nil, time.Second)
	if err == nil {
		t.Fatal("expected error pinging nil pool, got nil")
	}
}
