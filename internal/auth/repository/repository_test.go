package repository

import (
	"context"
	"errors"
	"testing"
)

func TestPostgresRepository_Interface(t *testing.T) {
	var repo Repository = NewPostgresRepository(nil)
	if repo == nil {
		t.Fatal("expected non-nil repository")
	}

	if err := repo.Ping(context.Background()); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented for nil pool ping, got %v", err)
	}

	repo.Close()
}
