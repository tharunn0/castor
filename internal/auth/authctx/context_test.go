package authctx

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/model"
)

func TestAuthContext(t *testing.T) {
	t.Run("nil context returns false", func(t *testing.T) {
		if _, ok := FromContext(nil); ok {
			t.Error("expected FromContext(nil) to return false")
		}
		if _, ok := UserID(nil); ok {
			t.Error("expected UserID(nil) to return false")
		}
		if _, ok := Username(nil); ok {
			t.Error("expected Username(nil) to return false")
		}
		if _, ok := UserRole(nil); ok {
			t.Error("expected UserRole(nil) to return false")
		}
	})

	t.Run("empty context returns false", func(t *testing.T) {
		ctx := context.Background()
		if _, ok := FromContext(ctx); ok {
			t.Error("expected FromContext(ctx) to return false")
		}
		if _, ok := UserID(ctx); ok {
			t.Error("expected UserID(ctx) to return false")
		}
	})

	t.Run("nil user id returns false", func(t *testing.T) {
		ctx := WithUser(context.Background(), UserContext{
			UserID:   uuid.Nil,
			Username: "alice",
			Role:     model.RoleUser,
		})
		if _, ok := FromContext(ctx); ok {
			t.Error("expected FromContext with uuid.Nil to return false")
		}
	})

	t.Run("valid user context extracts successfully", func(t *testing.T) {
		expectedID := uuid.New()
		expectedUser := UserContext{
			UserID:   expectedID,
			Username: "alice",
			Role:     model.RoleAdmin,
		}

		ctx := WithUser(context.Background(), expectedUser)

		u, ok := FromContext(ctx)
		if !ok {
			t.Fatal("expected FromContext to return true")
		}
		if u.UserID != expectedID || u.Username != "alice" || u.Role != model.RoleAdmin {
			t.Errorf("unexpected user context: %+v", u)
		}

		uid, ok := UserID(ctx)
		if !ok || uid != expectedID {
			t.Errorf("expected UserID %s, got %s (ok=%v)", expectedID, uid, ok)
		}

		name, ok := Username(ctx)
		if !ok || name != "alice" {
			t.Errorf("expected Username 'alice', got %s (ok=%v)", name, ok)
		}

		role, ok := UserRole(ctx)
		if !ok || role != model.RoleAdmin {
			t.Errorf("expected UserRole 'ADMIN', got %s (ok=%v)", role, ok)
		}
	})

	t.Run("WithUser handles nil parent context", func(t *testing.T) {
		expectedID := uuid.New()
		ctx := WithUser(nil, UserContext{
			UserID:   expectedID,
			Username: "bob",
			Role:     model.RoleUser,
		})
		if ctx == nil {
			t.Fatal("expected non-nil context")
		}
		uid, ok := UserID(ctx)
		if !ok || uid != expectedID {
			t.Errorf("expected UserID %s, got %s", expectedID, uid)
		}
	})
}
