package authctx

import (
	"context"

	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/model"
)

type contextKey struct{}

type UserContext struct {
	UserID   uuid.UUID
	Username string
	Role     model.Role
}

func WithUser(ctx context.Context, u UserContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, contextKey{}, u)
}

func FromContext(ctx context.Context) (UserContext, bool) {
	if ctx == nil {
		return UserContext{}, false
	}
	u, ok := ctx.Value(contextKey{}).(UserContext)
	if !ok || u.UserID == uuid.Nil {
		return UserContext{}, false
	}
	return u, true
}

func UserID(ctx context.Context) (uuid.UUID, bool) {
	u, ok := FromContext(ctx)
	return u.UserID, ok
}

func Username(ctx context.Context) (string, bool) {
	u, ok := FromContext(ctx)
	return u.Username, ok
}

func UserRole(ctx context.Context) (model.Role, bool) {
	u, ok := FromContext(ctx)
	return u.Role, ok
}
