package handler

import (
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/authctx"
	"github.com/tharunn0/castor/internal/auth/jwt"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/service"
)

type AuthHandler struct {
	svc service.AuthService
}

func NewAuthHandler(svc service.AuthService) *AuthHandler {
	return &AuthHandler{svc: svc}
}

func (h *AuthHandler) RegisterRoutes(app *fiber.App, jwtSecret ...string) {
	app.Post("/api/v1/register", h.Register)
	app.Post("/api/v1/login", h.Login)

	secret := ""
	if len(jwtSecret) > 0 {
		secret = jwtSecret[0]
	}
	if secret != "" {
		mw := jwt.NewMiddleware(secret)
		app.Get("/api/v1/dashboard", mw, h.Dashboard)
		app.Post("/api/v1/keys", mw, h.CreateKey)
		app.Get("/api/v1/keys", mw, h.ListKeys)
		app.Delete("/api/v1/keys/:key_id", mw, h.RevokeKey)
	} else {
		app.Get("/api/v1/dashboard", h.Dashboard)
		app.Post("/api/v1/keys", h.CreateKey)
		app.Get("/api/v1/keys", h.ListKeys)
		app.Delete("/api/v1/keys/:key_id", h.RevokeKey)
	}
	app.Get("/internal/v1/validate-key", h.ValidateKey)
}

func (h *AuthHandler) Register(c fiber.Ctx) error {
	var req model.RegisterRequest
	if err := c.Bind().JSON(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}

	resp, err := h.svc.Register(c.Context(), req)
	if err != nil {
		return err
	}

	return c.Status(fiber.StatusCreated).JSON(resp)
}

func (h *AuthHandler) Login(c fiber.Ctx) error {
	var req model.LoginRequest
	if err := c.Bind().JSON(&req); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid request body")
	}

	resp, err := h.svc.Login(c.Context(), req)
	if err != nil {
		return err
	}

	c.Cookie(&fiber.Cookie{
		Name:     "jwt_token",
		Value:    resp.Token,
		Expires:  resp.ExpiresAt,
		HTTPOnly: true,
		SameSite: "Lax",
	})

	return c.Status(fiber.StatusOK).JSON(resp)
}

func (h *AuthHandler) Dashboard(c fiber.Ctx) error {
	ctx := c.Context()
	user, ok := authctx.FromContext(ctx)
	if !ok {
		return fiber.NewError(fiber.StatusUnauthorized, "unauthorized")
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"message": "welcome to the castor dashboard",
		"user": fiber.Map{
			"id":       user.UserID,
			"username": user.Username,
			"role":     user.Role,
		},
		"stats": fiber.Map{
			"cluster_status": "healthy",
			"active_nodes":   3,
			"storage_used":   "0 B",
		},
	})
}

func (h *AuthHandler) CreateKey(c fiber.Ctx) error {
	ctx := c.Context()
	userID, ok := authctx.UserID(ctx)
	if !ok || userID == uuid.Nil {
		return fiber.NewError(fiber.StatusUnauthorized, "unauthorized")
	}

	var req struct {
		Label string `json:"label"`
	}
	_ = c.Bind().JSON(&req)

	cred, err := h.svc.CreateCredential(ctx, userID, req.Label)
	if err != nil {
		return err
	}

	return c.Status(fiber.StatusCreated).JSON(cred)
}

func (h *AuthHandler) ListKeys(c fiber.Ctx) error {
	ctx := c.Context()
	userID, ok := authctx.UserID(ctx)
	if !ok || userID == uuid.Nil {
		return fiber.NewError(fiber.StatusUnauthorized, "unauthorized")
	}

	creds, err := h.svc.ListCredentials(ctx, userID)
	if err != nil {
		return err
	}

	return c.Status(fiber.StatusOK).JSON(creds)
}

func (h *AuthHandler) RevokeKey(c fiber.Ctx) error {
	ctx := c.Context()
	userID, ok := authctx.UserID(ctx)
	if !ok || userID == uuid.Nil {
		return fiber.NewError(fiber.StatusUnauthorized, "unauthorized")
	}

	keyID := c.Params("key_id")
	if keyID == "" {
		return fiber.NewError(fiber.StatusBadRequest, "missing key_id parameter")
	}

	if err := h.svc.RevokeCredential(ctx, userID, keyID); err != nil {
		return err
	}

	return c.SendStatus(fiber.StatusNoContent)
}

func (h *AuthHandler) ValidateKey(c fiber.Ctx) error {
	accessKey := c.Query("access_key_id")
	if accessKey == "" {
		accessKey = c.Get("X-Access-Key-ID")
	}
	if accessKey == "" {
		return fiber.NewError(fiber.StatusBadRequest, "missing access_key_id parameter")
	}

	cred, err := h.svc.ValidateAccessKey(c.Context(), accessKey)
	if err != nil {
		return err
	}

	return c.Status(fiber.StatusOK).JSON(cred)
}
