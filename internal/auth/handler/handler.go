package handler

import (
	"github.com/gofiber/fiber/v3"
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
		app.Get("/api/v1/dashboard", jwt.NewMiddleware(secret), h.Dashboard)
	} else {
		app.Get("/api/v1/dashboard", h.Dashboard)
	}
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
	claims, ok := jwt.GetClaims(c)
	if !ok || claims == nil {
		return fiber.NewError(fiber.StatusUnauthorized, "unauthorized")
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"message": "welcome to the castor dashboard",
		"user": fiber.Map{
			"id":       claims.UserID,
			"username": claims.Username,
			"role":     claims.Role,
		},
		"stats": fiber.Map{
			"cluster_status": "healthy",
			"active_nodes":   3,
			"storage_used":   "0 B",
		},
	})
}
