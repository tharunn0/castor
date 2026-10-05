package handler

import (
	"errors"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
	"github.com/tharunn0/castor/internal/auth/service"
)

type AuthHandler struct {
	svc service.AuthService
}

func NewAuthHandler(svc service.AuthService) *AuthHandler {
	return &AuthHandler{svc: svc}
}

func (h *AuthHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/register", h.Register)
	app.Post("/api/auth/register", h.Register)
}

func (h *AuthHandler) Register(c fiber.Ctx) error {
	var req model.RegisterRequest
	if err := c.Bind().JSON(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body",
		})
	}

	resp, err := h.svc.Register(c.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, model.ErrInvalidUsername),
			errors.Is(err, model.ErrInvalidEmail),
			errors.Is(err, model.ErrInvalidPassword),
			errors.Is(err, model.ErrInvalidRole):
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": err.Error(),
			})
		case errors.Is(err, repository.ErrUserAlreadyExists):
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": "user already exists",
			})
		default:
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "internal server error",
			})
		}
	}

	return c.Status(fiber.StatusCreated).JSON(resp)
}
