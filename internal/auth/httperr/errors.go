package httperr

import (
	"errors"

	"github.com/gofiber/fiber/v3"
	"github.com/tharunn0/castor/internal/auth/jwt"
	"github.com/tharunn0/castor/internal/auth/model"
	"github.com/tharunn0/castor/internal/auth/repository"
)

type errorMapping struct {
	target error
	status int
}

var errorRegistry = []errorMapping{
	{target: model.ErrInvalidUserID, status: fiber.StatusBadRequest},
	{target: model.ErrInvalidUsername, status: fiber.StatusBadRequest},
	{target: model.ErrInvalidEmail, status: fiber.StatusBadRequest},
	{target: model.ErrInvalidPassword, status: fiber.StatusBadRequest},
	{target: model.ErrInvalidPasswordHash, status: fiber.StatusBadRequest},
	{target: model.ErrInvalidRole, status: fiber.StatusBadRequest},
	{target: model.ErrInvalidCreatedAt, status: fiber.StatusBadRequest},
	{target: model.ErrInvalidAccessKeyID, status: fiber.StatusBadRequest},
	{target: model.ErrInvalidSecretAccessKey, status: fiber.StatusBadRequest},
	{target: model.ErrInvalidLabel, status: fiber.StatusBadRequest},
	{target: model.ErrInvalidCredentialStatus, status: fiber.StatusBadRequest},
	{target: repository.ErrUserAlreadyExists, status: fiber.StatusConflict},
	{target: repository.ErrCredentialAlreadyExists, status: fiber.StatusConflict},
	{target: repository.ErrUserNotFound, status: fiber.StatusNotFound},
	{target: repository.ErrCredentialNotFound, status: fiber.StatusNotFound},
	{target: repository.ErrNotImplemented, status: fiber.StatusNotImplemented},
	{target: jwt.ErrInvalidToken, status: fiber.StatusUnauthorized},
	{target: jwt.ErrExpiredToken, status: fiber.StatusUnauthorized},
	{target: jwt.ErrMissingToken, status: fiber.StatusUnauthorized},
	{target: jwt.ErrInvalidSigningMethod, status: fiber.StatusUnauthorized},
	{target: jwt.ErrEmptySecret, status: fiber.StatusInternalServerError},
	{target: model.ErrInvalidCredentials, status: fiber.StatusUnauthorized},
}

// Translate converts an application domain error into an HTTP status code and client-safe message.
func Translate(err error) (int, string) {
	if err == nil {
		return fiber.StatusOK, ""
	}

	if fiberErr, ok := errors.AsType[*fiber.Error](err); ok {
		return fiberErr.Code, fiberErr.Message
	}

	for _, mapping := range errorRegistry {
		if errors.Is(err, mapping.target) {
			return mapping.status, mapping.target.Error()
		}
	}

	return fiber.StatusInternalServerError, "internal server error"
}

// ErrorHandler is the centralized Fiber error handling middleware.
func ErrorHandler(c fiber.Ctx, err error) error {
	status, msg := Translate(err)
	return c.Status(status).JSON(fiber.Map{
		"error": msg,
	})
}
