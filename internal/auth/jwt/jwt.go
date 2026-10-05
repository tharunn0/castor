package jwt

import (
	"errors"
	"strings"
	"time"

	golangjwt "github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/model"
)

var (
	ErrEmptySecret          = errors.New("empty jwt secret")
	ErrInvalidToken         = errors.New("invalid token")
	ErrExpiredToken         = errors.New("token has expired")
	ErrInvalidSigningMethod = errors.New("invalid signing method")
	ErrMissingToken         = errors.New("missing authorization token")
)

type Claims struct {
	UserID   uuid.UUID  `json:"user_id"`
	Username string     `json:"username"`
	Role     model.Role `json:"role"`
	golangjwt.RegisteredClaims
}

func GenerateToken(userID uuid.UUID, username string, role model.Role, secret string, ttl time.Duration) (string, error) {
	if secret == "" {
		return "", ErrEmptySecret
	}
	if userID == uuid.Nil {
		return "", model.ErrInvalidUserID
	}
	if err := role.Validate(); err != nil {
		return "", err
	}

	now := time.Now().UTC()
	claims := Claims{
		UserID:   userID,
		Username: username,
		Role:     role,
		RegisteredClaims: golangjwt.RegisteredClaims{
			Subject:   userID.String(),
			Issuer:    "castor-auth",
			IssuedAt:  golangjwt.NewNumericDate(now),
			NotBefore: golangjwt.NewNumericDate(now),
			ExpiresAt: golangjwt.NewNumericDate(now.Add(ttl)),
		},
	}

	token := golangjwt.NewWithClaims(golangjwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

func GenerateUserToken(user *model.User, secret string, ttl time.Duration) (string, error) {
	if user == nil {
		return "", errors.New("nil user")
	}
	return GenerateToken(user.ID, user.Username, user.Role, secret, ttl)
}

func VerifyToken(tokenStr string, secret string) (*Claims, error) {
	if secret == "" {
		return nil, ErrEmptySecret
	}
	if strings.TrimSpace(tokenStr) == "" {
		return nil, ErrMissingToken
	}

	claims := &Claims{}
	token, err := golangjwt.ParseWithClaims(tokenStr, claims, func(token *golangjwt.Token) (any, error) {
		if _, ok := token.Method.(*golangjwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidSigningMethod
		}
		return []byte(secret), nil
	})

	if err != nil {
		if errors.Is(err, golangjwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, ErrInvalidToken
	}

	if !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}
