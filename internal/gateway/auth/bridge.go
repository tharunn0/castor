package auth

import (
	"context"
	"errors"

	gwauth "github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/s3err"
)

var _ gwauth.IAMService = (*Bridge)(nil)

type Bridge struct {
	client        Client
	rootAccessKey string
	rootSecretKey string
}

func NewBridge(client Client, rootAccessKey, rootSecretKey string) *Bridge {
	return &Bridge{
		client:        client,
		rootAccessKey: rootAccessKey,
		rootSecretKey: rootSecretKey,
	}
}

func (b *Bridge) GetUserAccount(access string) (gwauth.Account, error) {
	if access == "" {
		return gwauth.Account{}, gwauth.ErrNoSuchUser
	}

	if b.rootAccessKey != "" && access == b.rootAccessKey {
		return gwauth.Account{
			Access: b.rootAccessKey,
			Secret: b.rootSecretKey,
			Role:   gwauth.RoleAdmin,
		}, nil
	}

	if b.client == nil {
		return gwauth.Account{}, gwauth.ErrNoSuchUser
	}

	resp, err := b.client.ValidateKey(context.Background(), access)
	if err != nil {
		if errors.Is(err, ErrCredentialNotFound) || errors.Is(err, ErrCredentialRevoked) || errors.Is(err, ErrInvalidAccessKey) {
			return gwauth.Account{}, gwauth.ErrNoSuchUser
		}
		return gwauth.Account{}, err
	}

	return gwauth.Account{
		Access: resp.AccessKeyID,
		Secret: resp.SecretAccessKey,
		Role:   gwauth.RoleUser,
	}, nil
}

func (b *Bridge) ResolveAccounts(accessKeyIDs []string) ([]string, error) {
	var missing []string
	for _, key := range accessKeyIDs {
		if _, err := b.GetUserAccount(key); err != nil {
			if errors.Is(err, gwauth.ErrNoSuchUser) {
				missing = append(missing, key)
				continue
			}
			return nil, err
		}
	}
	return missing, nil
}

func (b *Bridge) CreateAccount(account gwauth.Account) error {
	return s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}

func (b *Bridge) UpdateUserAccount(access string, props gwauth.MutableProps) error {
	return s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}

func (b *Bridge) DeleteUserAccount(access string) error {
	return s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}

func (b *Bridge) ListUserAccounts() ([]gwauth.Account, error) {
	return nil, s3err.GetAPIError(s3err.ErrAdminMethodNotSupported)
}

func (b *Bridge) Shutdown() error {
	return nil
}
