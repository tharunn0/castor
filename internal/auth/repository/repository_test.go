package repository

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tharunn0/castor/internal/auth/database"
	"github.com/tharunn0/castor/internal/auth/model"
)

func TestPostgresRepository_Interface(t *testing.T) {
	var repo Repository = NewPostgresRepository(nil)
	if repo == nil {
		t.Fatal("expected non-nil repository")
	}

	ctx := context.Background()

	if err := repo.Ping(ctx); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented for nil pool ping, got %v", err)
	}

	if _, err := repo.GetUserByID(ctx, uuid.New()); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented for nil pool GetUserByID, got %v", err)
	}

	if _, err := repo.CreateCredential(ctx, model.CreateCredentialInput{}); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented for nil pool CreateCredential, got %v", err)
	}

	if _, err := repo.GetCredentialByAccessKey(ctx, "AKIAEXAMPLE"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented for nil pool GetCredentialByAccessKey, got %v", err)
	}

	if _, err := repo.ListCredentialsByUserID(ctx, uuid.New()); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented for nil pool ListCredentialsByUserID, got %v", err)
	}

	if err := repo.UpdateCredentialStatus(ctx, "AKIAEXAMPLE", model.StatusActive); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented for nil pool UpdateCredentialStatus, got %v", err)
	}

	if err := repo.DeleteCredential(ctx, "AKIAEXAMPLE"); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented for nil pool DeleteCredential, got %v", err)
	}

	repo.Close()
}

func TestPostgresRepository_CredentialsIntegration(t *testing.T) {
	dsn := os.Getenv("AUTH_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://castor:castor123@localhost:5432/castor_auth?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := database.NewPool(ctx, dsn)
	if err != nil {
		t.Skipf("skipping live postgres integration test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping live postgres integration test, pool ping failed: %v", err)
	}

	repo := NewPostgresRepository(pool)

	testSuffix := uuid.New().String()[:8]
	testUsername := "repo_test_" + testSuffix
	testEmail := "repo_test_" + testSuffix + "@example.com"

	createdUser, err := repo.CreateUser(ctx, model.CreateUserInput{
		Username:     testUsername,
		Email:        testEmail,
		PasswordHash: "hashed_secret_test",
		Role:         model.RoleUser,
	})
	if err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}

	foundUser, err := repo.GetUserByID(ctx, createdUser.ID)
	if err != nil {
		t.Fatalf("failed to get user by id: %v", err)
	}
	if foundUser.ID != createdUser.ID || foundUser.Username != testUsername {
		t.Fatalf("expected user %s (%s), got %s (%s)", createdUser.ID, testUsername, foundUser.ID, foundUser.Username)
	}

	_, err = repo.GetUserByID(ctx, uuid.New())
	if !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("expected ErrUserNotFound for random uuid, got %v", err)
	}

	testAccessKey := "AKIA" + testSuffix + "TEST1234"
	testSecret := "secretkey1234567890abcdefghijklmnopqrst"

	cred, err := repo.CreateCredential(ctx, model.CreateCredentialInput{
		AccessKeyID:     testAccessKey,
		SecretAccessKey: testSecret,
		UserID:          createdUser.ID,
		Label:           "Test Label",
		Status:          model.StatusActive,
	})
	if err != nil {
		t.Fatalf("failed to create credential: %v", err)
	}
	if cred.AccessKeyID != testAccessKey || cred.SecretAccessKey != testSecret || cred.Label != "Test Label" || cred.Status != model.StatusActive {
		t.Fatalf("credential mismatch: %+v", cred)
	}

	_, err = repo.CreateCredential(ctx, model.CreateCredentialInput{
		AccessKeyID:     testAccessKey,
		SecretAccessKey: "another_secret",
		UserID:          createdUser.ID,
		Label:           "Duplicate",
		Status:          model.StatusActive,
	})
	if !errors.Is(err, ErrCredentialAlreadyExists) {
		t.Fatalf("expected ErrCredentialAlreadyExists, got %v", err)
	}

	_, err = repo.CreateCredential(ctx, model.CreateCredentialInput{
		AccessKeyID:     "AKIAFKVIOLATION1",
		SecretAccessKey: "some_secret",
		UserID:          uuid.New(),
		Label:           "FK Error",
		Status:          model.StatusActive,
	})
	if !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("expected ErrUserNotFound on foreign key violation, got %v", err)
	}

	fetchedCred, err := repo.GetCredentialByAccessKey(ctx, testAccessKey)
	if err != nil {
		t.Fatalf("failed to get credential: %v", err)
	}
	if fetchedCred.AccessKeyID != testAccessKey || fetchedCred.SecretAccessKey != testSecret || fetchedCred.Label != "Test Label" {
		t.Fatalf("fetched credential mismatch: %+v", fetchedCred)
	}

	_, err = repo.GetCredentialByAccessKey(ctx, "NONEXISTENTKEY123")
	if !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("expected ErrCredentialNotFound for unknown access key, got %v", err)
	}

	userCreds, err := repo.ListCredentialsByUserID(ctx, createdUser.ID)
	if err != nil {
		t.Fatalf("failed to list credentials: %v", err)
	}
	if len(userCreds) != 1 || userCreds[0].AccessKeyID != testAccessKey {
		t.Fatalf("expected 1 credential with key %s, got %d", testAccessKey, len(userCreds))
	}

	err = repo.UpdateCredentialStatus(ctx, testAccessKey, model.StatusRevoked)
	if err != nil {
		t.Fatalf("failed to update credential status: %v", err)
	}

	revokedCred, err := repo.GetCredentialByAccessKey(ctx, testAccessKey)
	if err != nil {
		t.Fatalf("failed to get revoked credential: %v", err)
	}
	if revokedCred.Status != model.StatusRevoked {
		t.Fatalf("expected status %s, got %s", model.StatusRevoked, revokedCred.Status)
	}

	err = repo.UpdateCredentialStatus(ctx, "NONEXISTENTKEY123", model.StatusRevoked)
	if !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("expected ErrCredentialNotFound on unknown key status update, got %v", err)
	}

	err = repo.DeleteCredential(ctx, testAccessKey)
	if err != nil {
		t.Fatalf("failed to delete credential: %v", err)
	}

	_, err = repo.GetCredentialByAccessKey(ctx, testAccessKey)
	if !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("expected ErrCredentialNotFound after delete, got %v", err)
	}

	err = repo.DeleteCredential(ctx, testAccessKey)
	if !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("expected ErrCredentialNotFound on deleting already deleted credential, got %v", err)
	}

	_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", createdUser.ID)
}
