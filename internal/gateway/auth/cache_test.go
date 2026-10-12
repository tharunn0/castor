package auth

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gwauth "github.com/versity/versitygw/auth"
)

type countingIAMService struct {
	mu           sync.Mutex
	callCount    map[string]int
	accounts     map[string]gwauth.Account
	errs         map[string]error
	shutdownDone bool
}

func newCountingIAMService() *countingIAMService {
	return &countingIAMService{
		callCount: make(map[string]int),
		accounts:  make(map[string]gwauth.Account),
		errs:      make(map[string]error),
	}
}

func (s *countingIAMService) GetUserAccount(access string) (gwauth.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callCount[access]++

	if err, ok := s.errs[access]; ok {
		return gwauth.Account{}, err
	}
	if acc, ok := s.accounts[access]; ok {
		return acc, nil
	}
	return gwauth.Account{}, gwauth.ErrNoSuchUser
}

func (s *countingIAMService) ResolveAccounts(accessKeyIDs []string) ([]string, error) {
	var missing []string
	for _, key := range accessKeyIDs {
		if _, err := s.GetUserAccount(key); err != nil {
			missing = append(missing, key)
		}
	}
	return missing, nil
}

func (s *countingIAMService) CreateAccount(account gwauth.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts[account.Access] = account
	return nil
}

func (s *countingIAMService) UpdateUserAccount(access string, props gwauth.MutableProps) error {
	return nil
}

func (s *countingIAMService) DeleteUserAccount(access string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.accounts, access)
	return nil
}

func (s *countingIAMService) ListUserAccounts() ([]gwauth.Account, error) {
	return nil, nil
}

func (s *countingIAMService) Shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdownDone = true
	return nil
}

func (s *countingIAMService) count(access string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.callCount[access]
}

func TestCachedBridge_PositiveCache(t *testing.T) {
	underlying := newCountingIAMService()
	underlying.accounts["AKIA123"] = gwauth.Account{
		Access: "AKIA123",
		Secret: "secret123",
		Role:   gwauth.RoleUser,
	}

	cached := NewCachedBridge(underlying, WithPositiveTTL(50*time.Millisecond))

	// First call -> cache miss, calls underlying
	acc, err := cached.GetUserAccount("AKIA123")
	if err != nil || acc.Secret != "secret123" {
		t.Fatalf("unexpected result: %+v, err: %v", acc, err)
	}
	if underlying.count("AKIA123") != 1 {
		t.Fatalf("expected 1 call, got %d", underlying.count("AKIA123"))
	}

	// Second call -> cache hit, does not call underlying
	acc2, err := cached.GetUserAccount("AKIA123")
	if err != nil || acc2.Secret != "secret123" {
		t.Fatalf("unexpected result: %+v, err: %v", acc2, err)
	}
	if underlying.count("AKIA123") != 1 {
		t.Fatalf("expected still 1 call (cache hit), got %d", underlying.count("AKIA123"))
	}

	// Sleep past TTL -> cache expires, calls underlying again
	time.Sleep(60 * time.Millisecond)
	acc3, err := cached.GetUserAccount("AKIA123")
	if err != nil || acc3.Secret != "secret123" {
		t.Fatalf("unexpected result: %+v, err: %v", acc3, err)
	}
	if underlying.count("AKIA123") != 2 {
		t.Fatalf("expected 2 calls after expiry, got %d", underlying.count("AKIA123"))
	}
}

func TestCachedBridge_NegativeCache(t *testing.T) {
	underlying := newCountingIAMService()

	cached := NewCachedBridge(underlying, WithNegativeTTL(50*time.Millisecond))

	// First call -> non-existent key, calls underlying
	_, err := cached.GetUserAccount("AKIANOTFOUND")
	if !errors.Is(err, gwauth.ErrNoSuchUser) {
		t.Fatalf("expected ErrNoSuchUser, got %v", err)
	}
	if underlying.count("AKIANOTFOUND") != 1 {
		t.Fatalf("expected 1 call, got %d", underlying.count("AKIANOTFOUND"))
	}

	// Second call -> negative cache hit
	_, err = cached.GetUserAccount("AKIANOTFOUND")
	if !errors.Is(err, gwauth.ErrNoSuchUser) {
		t.Fatalf("expected ErrNoSuchUser, got %v", err)
	}
	if underlying.count("AKIANOTFOUND") != 1 {
		t.Fatalf("expected still 1 call (negative cache hit), got %d", underlying.count("AKIANOTFOUND"))
	}

	// After negative TTL expiry -> queries underlying again
	time.Sleep(60 * time.Millisecond)
	_, err = cached.GetUserAccount("AKIANOTFOUND")
	if !errors.Is(err, gwauth.ErrNoSuchUser) {
		t.Fatalf("expected ErrNoSuchUser, got %v", err)
	}
	if underlying.count("AKIANOTFOUND") != 2 {
		t.Fatalf("expected 2 calls, got %d", underlying.count("AKIANOTFOUND"))
	}
}

func TestCachedBridge_TransientErrorNotCached(t *testing.T) {
	underlying := newCountingIAMService()
	transientErr := errors.New("temporary network timeout")
	underlying.errs["AKIATIMEOUT"] = transientErr

	cached := NewCachedBridge(underlying)

	// First call -> returns transient error
	_, err := cached.GetUserAccount("AKIATIMEOUT")
	if !errors.Is(err, transientErr) {
		t.Fatalf("expected transientErr, got %v", err)
	}
	if underlying.count("AKIATIMEOUT") != 1 {
		t.Fatalf("expected 1 call, got %d", underlying.count("AKIATIMEOUT"))
	}

	// Second call -> transient error must NOT be cached, should query underlying again
	_, err = cached.GetUserAccount("AKIATIMEOUT")
	if !errors.Is(err, transientErr) {
		t.Fatalf("expected transientErr, got %v", err)
	}
	if underlying.count("AKIATIMEOUT") != 2 {
		t.Fatalf("expected 2 calls (transient errors not cached), got %d", underlying.count("AKIATIMEOUT"))
	}
}

func TestCachedBridge_Invalidate(t *testing.T) {
	underlying := newCountingIAMService()
	underlying.accounts["AKIAREVOKE"] = gwauth.Account{
		Access: "AKIAREVOKE",
		Secret: "secret",
		Role:   gwauth.RoleUser,
	}

	cached := NewCachedBridge(underlying)

	_, _ = cached.GetUserAccount("AKIAREVOKE")
	if underlying.count("AKIAREVOKE") != 1 {
		t.Fatalf("expected 1 call, got %d", underlying.count("AKIAREVOKE"))
	}

	cached.Invalidate("AKIAREVOKE")

	_, _ = cached.GetUserAccount("AKIAREVOKE")
	if underlying.count("AKIAREVOKE") != 2 {
		t.Fatalf("expected 2 calls after explicit invalidate, got %d", underlying.count("AKIAREVOKE"))
	}
}

func TestCachedBridge_MutatingInvalidation(t *testing.T) {
	underlying := newCountingIAMService()
	underlying.accounts["AKIAMUTATE"] = gwauth.Account{Access: "AKIAMUTATE", Secret: "secret"}

	cached := NewCachedBridge(underlying)

	_, _ = cached.GetUserAccount("AKIAMUTATE")
	if underlying.count("AKIAMUTATE") != 1 {
		t.Fatalf("expected 1 call, got %d", underlying.count("AKIAMUTATE"))
	}

	_ = cached.UpdateUserAccount("AKIAMUTATE", gwauth.MutableProps{})
	_, _ = cached.GetUserAccount("AKIAMUTATE")
	if underlying.count("AKIAMUTATE") != 2 {
		t.Fatalf("expected 2 calls after UpdateUserAccount, got %d", underlying.count("AKIAMUTATE"))
	}

	_ = cached.DeleteUserAccount("AKIAMUTATE")
	if cached.Len() != 0 {
		t.Fatalf("expected cache to be empty after DeleteUserAccount, got len %d", cached.Len())
	}
}

func TestCachedBridge_ConcurrentAccess(t *testing.T) {
	underlying := newCountingIAMService()
	underlying.accounts["AKIACONCURRENT"] = gwauth.Account{
		Access: "AKIACONCURRENT",
		Secret: "secret",
		Role:   gwauth.RoleUser,
	}

	cached := NewCachedBridge(underlying)

	var wg sync.WaitGroup
	var successCount atomic.Int64

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				acc, err := cached.GetUserAccount("AKIACONCURRENT")
				if err == nil && acc.Access == "AKIACONCURRENT" {
					successCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	if successCount.Load() != 1000 {
		t.Fatalf("expected 1000 successful queries, got %d", successCount.Load())
	}
	if underlying.count("AKIACONCURRENT") > 5 {
		t.Fatalf("expected few underlying calls due to caching, got %d", underlying.count("AKIACONCURRENT"))
	}
}

func TestCachedBridge_PruneOnMaxEntries(t *testing.T) {
	underlying := newCountingIAMService()
	cached := NewCachedBridge(underlying,
		WithPositiveTTL(10*time.Millisecond),
		WithMaxEntries(3),
	)

	underlying.accounts["k1"] = gwauth.Account{Access: "k1"}
	underlying.accounts["k2"] = gwauth.Account{Access: "k2"}
	underlying.accounts["k3"] = gwauth.Account{Access: "k3"}
	underlying.accounts["k4"] = gwauth.Account{Access: "k4"}

	_, _ = cached.GetUserAccount("k1")
	_, _ = cached.GetUserAccount("k2")
	_, _ = cached.GetUserAccount("k3")

	time.Sleep(15 * time.Millisecond) // expire all 3

	// Fourth insertion triggers pruneExpiredLocked
	_, _ = cached.GetUserAccount("k4")

	if cached.Len() > 2 {
		t.Fatalf("expected expired entries to be pruned, got %d", cached.Len())
	}
}

func TestCachedBridge_Shutdown(t *testing.T) {
	underlying := newCountingIAMService()
	cached := NewCachedBridge(underlying)

	underlying.accounts["k1"] = gwauth.Account{Access: "k1"}
	_, _ = cached.GetUserAccount("k1")

	if err := cached.Shutdown(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cached.Len() != 0 {
		t.Fatalf("expected cache to be cleared after Shutdown, got %d", cached.Len())
	}
	if !underlying.shutdownDone {
		t.Fatalf("expected underlying Shutdown to be called")
	}
}
