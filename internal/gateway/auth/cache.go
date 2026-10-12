package auth

import (
	"errors"
	"sync"
	"time"

	gwauth "github.com/versity/versitygw/auth"
)

var _ gwauth.IAMService = (*CachedBridge)(nil)

const (
	DefaultPositiveTTL = 30 * time.Second
	DefaultNegativeTTL = 5 * time.Second
	DefaultMaxEntries  = 10000
)

type cacheEntry struct {
	account   gwauth.Account
	err       error
	expiresAt time.Time
}

type CachedBridge struct {
	underlying  gwauth.IAMService
	positiveTTL time.Duration
	negativeTTL time.Duration
	maxEntries  int

	mu    sync.RWMutex
	items map[string]cacheEntry
}

type CacheOption func(*CachedBridge)

func WithPositiveTTL(d time.Duration) CacheOption {
	return func(c *CachedBridge) {
		if d > 0 {
			c.positiveTTL = d
		}
	}
}

func WithNegativeTTL(d time.Duration) CacheOption {
	return func(c *CachedBridge) {
		if d > 0 {
			c.negativeTTL = d
		}
	}
}

func WithMaxEntries(max int) CacheOption {
	return func(c *CachedBridge) {
		if max > 0 {
			c.maxEntries = max
		}
	}
}

func NewCachedBridge(underlying gwauth.IAMService, opts ...CacheOption) *CachedBridge {
	c := &CachedBridge{
		underlying:  underlying,
		positiveTTL: DefaultPositiveTTL,
		negativeTTL: DefaultNegativeTTL,
		maxEntries:  DefaultMaxEntries,
		items:       make(map[string]cacheEntry),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *CachedBridge) GetUserAccount(access string) (gwauth.Account, error) {
	if access == "" {
		return gwauth.Account{}, gwauth.ErrNoSuchUser
	}

	now := time.Now()

	c.mu.RLock()
	entry, found := c.items[access]
	if found && now.Before(entry.expiresAt) {
		c.mu.RUnlock()
		if entry.err != nil {
			return gwauth.Account{}, entry.err
		}
		return entry.account, nil
	}
	c.mu.RUnlock()

	acc, err := c.underlying.GetUserAccount(access)

	c.mu.Lock()
	if len(c.items) >= c.maxEntries {
		c.pruneExpiredLocked(now)
	}

	if err == nil {
		c.items[access] = cacheEntry{
			account:   acc,
			expiresAt: now.Add(c.positiveTTL),
		}
	} else if errors.Is(err, gwauth.ErrNoSuchUser) {
		c.items[access] = cacheEntry{
			err:       err,
			expiresAt: now.Add(c.negativeTTL),
		}
	}
	c.mu.Unlock()

	return acc, err
}

func (c *CachedBridge) ResolveAccounts(accessKeyIDs []string) ([]string, error) {
	var missing []string
	for _, key := range accessKeyIDs {
		if _, err := c.GetUserAccount(key); err != nil {
			if errors.Is(err, gwauth.ErrNoSuchUser) {
				missing = append(missing, key)
				continue
			}
			return nil, err
		}
	}
	return missing, nil
}

func (c *CachedBridge) Invalidate(access string) {
	c.mu.Lock()
	delete(c.items, access)
	c.mu.Unlock()
}

func (c *CachedBridge) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

func (c *CachedBridge) pruneExpiredLocked(now time.Time) {
	for k, v := range c.items {
		if !now.Before(v.expiresAt) {
			delete(c.items, k)
		}
	}
}

func (c *CachedBridge) CreateAccount(account gwauth.Account) error {
	return c.underlying.CreateAccount(account)
}

func (c *CachedBridge) UpdateUserAccount(access string, props gwauth.MutableProps) error {
	c.Invalidate(access)
	return c.underlying.UpdateUserAccount(access, props)
}

func (c *CachedBridge) DeleteUserAccount(access string) error {
	c.Invalidate(access)
	return c.underlying.DeleteUserAccount(access)
}

func (c *CachedBridge) ListUserAccounts() ([]gwauth.Account, error) {
	return c.underlying.ListUserAccounts()
}

func (c *CachedBridge) Shutdown() error {
	c.mu.Lock()
	clear(c.items)
	c.mu.Unlock()
	return c.underlying.Shutdown()
}
