// Package codexprivacy derives account-local client identities from the stored
// credential identity. The derivation inputs and names are a versioned contract.
package codexprivacy

import (
	"container/list"
	"errors"
	"strings"
	"sync"

	"gpt-load/internal/affinity"

	"github.com/google/uuid"
)

const derivationVersion = "gpt-load/codex-privacy/v1/"

// ErrMissingIdentityFingerprint identifies an unusable selected account.
var ErrMissingIdentityFingerprint = errors.New("Codex account identity fingerprint is missing")

// This application namespace and purpose names are permanent. Changing them
// rotates every account identity, including after cache eviction.
var applicationNamespace = uuid.MustParse("ce12d2ae-fc14-5a53-bfe6-1aa859d0c91a")

type Identity struct {
	Namespace      uuid.UUID
	InstallationID string
}

type cacheEntry struct {
	key      string
	identity Identity
}

// Cache keeps deterministic identities separate from affinity targets and
// server routing state. It uses affinity's bounded LRU policy and default bound;
// affinity's conditional target revisions have a different lifecycle.
type Cache struct {
	mu       sync.Mutex
	entries  map[string]*list.Element
	recent   list.List
	capacity int
}

func NewCache() *Cache {
	return &Cache{capacity: affinity.DefaultCapacity, entries: make(map[string]*list.Element)}
}

func (c *Cache) Resolve(fingerprint string) (Identity, error) {
	if strings.TrimSpace(fingerprint) == "" {
		return Identity{}, ErrMissingIdentityFingerprint
	}
	key := derivationVersion + fingerprint
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.recent.MoveToFront(e)
		return e.Value.(cacheEntry).identity, nil
	}
	base := uuid.NewSHA1(applicationNamespace, []byte(key))
	identity := Identity{Namespace: base, InstallationID: uuid.NewSHA1(base, []byte("installation/v1")).String()}
	for len(c.entries) >= c.capacity {
		e := c.recent.Back()
		delete(c.entries, e.Value.(cacheEntry).key)
		c.recent.Remove(e)
	}
	c.entries[key] = c.recent.PushFront(cacheEntry{key: key, identity: identity})
	return identity, nil
}

// Clear releases runtime entries; subsequent requests recompute the same IDs.
func (c *Cache) Clear() { c.mu.Lock(); defer c.mu.Unlock(); clear(c.entries); c.recent.Init() }
