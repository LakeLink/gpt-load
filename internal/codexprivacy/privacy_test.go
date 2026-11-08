package codexprivacy

import (
	"sync"
	"testing"
)

func TestIdentityVectorsAndCacheLifecycle(t *testing.T) {
	vectors := []struct{ fingerprint, base, installation string }{
		{"account-fingerprint-A", "033d4d64-09d1-5cd3-b44f-23d56acf215d", "c6bd9741-c362-5580-9e4c-c7e5034aae94"},
		{"account-fingerprint-B", "798ced0b-400d-5d09-89d2-5a1380122940", "7f1e06d1-cc08-51fe-a003-301d39f56b6a"},
		{" account-fingerprint-A", "830ac7d1-a242-5678-8f1f-219f38bbc0ca", "c98dbb4f-1f7f-56b3-ae88-ea8ff5827760"},
	}
	cache := NewCache()
	cache.capacity = 1
	for _, v := range vectors {
		identity, err := cache.Resolve(v.fingerprint)
		if err != nil || identity.Namespace.String() != v.base || identity.InstallationID != v.installation {
			t.Fatalf("vector %q: identity=%+v err=%v", v.fingerprint, identity, err)
		}
	}
	// The first entry was evicted; clearing and process re-creation also preserve
	// the same deterministic result. Previously frozen values remain usable.
	first, _ := cache.Resolve(vectors[0].fingerprint)
	cache.Clear()
	for _, c := range []*Cache{cache, NewCache()} {
		got, err := c.Resolve(vectors[0].fingerprint)
		if err != nil || got != first {
			t.Fatalf("recomputation changed identity: %+v %v", got, err)
		}
	}
	for _, invalid := range []string{"", "  "} {
		if _, err := cache.Resolve(invalid); err == nil {
			t.Fatal("missing fingerprint accepted")
		}
	}
}

func TestConcurrentIdentityMisses(t *testing.T) {
	cache := NewCache()
	cache.capacity = 2
	expected, _ := NewCache().Resolve("concurrent-account")
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() {
			for range 20 {
				identity, err := cache.Resolve("concurrent-account")
				if err != nil || identity != expected {
					t.Errorf("concurrent derivation: %+v %v", identity, err)
				}
				cache.Resolve("other")
				cache.Resolve("evict")
				cache.Clear()
			}
		})
	}
	workers.Wait()
	if len(cache.entries) > cache.capacity {
		t.Fatalf("cache exceeded capacity: %d", len(cache.entries))
	}
}
