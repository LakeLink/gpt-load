package codexprivacy

import (
	"container/list"
	"crypto/sha256"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http/httpguts"
)

const (
	turnCapacity      = 4096
	turnTTL           = time.Hour
	maxTurnStateBytes = 16 << 10
)

type turnKey struct{ scope, turn [32]byte }

type turnEntry struct {
	key     turnKey
	value   string
	expires time.Time
}

// Turns owns first-value routing hints for logical turns, independently of
// request completion. A lease keeps the generation captured by its attempt.
type Turns struct {
	mu       sync.Mutex
	entries  map[turnKey]*list.Element
	recent   list.List
	capacity int
	ttl      time.Duration
	now      func() time.Time
}

type Turn struct {
	store *Turns
	entry *turnEntry
}

func NewTurns() *Turns {
	return &Turns{entries: make(map[turnKey]*list.Element), capacity: turnCapacity, ttl: turnTTL, now: time.Now}
}

func (store *Turns) Acquire(scope, turn string) *Turn {
	key := turnKey{scope: sha256.Sum256([]byte(scope)), turn: sha256.Sum256([]byte(turn))}
	store.mu.Lock()
	defer store.mu.Unlock()
	now := store.now()
	if element := store.entries[key]; element != nil {
		entry := element.Value.(*turnEntry)
		if now.Before(entry.expires) {
			entry.expires = now.Add(store.ttl)
			store.recent.MoveToFront(element)
			return &Turn{store, entry}
		}
		store.remove(element)
	}
	for len(store.entries) >= store.capacity {
		store.remove(store.recent.Back())
	}
	entry := &turnEntry{key: key, expires: now.Add(store.ttl)}
	store.entries[key] = store.recent.PushFront(entry)
	return &Turn{store, entry}
}

func (store *Turns) remove(element *list.Element) {
	delete(store.entries, element.Value.(*turnEntry).key)
	store.recent.Remove(element)
}

// current is called under the store lock. Expired/evicted generations never
// attach delayed responses to a later entry for the same logical turn.
func (turn *Turn) current() bool {
	element := turn.store.entries[turn.entry.key]
	if element == nil || element.Value.(*turnEntry) != turn.entry {
		return false
	}
	if !turn.store.now().Before(turn.entry.expires) {
		turn.store.remove(element)
		return false
	}
	return true
}

func (turn *Turn) Value() string {
	turn.store.mu.Lock()
	defer turn.store.mu.Unlock()
	if !turn.current() {
		return ""
	}
	return turn.entry.value
}

// Capture retains the first valid opaque server value, including its original
// bytes. Whitespace-only, oversized, or header-unsafe values are ignored.
func (turn *Turn) Capture(value string) {
	if len(value) > maxTurnStateBytes || strings.TrimSpace(value) == "" || !httpguts.ValidHeaderFieldValue(value) {
		return
	}
	turn.store.mu.Lock()
	defer turn.store.mu.Unlock()
	if turn.current() && turn.entry.value == "" {
		turn.entry.value = value
	}
}
