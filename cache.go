package main

import (
	"container/list"
	"sync"
	"time"
)

// CacheEntry is a stored HTTP response.
type CacheEntry struct {
	Key        string
	Status     int
	Header     map[string][]string
	Body       []byte
	StoredAt   time.Time
	ExpiresAt  time.Time // fresh until
	StaleUntil time.Time // serve-stale until (ExpiresAt+SWR)
}

type cacheItem struct {
	key   string
	entry *CacheEntry
}

// LookupResult distinguishes fresh / stale / miss for SWR handling.
type LookupResult int

const (
	LookupMiss LookupResult = iota
	LookupFresh
	LookupStale
)

func (r LookupResult) String() string {
	switch r {
	case LookupFresh:
		return "HIT"
	case LookupStale:
		return "STALE"
	default:
		return "MISS"
	}
}

type CacheStats struct {
	Hits      int64 `json:"hits"`
	StaleHits int64 `json:"stale_hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
	Size      int   `json:"size"`
	Capacity  int   `json:"capacity"`
}

// Cache is a thread-safe LRU response cache with per-entry TTL and
// stale-while-revalidate. An entry is fresh until ExpiresAt, stale-servable
// until StaleUntil, then treated as a miss (and evicted lazily).
type Cache struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	swr      time.Duration
	ll       *list.List // front = most recently used
	items    map[string]*list.Element
	// revalidating tracks keys with a background refresh in flight so only
	// one revalidation per key runs at a time (SWR single-flight).
	revalidating map[string]bool
	stats        CacheStats
	now          func() time.Time // injectable for tests
}

func NewCache(capacity int, ttl, swr time.Duration) *Cache {
	if capacity <= 0 {
		capacity = 256
	}
	return &Cache{
		capacity:     capacity,
		ttl:          ttl,
		swr:          swr,
		ll:           list.New(),
		items:        make(map[string]*list.Element),
		revalidating: make(map[string]bool),
		now:          time.Now,
	}
}

// SetNow overrides the clock (tests only).
func (c *Cache) SetNow(f func() time.Time) { c.mu.Lock(); defer c.mu.Unlock(); c.now = f }

// Get returns (entry, LookupFresh|LookupStale|LookupMiss). A stale entry is
// returned so the caller can serve it while revalidating in the background.
func (c *Cache) Get(key string) (*CacheEntry, LookupResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		c.stats.Misses++
		return nil, LookupMiss
	}
	item := el.Value.(*cacheItem)
	now := c.now()
	if now.After(item.entry.StaleUntil) {
		c.removeLocked(el)
		c.stats.Misses++
		return nil, LookupMiss
	}
	c.ll.MoveToFront(el)
	cp := *item.entry
	if now.After(item.entry.ExpiresAt) {
		c.stats.StaleHits++
		return &cp, LookupStale
	}
	c.stats.Hits++
	return &cp, LookupFresh
}

// Peek returns the lookup state without touching stats (tests / control plane).
func (c *Cache) Peek(key string) LookupResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return LookupMiss
	}
	now := c.now()
	e := el.Value.(*cacheItem).entry
	if now.After(e.StaleUntil) {
		return LookupMiss
	}
	if now.After(e.ExpiresAt) {
		return LookupStale
	}
	return LookupFresh
}

func (c *Cache) Set(key string, status int, header map[string][]string, body []byte) {
	c.SetWithTTL(key, status, header, body, c.ttl)
}

func (c *Cache) SetWithTTL(key string, status int, header map[string][]string, body []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	entry := &CacheEntry{
		Key: key, Status: status, Header: header,
		Body: append([]byte(nil), body...), StoredAt: now,
		ExpiresAt: now.Add(ttl), StaleUntil: now.Add(ttl + c.swr),
	}
	if el, ok := c.items[key]; ok {
		el.Value.(*cacheItem).entry = entry
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&cacheItem{key: key, entry: entry})
	c.items[key] = el
	for c.ll.Len() > c.capacity {
		c.removeLocked(c.ll.Back())
		c.stats.Evictions++
	}
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.removeLocked(el)
	}
}

func (c *Cache) removeLocked(el *list.Element) {
	if el == nil {
		return
	}
	c.ll.Remove(el)
	delete(c.items, el.Value.(*cacheItem).key)
}

// TryStartRevalidation returns true if the caller won the right to revalidate
// key in the background; false if one is already in flight.
func (c *Cache) TryStartRevalidation(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.revalidating[key] {
		return false
	}
	c.revalidating[key] = true
	return true
}

func (c *Cache) FinishRevalidation(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.revalidating, key)
}

func (c *Cache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.Size = c.ll.Len()
	s.Capacity = c.capacity
	return s
}

// HitRate is hits+stale / total lookups, 0 when no lookups yet.
func (s CacheStats) HitRate() float64 {
	total := s.Hits + s.StaleHits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits+s.StaleHits) / float64(total)
}
