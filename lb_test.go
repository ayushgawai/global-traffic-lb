package main

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestWeightedRoundRobinFairness(t *testing.T) {
	tests := []struct {
		name    string
		weights []int
		picks   int
		want    []float64 // expected proportions
	}{
		{"equal", []int{1, 1, 1}, 300, []float64{1.0 / 3, 1.0 / 3, 1.0 / 3}},
		{"2-1", []int{2, 1}, 300, []float64{2.0 / 3, 1.0 / 3}},
		{"3-2-1", []int{3, 2, 1}, 600, []float64{0.5, 1.0 / 3, 1.0 / 6}},
		{"5-1-1", []int{5, 1, 1}, 700, []float64{5.0 / 7, 1.0 / 7, 1.0 / 7}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ups []*Upstream
			for i, w := range tc.weights {
				ups = append(ups, NewUpstream(fmt.Sprint(i), "127.0.0.1:1", w))
			}
			s := WeightedRoundRobin{}
			counts := make([]int, len(ups))
			for i := 0; i < tc.picks; i++ {
				got := s.Pick(ups)
				for j, u := range ups {
					if u == got {
						counts[j]++
					}
				}
			}
			for i, want := range tc.want {
				gotProp := float64(counts[i]) / float64(tc.picks)
				if diff := gotProp - want; diff > 0.02 || diff < -0.02 {
					t.Errorf("backend %d: proportion %.3f want %.3f (counts=%v)", i, gotProp, want, counts)
				}
			}
		})
	}
}

func TestWeightedRoundRobinInterleaving(t *testing.T) {
	a := NewUpstream("a", "x", 2)
	b := NewUpstream("b", "x", 1)
	s := WeightedRoundRobin{}
	var seq string
	for i := 0; i < 3; i++ {
		seq += s.Pick([]*Upstream{a, b}).ID
	}
	// Smooth WRR interleaves: a b a, not a a b.
	if seq != "aba" {
		t.Errorf("smooth WRR sequence = %q, want aba", seq)
	}
}

func TestLeastConnectionsSelection(t *testing.T) {
	tests := []struct {
		name   string
		active []int64
		want   string
	}{
		{"all idle picks first", []int64{0, 0, 0}, "a"},
		{"picks emptiest", []int64{5, 1, 3}, "b"},
		{"picks last when emptiest", []int64{5, 4, 0}, "c"},
	}
	ids := []string{"a", "b", "c"}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ups []*Upstream
			for i, id := range ids {
				u := NewUpstream(id, "x", 1)
				u.ActiveConns.Store(tc.active[i])
				ups = append(ups, u)
			}
			if got := (LeastConnections{}).Pick(ups); got.ID != tc.want {
				t.Errorf("pick = %s want %s", got.ID, tc.want)
			}
		})
	}
	t.Run("weight normalises load", func(t *testing.T) {
		heavy := NewUpstream("heavy", "x", 4) // 4 conns / weight 4 = 1.0
		heavy.ActiveConns.Store(4)
		light := NewUpstream("light", "x", 1) // 2/1 = 2.0
		light.ActiveConns.Store(2)
		if got := (LeastConnections{}).Pick([]*Upstream{light, heavy}); got.ID != "heavy" {
			t.Errorf("pick = %s want heavy (lower active/weight ratio)", got.ID)
		}
	})
}

func TestCacheLRUTTLAndSWR(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	c := NewCache(2, 10*time.Second, 5*time.Second)
	c.SetNow(func() time.Time { return now })

	c.Set("k1", 200, map[string][]string{"Content-Type": {"text/plain"}}, []byte("hello"))
	if e, res := c.Get("k1"); res != LookupFresh || string(e.Body) != "hello" {
		t.Fatalf("fresh get = %v %q", res, e.Body)
	}
	// Within TTL+SWR but past TTL => stale, still served.
	now = now.Add(12 * time.Second)
	if e, res := c.Get("k1"); res != LookupStale || string(e.Body) != "hello" {
		t.Fatalf("stale get = %v %q, want STALE hello", res, e.Body)
	}
	// Past stale window => miss.
	now = now.Add(10 * time.Second)
	if _, res := c.Get("k1"); res != LookupMiss {
		t.Fatalf("expired get = %v want MISS", res)
	}

	// LRU eviction: capacity 2, access a,c order matters.
	now = time.Unix(1_700_000_000, 0)
	c2 := NewCache(2, time.Minute, time.Minute)
	c2.SetNow(func() time.Time { return now })
	c2.Set("a", 200, nil, []byte("a"))
	c2.Set("b", 200, nil, []byte("b"))
	c2.Get("a")                        // a now MRU, b LRU
	c2.Set("c", 200, nil, []byte("c")) // evicts b
	if _, res := c2.Get("b"); res != LookupMiss {
		t.Errorf("b should be evicted, got %v", res)
	}
	if _, res := c2.Get("a"); res != LookupFresh {
		t.Errorf("a should survive, got %v", res)
	}
	if st := c2.Stats(); st.Evictions != 1 || st.Size != 2 {
		t.Errorf("stats = %+v want evictions=1 size=2", st)
	}

	// SWR single-flight: only one revalidator wins.
	if !c.TryStartRevalidation("k") {
		t.Fatal("first revalidation should win")
	}
	if c.TryStartRevalidation("k") {
		t.Fatal("second concurrent revalidation should lose")
	}
	c.FinishRevalidation("k")
	if !c.TryStartRevalidation("k") {
		t.Fatal("revalidation after finish should win")
	}
}

func TestCircuitBreakerTransitions(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cb := NewCircuitBreaker(3, 10*time.Second)
	cb.SetNow(func() time.Time { return now })

	if cb.State() != CircuitClosed || !cb.Allow() {
		t.Fatal("should start closed")
	}
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.State() != CircuitClosed {
		t.Fatal("2 failures below threshold=3 should stay closed")
	}
	cb.RecordFailure()
	if cb.State() != CircuitOpen || cb.Allow() {
		t.Fatal("3rd failure should open and block")
	}
	now = now.Add(5 * time.Second)
	if cb.State() != CircuitOpen {
		t.Fatal("still within cooldown, should stay open")
	}
	now = now.Add(6 * time.Second) // past cooldown
	if cb.State() != CircuitHalfOpen {
		t.Fatal("after cooldown should be half-open")
	}
	if !cb.Allow() {
		t.Fatal("half-open should allow one probe")
	}
	if cb.Allow() {
		t.Fatal("half-open should allow only one concurrent probe")
	}
	cb.RecordFailure() // probe fails -> re-open
	if cb.State() != CircuitOpen {
		t.Fatal("failed probe should re-open")
	}
	now = now.Add(11 * time.Second)
	if !cb.Allow() {
		t.Fatal("second probe should be allowed after cooldown")
	}
	cb.RecordSuccess()
	if cb.State() != CircuitClosed || !cb.Allow() {
		t.Fatal("successful probe should close")
	}
}

func TestRoutingTableLongestPrefix(t *testing.T) {
	rt := NewRoutingTable([]Route{
		{Prefix: "/", BackendIDs: []string{"root"}},
		{Prefix: "/api/", BackendIDs: []string{"api"}},
		{Prefix: "/api/v2/", BackendIDs: []string{"apiv2"}},
	})
	tests := []struct{ path, want string }{
		{"/", "root"},
		{"/anything", "root"},
		{"/api/users", "api"},
		{"/api/v2/users", "apiv2"},
		{"/api/v2", "api"}, // no trailing slash: /api/ is longest matching
	}
	for _, tc := range tests {
		r := rt.Match(tc.path)
		if r == nil || len(r.BackendIDs) == 0 || r.BackendIDs[0] != tc.want {
			t.Errorf("Match(%q) = %+v want backend %s", tc.path, r, tc.want)
		}
	}
	if r := NewRoutingTable(nil).Match("/x"); r != nil {
		t.Errorf("empty table Match = %+v want nil", r)
	}
}

func TestCacheKeyAndProxyRoutingSmoke(t *testing.T) {
	// Routing resolution through a registry (no network).
	reg := NewRegistry()
	reg.Upsert(NewUpstream("api", "http://127.0.0.1:1", 1))
	reg.Upsert(NewUpstream("root", "http://127.0.0.1:2", 1))
	rt := NewRoutingTable([]Route{
		{Prefix: "/", BackendIDs: []string{"root"}},
		{Prefix: "/api/", BackendIDs: []string{"api"}},
	})
	got := rt.BackendsFor("/api/x", reg)
	if len(got) != 1 || got[0].ID != "api" {
		t.Fatalf("BackendsFor /api/x = %v want [api]", got)
	}
	req, _ := http.NewRequest("GET", "http://example.com/cached/item-1", nil)
	if k := cacheKey(req); k != "GET example.com/cached/item-1" {
		t.Errorf("cacheKey = %q", k)
	}
}
