package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Circuit breaker
// ---------------------------------------------------------------------------

type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

// CircuitBreaker opens after Threshold consecutive failures, then after
// Cooldown allows a single half-open probe. Success closes it, failure re-opens.
type CircuitBreaker struct {
	mu        sync.Mutex
	Threshold int
	Cooldown  time.Duration
	failures  int
	state     CircuitState
	openedAt  time.Time
	probing   bool
	now       func() time.Time
}

func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	if threshold <= 0 {
		threshold = 3
	}
	return &CircuitBreaker{Threshold: threshold, Cooldown: cooldown, now: time.Now}
}

func (cb *CircuitBreaker) SetNow(f func() time.Time) { cb.mu.Lock(); defer cb.mu.Unlock(); cb.now = f }

func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transitionLocked()
	return cb.state
}

func (cb *CircuitBreaker) transitionLocked() {
	if cb.state == CircuitOpen && cb.now().Sub(cb.openedAt) >= cb.Cooldown {
		cb.state = CircuitHalfOpen
		cb.probing = false
	}
}

// Allow reports whether a request may proceed.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transitionLocked()
	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitHalfOpen:
		if cb.probing {
			return false
		}
		cb.probing = true
		return true
	default:
		return false
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.probing = false
	cb.state = CircuitClosed
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.probing = false
	if cb.state == CircuitHalfOpen || cb.failures >= cb.Threshold {
		cb.state = CircuitOpen
		cb.openedAt = cb.now()
	}
}

func (cb *CircuitBreaker) Failures() int { cb.mu.Lock(); defer cb.mu.Unlock(); return cb.failures }

// ---------------------------------------------------------------------------
// Upstream & registry (shared by L4 and L7)
// ---------------------------------------------------------------------------

type Upstream struct {
	ID      string `json:"id"`
	Addr    string `json:"addr"` // host:port for TCP, http://host:port for HTTP
	Weight  int    `json:"weight"`
	Drained bool   `json:"drained"`

	Healthy     atomic.Bool
	ActiveConns atomic.Int64
	Circuit     *CircuitBreaker `json:"-"`

	mu         sync.Mutex
	currentWRR int // smooth weighted-round-robin current weight
	LastCheck  time.Time
	LastError  string
	totalConns atomic.Int64
}

func NewUpstream(id, addr string, weight int) *Upstream {
	if weight <= 0 {
		weight = 1
	}
	u := &Upstream{ID: id, Addr: addr, Weight: weight, Circuit: NewCircuitBreaker(3, 5*time.Second)}
	u.Healthy.Store(true)
	return u
}

func (u *Upstream) Available() bool {
	return u.Healthy.Load() && !u.Drained && u.Circuit.Allow()
}

// Snapshot is the JSON view used by the control plane.
type UpstreamSnapshot struct {
	ID          string `json:"id"`
	Addr        string `json:"addr"`
	Weight      int    `json:"weight"`
	Drained     bool   `json:"drained"`
	Healthy     bool   `json:"healthy"`
	ActiveConns int64  `json:"active_conns"`
	TotalConns  int64  `json:"total_conns"`
	Circuit     string `json:"circuit"`
	Failures    int    `json:"failures"`
	LastError   string `json:"last_error,omitempty"`
}

func (u *Upstream) Snapshot() UpstreamSnapshot {
	u.mu.Lock()
	lastErr := u.LastError
	u.mu.Unlock()
	return UpstreamSnapshot{
		ID: u.ID, Addr: u.Addr, Weight: u.Weight, Drained: u.Drained,
		Healthy: u.Healthy.Load(), ActiveConns: u.ActiveConns.Load(),
		TotalConns: u.totalConns.Load(), Circuit: u.Circuit.State().String(),
		Failures: u.Circuit.Failures(), LastError: lastErr,
	}
}

// Registry is a thread-safe set of upstreams.
type Registry struct {
	mu    sync.RWMutex
	order []string
	items map[string]*Upstream
}

func NewRegistry() *Registry { return &Registry{items: make(map[string]*Upstream)} }

func (r *Registry) Upsert(u *Upstream) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[u.ID]; !ok {
		r.order = append(r.order, u.ID)
	} else {
		// preserve circuit/health of the existing entry on weight-only updates is
		// the caller's job; here we replace the pointer's mutable fields.
		old := r.items[u.ID]
		old.Addr, old.Weight = u.Addr, u.Weight
		return
	}
	r.items[u.ID] = u
}

func (r *Registry) Get(id string) (*Upstream, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.items[id]
	return u, ok
}

func (r *Registry) Remove(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.items[id]; !ok {
		return false
	}
	delete(r.items, id)
	for i, v := range r.order {
		if v == id {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	return true
}

func (r *Registry) List() []*Upstream {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Upstream, 0, len(r.order))
	for _, id := range r.order {
		out = append(out, r.items[id])
	}
	return out
}

// Eligible returns upstreams that may receive traffic right now.
// Note: Circuit.Allow() has a half-open side effect (claims the probe), so the
// caller should only call Eligible when it is about to pick one.
func eligible(list []*Upstream) []*Upstream {
	out := make([]*Upstream, 0, len(list))
	for _, u := range list {
		if u.Healthy.Load() && !u.Drained && u.Circuit.State() != CircuitOpen {
			out = append(out, u)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Scheduling
// ---------------------------------------------------------------------------

type Scheduler interface {
	Pick(candidates []*Upstream) *Upstream
	Name() string
}

// WeightedRoundRobin implements smooth WRR (the nginx algorithm): each pick
// adds Weight to every candidate's current weight, selects the max, then
// subtracts the total weight from the winner. This yields interleaved,
// proportionally fair sequences (e.g. weights 2:1 -> a b a, not a a b).
type WeightedRoundRobin struct{}

func (WeightedRoundRobin) Name() string { return "weighted-round-robin" }
func (WeightedRoundRobin) Pick(candidates []*Upstream) *Upstream {
	var best *Upstream
	total := 0
	for _, u := range candidates {
		total += u.Weight
		u.mu.Lock()
		u.currentWRR += u.Weight
		cur := u.currentWRR
		u.mu.Unlock()
		if best == nil {
			best = u
			continue
		}
		best.mu.Lock()
		bestCur := best.currentWRR
		best.mu.Unlock()
		if cur > bestCur {
			best = u
		}
	}
	if best != nil {
		best.mu.Lock()
		best.currentWRR -= total
		best.mu.Unlock()
	}
	return best
}

// LeastConnections picks the upstream with the fewest active connections,
// breaking ties by weight (higher weight wins ties), then by order.
type LeastConnections struct{}

func (LeastConnections) Name() string { return "least-connections" }
func (LeastConnections) Pick(candidates []*Upstream) *Upstream {
	var best *Upstream
	for _, u := range candidates {
		if best == nil {
			best = u
			continue
		}
		// Compare load ratio active/weight so heavier backends take more.
		uScore := float64(u.ActiveConns.Load()) / float64(maxW(u.Weight))
		bScore := float64(best.ActiveConns.Load()) / float64(maxW(best.Weight))
		if uScore < bScore {
			best = u
		}
	}
	return best
}

func maxW(w int) int {
	if w <= 0 {
		return 1
	}
	return w
}

func SchedulerByName(name string) Scheduler {
	if name == "least-connections" || name == "leastconn" {
		return LeastConnections{}
	}
	return WeightedRoundRobin{}
}

// ---------------------------------------------------------------------------
// L4 TCP load balancer
// ---------------------------------------------------------------------------

type L4Balancer struct {
	reg            *Registry
	scheduler      Scheduler
	listener       net.Listener
	HealthInterval time.Duration
	DialTimeout    time.Duration
}

func NewL4Balancer(reg *Registry, scheduler Scheduler) *L4Balancer {
	return &L4Balancer{reg: reg, scheduler: scheduler, HealthInterval: 2 * time.Second, DialTimeout: 3 * time.Second}
}

func (lb *L4Balancer) ListenAndServe(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	lb.listener = ln
	log.Printf("l4: listening on %s scheduler=%s", addr, lb.scheduler.Name())
	go lb.healthLoop(ctx)
	go func() { <-ctx.Done(); ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go lb.handle(conn)
	}
}

// Pick chooses an upstream for a new L4 connection (exported for tests).
func (lb *L4Balancer) Pick() *Upstream {
	cands := eligible(lb.reg.List())
	if len(cands) == 0 {
		return nil
	}
	return lb.scheduler.Pick(cands)
}

func (lb *L4Balancer) handle(client net.Conn) {
	defer client.Close()
	up := lb.Pick()
	if up == nil {
		return
	}
	// Claim the half-open probe if applicable.
	if up.Circuit.State() == CircuitHalfOpen && !up.Circuit.Allow() {
		return
	}
	up.ActiveConns.Add(1)
	up.totalConns.Add(1)
	defer up.ActiveConns.Add(-1)

	backend, err := net.DialTimeout("tcp", up.Addr, lb.DialTimeout)
	if err != nil {
		up.Circuit.RecordFailure()
		up.mu.Lock()
		up.LastError = err.Error()
		up.mu.Unlock()
		return
	}
	defer backend.Close()
	up.Circuit.RecordSuccess()

	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if tc, ok := dst.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
		done <- struct{}{}
	}
	go cp(backend, client)
	go cp(client, backend)
	<-done
}

// healthLoop actively health-checks TCP backends: dial and read a ready line.
func (lb *L4Balancer) healthLoop(ctx context.Context) {
	t := time.NewTicker(lb.HealthInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, u := range lb.reg.List() {
				go lb.checkTCP(u)
			}
		}
	}
}

// CheckTCPNow performs one synchronous health check (used by tests too).
func (lb *L4Balancer) CheckTCPNow(u *Upstream) bool { return lb.checkTCP(u) }

func (lb *L4Balancer) checkTCP(u *Upstream) bool {
	conn, err := net.DialTimeout("tcp", stripScheme(u.Addr), 1500*time.Millisecond)
	if err != nil {
		u.Healthy.Store(false)
		u.Circuit.RecordFailure()
		u.mu.Lock()
		u.LastError = err.Error()
		u.LastCheck = time.Now()
		u.mu.Unlock()
		return false
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
	line, err := bufio.NewReader(conn).ReadString('\n')
	u.mu.Lock()
	u.LastCheck = time.Now()
	if err != nil && line == "" {
		u.LastError = fmt.Sprintf("health check read: %v", err)
	} else {
		u.LastError = ""
	}
	u.mu.Unlock()
	if err != nil && line == "" {
		// Listener that accepts but never speaks is considered unhealthy only
		// for our demo protocol; a bare accept still proves reachability, so
		// require the ready line strictly:
		u.Healthy.Store(false)
		u.Circuit.RecordFailure()
		return false
	}
	u.Healthy.Store(true)
	u.Circuit.RecordSuccess()
	return true
}

func stripScheme(addr string) string {
	for _, p := range []string{"http://", "https://", "tcp://"} {
		if len(addr) > len(p) && addr[:len(p)] == p {
			return addr[len(p):]
		}
	}
	return addr
}
