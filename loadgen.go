package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LoadGenConfig configures the benchmark load generator.
type LoadGenConfig struct {
	Target        string // e.g. http://127.0.0.1:8080
	Concurrency   int
	Duration      time.Duration
	CacheHitRatio float64 // 0..1 fraction of requests aimed at a small cacheable set
	Seed          int64
	MaxRequests   int64 // >0 caps total requests (whichever of duration/cap hits first)
}

type LoadResult struct {
	Requests    int64         `json:"requests"`
	Errors      int64         `json:"errors"`
	Status2xx   int64         `json:"status_2xx"`
	CacheHits   int64         `json:"cache_hits"` // X-Cache: HIT or STALE
	Duration    time.Duration `json:"duration"`
	ReqPerSec   float64       `json:"req_per_sec"`
	P50         time.Duration `json:"p50"`
	P95         time.Duration `json:"p95"`
	P99         time.Duration `json:"p99"`
	CacheHitPct float64       `json:"cache_hit_rate_pct"`
}

func (r LoadResult) String() string {
	return fmt.Sprintf("requests=%d errors=%d req/s=%.1f p50=%s p95=%s p99=%s cache_hit_rate=%.1f%%",
		r.Requests, r.Errors, r.ReqPerSec, r.P50.Round(time.Microsecond), r.P95.Round(time.Microsecond), r.P99.Round(time.Microsecond), r.CacheHitPct)
}

// Percentile returns the p-th percentile (0..100) of durations.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p / 100 * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}

// RunLoadGen drives load at Target for Duration with Concurrency workers.
// Cacheable requests cycle over /cached/item-{0..7} (small working set, so the
// LB cache can hit); non-cacheable requests use a unique /item/{n} path each.
func RunLoadGen(ctx context.Context, cfg LoadGenConfig) (LoadResult, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 5 * time.Second
	}
	client := &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: cfg.Concurrency * 2, MaxConnsPerHost: cfg.Concurrency * 2},
		Timeout:   5 * time.Second,
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	var reqs, errs, ok2xx, hits atomic.Int64
	var mu sync.Mutex
	var lat []time.Duration
	var counter atomic.Int64

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(worker)*7919))
			for ctx.Err() == nil {
				n := counter.Add(1)
				if cfg.MaxRequests > 0 && n > cfg.MaxRequests {
					return
				}
				var path string
				if rng.Float64() < cfg.CacheHitRatio {
					path = fmt.Sprintf("/cached/item-%d", rng.Intn(8))
				} else {
					path = fmt.Sprintf("/item/%d", n)
				}
				t0 := time.Now()
				resp, err := client.Get(cfg.Target + path)
				elapsed := time.Since(t0)
				reqs.Add(1)
				mu.Lock()
				lat = append(lat, elapsed)
				mu.Unlock()
				if err != nil {
					errs.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					ok2xx.Add(1)
				} else {
					errs.Add(1)
				}
				if xc := resp.Header.Get("X-Cache"); xc == "HIT" || xc == "STALE" {
					hits.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	total := reqs.Load()
	res := LoadResult{
		Requests: total, Errors: errs.Load(), Status2xx: ok2xx.Load(), CacheHits: hits.Load(),
		Duration: elapsed, P50: Percentile(lat, 50), P95: Percentile(lat, 95), P99: Percentile(lat, 99),
	}
	if elapsed.Seconds() > 0 {
		res.ReqPerSec = float64(total) / elapsed.Seconds()
	}
	if total > 0 {
		res.CacheHitPct = 100 * float64(hits.Load()) / float64(total)
	}
	return res, nil
}
