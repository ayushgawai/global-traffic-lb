# global-traffic-lb

An L4/L7 load balancer and reverse proxy with response caching, plus a
self-service REST control plane and a benchmark load generator. One Go package,
standard library only (no external `go.mod` dependencies), Go 1.21+.

```bash
go run .            # starts demo backends + L7 :8080, control :8081, L4 :9000
go test ./...
```

## Design

```
                          control plane (separate port, e.g. :8081)
                          ┌───────────────────────────────────────┐
                          │  REST: /backends /routes /weights     │
                          │        /stats    /healthz             │
                          └──────────────┬────────────────────────┘
                                         │ upsert / drain / re-weight
                                         ▼
   HTTP/1.1 ──┐                   ┌─────────────┐   pick    ┌────────────┐
   h2c ───────┼─► sniffListener ─►│  L7 Proxy   ├──────────►│ Registry   │
   (prior     │   (server.go)     │  (server.go)│ scheduler │ backends   │
    knowledge)│                   └──────┬──────┘           │ + health   │
              │                          │ cache hit        │ + circuit  │
              │                   ┌──────▼──────┐           └─────┬──────┘
              │                   │ LRU+TTL+SWR │                 │ bounded
              │                   │ cache.go    │                 ▼ pool
              │                   └─────────────┘        upstream HTTP svcs
              │
   TCP ───────┴─► L4Balancer (l4.go): dial + READY-line health checks,
                  weighted-round-robin / least-connections, circuit
                  breaking, bidirectional io.Copy proxying ──► TCP backends

   loadgen (loadgen.go): N workers, cache-hit-ratio mix ──► LB ──► report
                          req/s, p50/p95/p99, cache hit rate (X-Cache header)
```

### Design table

| Component | File | Responsibility |
|---|---|---|
| L7 reverse proxy | `server.go` | HTTP/1.1 + prior-knowledge h2c serving (see Limitations), longest-prefix routing, bounded `http.Transport` pool (`MaxConnsPerHost`, idle caps), populates/serves cache, records success/failure to the circuit breaker |
| Routing table | `server.go` | Routes sorted by descending prefix length; `Match` = longest path prefix; `BackendsFor` resolves a path to route backends or all backends |
| L4 TCP balancer | `l4.go` | TCP accept loop, scheduler pick, bidirectional copy with half-close, active health checks (dial + read `READY\n` line) |
| Registry + upstreams | `l4.go` | Thread-safe backend set shared by L4/L7: weight, drained flag, healthy flag, active/total connection counters |
| Schedulers | `l4.go` | Smooth weighted round-robin (nginx algorithm, interleaved) and least-connections (normalised by weight: `active/weight`) |
| Circuit breaker | `l4.go` | Per-upstream: closed → open after N consecutive failures → half-open single probe after cooldown → closed on success / re-open on failure |
| Response cache | `cache.go` | Thread-safe LRU with per-entry TTL and stale-while-revalidate: stale entries are served while one single-flight background revalidation runs; stats (hits/stale/misses/evictions) |
| Control plane | `controlplane.go` | REST on its own port/mux: upsert/remove/drain backends, change weights and routes at runtime, list backends + health + cache stats |
| Load generator | `loadgen.go` | Configurable concurrency/duration/cache-hit-ratio request mix; reports req/s, p50/p95/p99 latency and cache hit rate from `X-Cache: HIT\|STALE` |
| Entry point / demo | `main.go` | Flags, three in-process demo HTTP backends (weights 1/2/3) + TCP echo backends speaking the ready-line protocol, graceful shutdown on SIGINT/SIGTERM |

## Run

```bash
# Demo mode (default): 3 weighted in-process backends, L7 :8080,
# control plane :8081, L4 TCP :9000
go run .

# Against your own upstreams
go run . -port 8080 -control-port 8081 -l4-port 9000 \
  -scheduler weighted-round-robin \
  -backends backend-1=http://127.0.0.1:9001,backend-2=http://127.0.0.1:9002

# Least-connections scheduling
go run . -scheduler least-connections

# Useful flags: -cache-capacity 256 -cache-ttl 5s -cache-swr 10s
```

Try it:

```bash
curl -i http://127.0.0.1:8080/cached/item-1   # X-Cache: MISS, then HIT
curl -i http://127.0.0.1:8080/api/hello       # routed by longest prefix
printf 'ping\n' | nc 127.0.0.1 9000           # L4 -> TCP echo backend
```

## Control plane

Separate port so control traffic never competes with proxied traffic.

```bash
# List backends + health + circuit state, cache stats
curl http://127.0.0.1:8081/stats
curl http://127.0.0.1:8081/backends

# Add / upsert a backend with weight
curl -X POST http://127.0.0.1:8081/backends \
  -d '{"id":"backend-4","addr":"http://127.0.0.1:9004","weight":2}'

# Drain / undrain, change weight, remove
curl -X POST http://127.0.0.1:8081/backends/backend-4/drain
curl -X POST http://127.0.0.1:8081/backends/backend-4/undrain
curl -X POST http://127.0.0.1:8081/weights -d '{"id":"backend-4","weight":5}'
curl -X DELETE http://127.0.0.1:8081/backends/backend-4

# Replace the routing table (longest prefix wins)
curl -X PUT http://127.0.0.1:8081/routes \
  -d '{"routes":[{"prefix":"/api/","backends":["backend-4"]},{"prefix":"/","backends":["backend-1"]}]}'
curl http://127.0.0.1:8081/healthz
```

## Test

```bash
go test ./...
go vet ./...
```

Table-driven tests cover: weighted round-robin fairness (equal, 2:1, 3:2:1,
5:1:1 proportions plus smooth interleaving `a b a`), least-connections
selection (including weight normalisation), LRU eviction + TTL expiry +
stale-while-revalidate serving + SWR single-flight, circuit breaker
closed/open/half-open/closed transitions with an injected clock, and routing
table longest-prefix selection.

## Benchmark / sanity runs

Real output, captured on Windows (go1.22.12, loopback, demo backends with
weights 1/2/3) against a server started with:

```bash
go run . -port 28080 -control-port 28081 -l4-port 29000
```

Load generator (also runnable standalone against any running LB):

```bash
go run . -loadgen -target http://127.0.0.1:28080 -concurrency 10 -duration 5s -hit-ratio 0.7
```

```
requests=39033 errors=0 req/s=6612.2 p50=499µs p95=1ms p99=1.499ms cache_hit_rate=69.9%
2xx=39033 cache_hits=27282 duration=5.903s
```

A smaller second run:

```bash
go run . -loadgen -target http://127.0.0.1:28080 -concurrency 5 -duration 2s -hit-ratio 0.7
```

```
requests=5669 errors=0 req/s=2830.6 p50=0s p95=1ms p99=80.08ms cache_hit_rate=69.8%
2xx=5669 cache_hits=3955 duration=2.003s
```

Read honestly: this is a tiny loopback benchmark — three in-process demo
backends returning a few bytes of JSON on the same machine, so the numbers
mostly measure loopback + Go HTTP overhead, not a production topology. The
cache hit rate tracks the requested 0.7 hit ratio because 70% of requests aim
at the 8-key `/cached/item-*` working set. p50 printing as `0s` in the second
run is sub-millisecond timer/rounding granularity in the percentile output,
not zero-cost requests. Scale was kept small (a handful of backends, seconds
of load); there is no cross-machine, TLS, or large-payload measurement here.

## Limitations

- **No real QUIC / HTTP/3.** Not implemented at all (and not possible with the
  standard library only).
- **No TLS terminator.** No HTTPS listener, no certificate management; put a
  TLS-terminating proxy in front for production-shaped use.
- **h2c is a minimal subset, not full HTTP/2.** `golang.org/x/net/http2/h2c`
  is an external module and excluded by the stdlib-only constraint, so
  `server.go` sniffs the HTTP/2 client preface and speaks a small framing /
  HPACK subset itself: SETTINGS exchange and one simple GET per connection,
  no Huffman-coded strings, no flow-control tuning, no multiplexed streams.
  HTTP/1.1 proxying (the default path via `net/http`) is the fully supported
  mode; treat h2c as a demonstration of prior-knowledge serving.
- **L4 is in-process / toy scale.** Demo TCP backends, the balancer and the
  load generator all run in one process on loopback by default; health checks
  require backends to speak the `READY\n` ready-line protocol, so arbitrary
  TCP services that stay silent on connect will be marked unhealthy.
- **Cache correctness is deliberately simple:** GET + 200 only, keyed by
  `method host URI`; it ignores `Cache-Control`, `Vary`, cookies and
  authorisation headers, so it must not front personalised or authenticated
  responses. Background SWR revalidation re-issues the triggering request
  shape; POST/other methods are never cached.
- **Other simplifications:** no persistence (control-plane changes are lost
  on restart), no authentication on the control plane (do not expose it
  publicly), circuit-breaker/health state is per-process, and the
  smooth-WRR scheduler mutates per-upstream counters under per-upstream locks
  rather than a lock-free design.
- Unfinished / rough edges are called out above rather than hidden; the test
  suite and sanity runs in this README are the real, current state of the
  program.

## License

MIT — see `LICENSE`.
