# global-traffic-lb

An L4/L7 load balancer and reverse proxy with response caching, plus a
self-service REST control plane and a benchmark load generator. Idiomatic Go
(1.21+), **standard library only** — no external `go.mod` dependencies — and it
runs with a single `go run .`.

## Design

```
                        ┌─────────────────────────────────────────────┐
     HTTP/1.1 / h2c     │                L7 proxy (:8080)             │
  clients ─────────────►│  sniffListener ──► Proxy.ServeHTTP          │
                        │     │                │                      │
                        │     │ h2c preface    ├─► Cache (LRU+TTL+SWR)│
                        │     ▼                │    HIT/STALE served  │
                        │  serveH2C            │    locally, SWR      │
                        │  (minimal HTTP/2)    │    revalidates async │
                        │                      ▼                      │
                        │               RoutingTable (longest prefix) │
                        │                      │                      │
                        │               Scheduler: smooth WRR /       │
                        │               least-connections             │
                        │                      │   CircuitBreaker     │
                        └──────────────────────┼──────────────────────┘
                                               ▼
                                  upstreams backend-1..N (HTTP)

     TCP clients ───────► L4 balancer (:9000) ──► TCP backends
                               │  WRR / least-conns, active health checks
                               │  (dial + "READY\n" line), circuit breaking

     operators ─────────► Control plane (:8081, separate mux/port)
                               GET/POST/DELETE /backends, /drain, /weights,
                               /routes, /stats (health + cache)

     loadgen ───────────► hits the L7 port with a configurable mix and
                          reports req/s, p50/p95/p99, cache hit rate
```

| Component | File | Responsibility |
|---|---|---|
| L7 reverse proxy | `server.go` | Path-prefix routing table (longest-prefix match), proxying via `httputil.ReverseProxy` with a bounded `http.Transport` pool (`MaxConnsPerHost=50`, `MaxIdleConnsPerHost=20`), `X-Backend`/`X-Cache` headers, h2c prior-knowledge detection + minimal HTTP/2 framing/HPACK handling |
| L4 TCP load balancer | `l4.go` | TCP proxying with smooth weighted round-robin and least-connections (active/weight) scheduling, active health checks (dial + read a ready line), per-upstream circuit breaker (open after N failures, half-open single probe after cooldown) |
| Response cache | `cache.go` | Thread-safe LRU (`container/list`) with per-entry TTL and stale-while-revalidate: stale entries are served while exactly one background revalidation per key is in flight (single-flight map) |
| Control plane | `controlplane.go` | REST API on its own port/mux: upsert/list/remove/drain backends, change weights at runtime, replace routes, view backend health + cache stats |
| Load generator | `loadgen.go` | Concurrent workers with a configurable cache-hit ratio (small `/cached/item-{0..7}` working set vs unique paths), concurrency, duration and request cap; reports req/s, p50/p95/p99 and cache hit rate (from `X-Cache`) |
| Entry point / demo backends | `main.go` | Flags, wiring, and in-process demo HTTP + TCP backends so `go run .` works with zero setup |
| Tests | `lb_test.go` | Table tests for WRR fairness/interleaving, least-connections, LRU TTL+SWR, circuit-breaker transitions, longest-prefix routing |

## Run

```sh
go run .                                   # demo: L7 :8080, control :8081, L4 :9000
go run . -port 8080 -control-port 8081 -l4-port 9000 \
         -scheduler weighted-round-robin   # or least-connections
go run . -backends b1=http://127.0.0.1:9001,b2=http://127.0.0.1:9002  # your own upstreams
```

Try it:

```sh
curl -i http://127.0.0.1:8080/cached/item-1   # X-Cache: MISS, then HIT on repeat
curl http://127.0.0.1:8081/stats              # backends, health, cache stats
curl -X POST http://127.0.0.1:8081/backends -d '{"id":"b4","addr":"http://127.0.0.1:9004","weight":2}'
curl -X POST http://127.0.0.1:8081/backends/b4/drain
curl -X POST http://127.0.0.1:8081/weights  -d '{"id":"b1","weight":5}'
curl -X DELETE http://127.0.0.1:8081/backends/b4
```

## Test

```sh
go test ./... -count=1 -v
```

Real output (this machine, Go 1.22, Windows):

```
--- PASS: TestWeightedRoundRobinFairness (0.00s)   (equal, 2-1, 3-2-1, 5-1-1)
--- PASS: TestWeightedRoundRobinInterleaving (0.00s)
--- PASS: TestLeastConnectionsSelection (0.00s)
--- PASS: TestCacheLRUTTLAndSWR (0.00s)
--- PASS: TestCircuitBreakerTransitions (0.00s)
--- PASS: TestRoutingTableLongestPrefix (0.00s)
--- PASS: TestCacheKeyAndProxyRoutingSmoke (0.00s)
PASS
ok  	global-traffic-lb	0.247s
```

## Benchmark / sanity runs

Loadgen in a second terminal against the demo deployment (3 in-process
backends, weights 1/2/3). Scaled down honestly — a few hundred requests:

```sh
go run . -loadgen -target http://127.0.0.1:18080 \
  -concurrency 10 -duration 10s -hit-ratio 0.7 -max-requests 500
```

Real captured output:

```
requests=500 errors=0 req/s=14482.7 p50=499µs p95=1.999ms p99=10.615ms cache_hit_rate=66.6%
2xx=500 cache_hits=333 duration=35ms
```

A longer uncapped run on the same setup, for reference:

```sh
go run . -loadgen -target http://127.0.0.1:18080 -concurrency 10 -duration 5s -hit-ratio 0.7
```
```
requests=137318 errors=0 req/s=27462.7 p50=499µs p95=1ms p99=1.498ms cache_hit_rate=70.0%
2xx=137318 cache_hits=96146 duration=5s
```

These numbers are **localhost, in-process demo backends serving tiny JSON
bodies** — they measure loopback + cache speed, not production throughput.
The interesting sanity signals are `errors=0` and the cache hit rate tracking
the requested 0.7 mix (66.6% on the small run, 70.0% on the long run).

## Limitations

- **No real QUIC / HTTP/3**, and no TLS terminator — there is no TLS at all.
- **h2c is minimal, not a full HTTP/2 implementation.** Because
  `golang.org/x/net/http2/h2c` is an external module and this project is
  stdlib-only, `server.go` hand-rolls the h2c prior-knowledge handshake: it
  detects the client preface, exchanges SETTINGS, and answers simple GETs on
  a single stream with a small HPACK decoder that does **not** support
  Huffman-coded strings, multiplexing, flow control or server push. Clients
  that Huffman-encode header values (many do) will not get far; HTTP/1.1 is
  the fully supported path. On Go 1.24+ the stdlib `http.Server.Protocols`
  API would replace this cleanly.
- **L4 is in-process for the demo**: the demo TCP backends live in the same
  process; the balancer itself dials arbitrary external `host:port` backends
  fine, but nothing here is a separate daemon, and L4 health requires
  backends that send a `READY\n` line on connect.
- Cache is per-process, in-memory only; no persistence, no shared/distributed
  cache, `Cache-Control` from upstreams is not honoured (TTL is global /
  per-`Set` only), and only `GET` 200 responses are cached.
- Single node, no config file, no metrics endpoint beyond `/stats`, no
  graceful drain other than the control-plane `drain` flag, and control plane
  has no auth — do not expose it publicly.
- Loadgen measures latency client-side on loopback by default; it is a
  sanity/benchmark tool, not a replacement for a dedicated load tester.

MIT licensed — see `LICENSE`.
