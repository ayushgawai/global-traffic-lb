package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	var (
		port        = flag.Int("port", 8080, "L7 proxy port")
		l4Port      = flag.Int("l4-port", 9000, "L4 TCP port (0 disables)")
		controlPort = flag.Int("control-port", 8081, "control plane port")
		scheduler   = flag.String("scheduler", "weighted-round-robin", "weighted-round-robin|least-connections")
		backends    = flag.String("backends", "", "comma list id=http://host:port,...")
		demo        = flag.Bool("demo", true, "run in-process demo backends when -backends is empty")
		cacheCap    = flag.Int("cache-capacity", 256, "cache entries")
		cacheTTL    = flag.Duration("cache-ttl", 5*time.Second, "cache TTL")
		cacheSWR    = flag.Duration("cache-swr", 10*time.Second, "stale-while-revalidate window")
		loadgen     = flag.Bool("loadgen", false, "run load generator against -target and exit")
		target      = flag.String("target", "http://127.0.0.1:8080", "loadgen target")
		conc        = flag.Int("concurrency", 10, "loadgen concurrency")
		dur         = flag.Duration("duration", 5*time.Second, "loadgen duration")
		hitRatio    = flag.Float64("hit-ratio", 0.7, "loadgen cache-hit ratio 0..1")
		maxReq      = flag.Int64("max-requests", 0, "loadgen cap on total requests (0 = duration only)")
	)
	flag.Parse()

	if *loadgen {
		res, err := RunLoadGen(context.Background(), LoadGenConfig{
			Target: *target, Concurrency: *conc, Duration: *dur, CacheHitRatio: *hitRatio, Seed: 42, MaxRequests: *maxReq,
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(res.String())
		fmt.Printf("2xx=%d cache_hits=%d duration=%s\n", res.Status2xx, res.CacheHits, res.Duration.Round(time.Millisecond))
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := NewRegistry()
	l4Reg := NewRegistry()
	routes := NewRoutingTable([]Route{
		{Prefix: "/api/", BackendIDs: nil}, // nil = all backends
		{Prefix: "/cached/", BackendIDs: nil},
		{Prefix: "/", BackendIDs: nil},
	})
	cache := NewCache(*cacheCap, *cacheTTL, *cacheSWR)
	sched := SchedulerByName(*scheduler)

	if *backends != "" {
		for _, part := range strings.Split(*backends, ",") {
			kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
			if len(kv) == 2 {
				reg.Upsert(NewUpstream(kv[0], kv[1], 1))
			}
		}
	} else if *demo {
		startDemoBackends(ctx, reg, l4Reg)
	}

	proxy := NewProxy(reg, routes, cache, sched)
	cp := NewControlPlane(reg, routes, cache, sched)

	go func() { log.Fatal(cp.ListenAndServe(fmt.Sprintf(":%d", *controlPort))) }()
	if *l4Port > 0 {
		l4 := NewL4Balancer(l4Reg, sched)
		go func() {
			if err := l4.ListenAndServe(ctx, fmt.Sprintf(":%d", *l4Port)); err != nil {
				log.Printf("l4: %v", err)
			}
		}()
	}
	log.Printf("global-traffic-lb: L7 :%d control :%d scheduler=%s backends=%d", *port, *controlPort, sched.Name(), len(reg.List()))
	if err := ListenAndServeProxy(ctx, fmt.Sprintf(":%d", *port), proxy); err != nil {
		log.Fatal(err)
	}
}

// startDemoBackends launches 3 tiny HTTP upstreams on ephemeral ports and a
// set of TCP echo services that speak the health-check ready-line protocol.
func startDemoBackends(ctx context.Context, reg *Registry, l4Reg *Registry) {
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("backend-%d", i)
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"backend":%q,"path":%q}`, id, r.URL.Path)
		})
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatal(err)
		}
		srv := &http.Server{Handler: mux}
		go srv.Serve(ln)
		go func() { <-ctx.Done(); srv.Close() }()
		reg.Upsert(NewUpstream(id, "http://"+ln.Addr().String(), i)) // weights 1,2,3
		log.Printf("demo backend %s at %s weight=%d", id, ln.Addr(), i)
		startDemoTCPBackend(ctx, l4Reg, id)
	}
}

// startDemoTCPBackend starts a TCP service that sends "READY\n" on connect
// (the L4 health-check protocol) and then echoes bytes back.
func startDemoTCPBackend(ctx context.Context, reg *Registry, id string) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return
	}
	reg.Upsert(NewUpstream(id+"-tcp", ln.Addr().String(), 1))
	go func() { <-ctx.Done(); ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				fmt.Fprint(c, "READY\n")
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						c.Write(buf[:n])
					}
					if err != nil {
						return
					}
				}
			}(c)
		}
	}()
	_ = ln.Addr()
}
