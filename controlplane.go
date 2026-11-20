package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

// ControlPlane is a self-service REST API on its own port/mux, separate from
// the data plane so control traffic never competes with proxied traffic.
type ControlPlane struct {
	reg       *Registry
	routes    *RoutingTable
	cache     *Cache
	scheduler Scheduler
	started   time.Time
}

func NewControlPlane(reg *Registry, routes *RoutingTable, cache *Cache, sched Scheduler) *ControlPlane {
	return &ControlPlane{reg: reg, routes: routes, cache: cache, scheduler: sched, started: time.Now()}
}

func (cp *ControlPlane) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"status": "ok", "uptime_s": int(time.Since(cp.started).Seconds())})
	})
	mux.HandleFunc("/backends", cp.handleBackends) // GET list, POST upsert
	mux.HandleFunc("/backends/", cp.handleBackend) // DELETE /backends/{id}, POST /backends/{id}/drain, PUT .../weight
	mux.HandleFunc("/routes", cp.handleRoutes)     // GET list, PUT replace
	mux.HandleFunc("/weights", cp.handleWeights)   // POST {id,weight}
	mux.HandleFunc("/stats", cp.handleStats)       // GET backends+health+cache
	return mux
}

func (cp *ControlPlane) handleBackends(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, 200, map[string]any{"backends": snapshots(cp.reg.List())})
	case http.MethodPost:
		var body struct {
			ID     string `json:"id"`
			Addr   string `json:"addr"`
			Weight int    `json:"weight"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ID == "" || body.Addr == "" {
			writeJSON(w, 400, map[string]any{"error": "id and addr required"})
			return
		}
		if body.Weight <= 0 {
			body.Weight = 1
		}
		cp.reg.Upsert(NewUpstream(body.ID, body.Addr, body.Weight))
		u, _ := cp.reg.Get(body.ID)
		writeJSON(w, 201, u.Snapshot())
	default:
		writeJSON(w, 405, map[string]any{"error": "method not allowed"})
	}
}

func (cp *ControlPlane) handleBackend(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/backends/")
	parts := strings.Split(rest, "/")
	id := parts[0]
	u, ok := cp.reg.Get(id)
	if !ok {
		writeJSON(w, 404, map[string]any{"error": "backend not found"})
		return
	}
	if len(parts) == 2 && parts[1] == "drain" && r.Method == http.MethodPost {
		u.Drained = true
		writeJSON(w, 200, u.Snapshot())
		return
	}
	if len(parts) == 2 && parts[1] == "undrain" && r.Method == http.MethodPost {
		u.Drained = false
		writeJSON(w, 200, u.Snapshot())
		return
	}
	if len(parts) == 2 && parts[1] == "weight" && (r.Method == http.MethodPut || r.Method == http.MethodPost) {
		var body struct {
			Weight int `json:"weight"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Weight <= 0 {
			writeJSON(w, 400, map[string]any{"error": "positive weight required"})
			return
		}
		u.Weight = body.Weight
		writeJSON(w, 200, u.Snapshot())
		return
	}
	if len(parts) == 1 && r.Method == http.MethodDelete {
		cp.reg.Remove(id)
		writeJSON(w, 200, map[string]any{"removed": id})
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		writeJSON(w, 200, u.Snapshot())
		return
	}
	writeJSON(w, 405, map[string]any{"error": "method not allowed"})
}

func (cp *ControlPlane) handleRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, 200, map[string]any{"routes": cp.routes.Routes()})
	case http.MethodPut, http.MethodPost:
		var body struct {
			Routes []Route `json:"routes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, 400, map[string]any{"error": "invalid routes"})
			return
		}
		cp.routes.Replace(body.Routes)
		writeJSON(w, 200, map[string]any{"routes": cp.routes.Routes()})
	default:
		writeJSON(w, 405, map[string]any{"error": "method not allowed"})
	}
}

// handleWeights changes routing weights at runtime: POST {"id":"b1","weight":5}
func (cp *ControlPlane) handleWeights(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		writeJSON(w, 405, map[string]any{"error": "method not allowed"})
		return
	}
	var body struct {
		ID     string `json:"id"`
		Weight int    `json:"weight"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Weight <= 0 {
		writeJSON(w, 400, map[string]any{"error": "id and positive weight required"})
		return
	}
	u, ok := cp.reg.Get(body.ID)
	if !ok {
		writeJSON(w, 404, map[string]any{"error": "backend not found"})
		return
	}
	u.Weight = body.Weight
	writeJSON(w, 200, u.Snapshot())
}

func (cp *ControlPlane) handleStats(w http.ResponseWriter, r *http.Request) {
	cs := cp.cacheStats()
	writeJSON(w, 200, map[string]any{
		"backends":       snapshots(cp.reg.List()),
		"routes":         cp.routes.Routes(),
		"scheduler":      cp.scheduler.Name(),
		"cache":          cs,
		"cache_hit_rate": cs.HitRate(),
		"uptime_s":       int(time.Since(cp.started).Seconds()),
	})
}

func (cp *ControlPlane) cacheStats() CacheStats {
	if cp.cache == nil {
		return CacheStats{}
	}
	return cp.cache.Stats()
}

func snapshots(list []*Upstream) []UpstreamSnapshot {
	out := make([]UpstreamSnapshot, 0, len(list))
	for _, u := range list {
		out = append(out, u.Snapshot())
	}
	return out
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (cp *ControlPlane) ListenAndServe(addr string) error {
	log.Printf("control: listening on %s", addr)
	srv := &http.Server{Addr: addr, Handler: cp.Handler(), ReadHeaderTimeout: 5 * time.Second}
	return srv.ListenAndServe()
}
