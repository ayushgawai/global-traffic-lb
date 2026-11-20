package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Routing table: longest path-prefix selection
// ---------------------------------------------------------------------------

type Route struct {
	Prefix     string   `json:"prefix"`
	BackendIDs []string `json:"backends"`
}

type RoutingTable struct {
	routes []Route
}

func NewRoutingTable(routes []Route) *RoutingTable {
	rt := &RoutingTable{}
	rt.Replace(routes)
	return rt
}

// Replace swaps the table; routes are kept sorted by descending prefix length
// so Match is a longest-prefix scan.
func (rt *RoutingTable) Replace(routes []Route) {
	cp := append([]Route(nil), routes...)
	sort.SliceStable(cp, func(i, j int) bool { return len(cp[i].Prefix) > len(cp[j].Prefix) })
	rt.routes = cp
}

func (rt *RoutingTable) Routes() []Route { return append([]Route(nil), rt.routes...) }

// Match returns the route with the longest prefix matching path, or nil.
func (rt *RoutingTable) Match(path string) *Route {
	for i := range rt.routes {
		if strings.HasPrefix(path, rt.routes[i].Prefix) {
			return &rt.routes[i]
		}
	}
	return nil
}

// BackendsFor resolves a path to candidate upstreams: the matched route's
// backends, or all registry backends when no route matches (default route).
func (rt *RoutingTable) BackendsFor(path string, reg *Registry) []*Upstream {
	if rt != nil {
		if r := rt.Match(path); r != nil && len(r.BackendIDs) > 0 {
			out := make([]*Upstream, 0, len(r.BackendIDs))
			for _, id := range r.BackendIDs {
				if u, ok := reg.Get(id); ok {
					out = append(out, u)
				}
			}
			if len(out) > 0 {
				return out
			}
		}
	}
	return reg.List()
}

// ---------------------------------------------------------------------------
// L7 reverse proxy
// ---------------------------------------------------------------------------

type Proxy struct {
	reg       *Registry
	routes    *RoutingTable
	cache     *Cache
	scheduler Scheduler
	transport *http.Transport
}

func NewProxy(reg *Registry, routes *RoutingTable, cache *Cache, scheduler Scheduler) *Proxy {
	tr := &http.Transport{
		Proxy:       nil,
		DialContext: (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		// Bounded connection pool.
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		MaxConnsPerHost:     50,
		IdleConnTimeout:     30 * time.Second,
	}
	return &Proxy{reg: reg, routes: routes, cache: cache, scheduler: scheduler, transport: tr}
}

func cacheKey(r *http.Request) string {
	return r.Method + " " + r.Host + r.URL.RequestURI()
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && p.cache != nil {
		if entry, res := p.cache.Get(cacheKey(r)); res != LookupMiss {
			writeCached(w, entry, res.String())
			if res == LookupStale && p.cache.TryStartRevalidation(cacheKey(r)) {
				go func() {
					defer p.cache.FinishRevalidation(cacheKey(r))
					_, _ = p.fetch(r.Clone(context.Background()))
				}()
			}
			return
		}
	}
	// Cache miss: proxy through, writing to the real client. ModifyResponse
	// in fetchAndWrite populates the cache for successful GETs.
	_, _ = p.fetchAndWrite(w, r)
}

// fetch revalidates in the background, discarding the client-facing body;
// fetchAndWrite's ModifyResponse refreshes the cache entry.
func (p *Proxy) fetch(r *http.Request) (int, error) {
	rec := &discardRecorder{header: make(http.Header)}
	return p.fetchAndWrite(rec, r)
}

type discardRecorder struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (d *discardRecorder) Header() http.Header  { return d.header }
func (d *discardRecorder) WriteHeader(code int) { d.status = code }
func (d *discardRecorder) Write(b []byte) (int, error) {
	if d.status == 0 {
		d.status = 200
	}
	return d.body.Write(b)
}

// proxyWithCapture is used by ServeHTTP's non-cache path and by fetch.
func (p *Proxy) fetchAndWrite(w http.ResponseWriter, r *http.Request) (int, error) {
	cands := eligible(p.routes.BackendsFor(r.URL.Path, p.reg))
	if len(cands) == 0 {
		http.Error(w, "no healthy backends", http.StatusServiceUnavailable)
		return http.StatusServiceUnavailable, fmt.Errorf("no healthy backends")
	}
	up := p.scheduler.Pick(cands)
	if up == nil {
		http.Error(w, "no backend selected", http.StatusServiceUnavailable)
		return http.StatusServiceUnavailable, fmt.Errorf("no backend selected")
	}
	target, err := url.Parse(normalizeHTTPAddr(up.Addr))
	if err != nil {
		return 0, err
	}
	up.ActiveConns.Add(1)
	up.totalConns.Add(1)
	defer up.ActiveConns.Add(-1)

	status := http.StatusOK
	proxy := &httputil.ReverseProxy{
		Transport: p.transport,
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.Host = target.Host
			if _, ok := req.Header["User-Agent"]; !ok {
				req.Header.Set("User-Agent", "global-traffic-lb")
			}
		},
		ModifyResponse: func(resp *http.Response) error {
			status = resp.StatusCode
			resp.Header.Set("X-Backend", up.ID)
			resp.Header.Set("X-Cache", "MISS")
			if resp.StatusCode >= 500 {
				up.Circuit.RecordFailure()
			} else {
				up.Circuit.RecordSuccess()
			}
			// Cache successful GET responses.
			if p.cache != nil && r.Method == http.MethodGet && resp.StatusCode == http.StatusOK && resp.Body != nil {
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err == nil {
					hdr := make(map[string][]string)
					for k, v := range resp.Header {
						hdr[k] = append([]string(nil), v...)
					}
					p.cache.Set(cacheKey(r), resp.StatusCode, hdr, body)
					resp.Body = io.NopCloser(bytes.NewReader(body))
					resp.ContentLength = int64(len(body))
				}
			}
			return nil
		},
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
			up.Circuit.RecordFailure()
			up.mu.Lock()
			up.LastError = err.Error()
			up.mu.Unlock()
			http.Error(rw, "bad gateway", http.StatusBadGateway)
			status = http.StatusBadGateway
		},
	}
	proxy.ServeHTTP(w, r)
	return status, nil
}

func writeCached(w http.ResponseWriter, e *CacheEntry, cacheState string) {
	for k, vs := range e.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("X-Cache", cacheState)
	w.Header().Set("Age", fmt.Sprintf("%d", int(time.Since(e.StoredAt).Seconds())))
	w.WriteHeader(e.Status)
	_, _ = w.Write(e.Body)
}

func normalizeHTTPAddr(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr
	}
	return "http://" + addr
}

// ---------------------------------------------------------------------------
// HTTP server with h2c (HTTP/2 cleartext, prior-knowledge) support
// ---------------------------------------------------------------------------
//
// Stdlib-only note: golang.org/x/net/http2/h2c is an external module and is
// excluded by the stdlib-only constraint. This server therefore implements
// the h2c handshake itself: it peeks at the first bytes of each connection,
// and if it sees the HTTP/2 client connection preface ("PRI * HTTP/2.0")
// it speaks a minimal HTTP/2 framing layer (SETTINGS exchange, frame parsing,
// HPACK literal/indexed decoding without Huffman-coded strings) sufficient
// to answer simple h2c GET requests on a single stream. Anything else is
// served as HTTP/1.1 by net/http. See README Limitations for the exact scope.

const h2Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

func ListenAndServeProxy(ctx context.Context, addr string, handler http.Handler) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("l7: listening on %s (http/1.1 + h2c prior-knowledge)", addr)
	go func() { <-ctx.Done(); _ = srv.Close() }()
	err = srv.Serve(&sniffListener{Listener: ln, handler: handler})
	if ctx.Err() != nil || err == http.ErrServerClosed {
		return nil
	}
	return err
}

// sniffListener peeks at each accepted connection: h2c prior-knowledge
// connections are diverted to serveH2C, everything else is returned to
// net/http with the peeked bytes replayed.
type sniffListener struct {
	net.Listener
	handler http.Handler
}

func (l *sniffListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		br := newPeekReader(conn)
		head, err := br.Peek(len(h2Preface))
		if err == nil && string(head) == h2Preface {
			go serveH2C(conn, br, l.handler)
			continue
		}
		return &peekConn{Conn: conn, r: br}, nil
	}
}

// serveH2C implements the minimal HTTP/2 server described above.
func serveH2C(conn net.Conn, br *peekReader, handler http.Handler) {
	defer conn.Close()
	_, _ = br.Discard(len(h2Preface))
	// Send our SETTINGS (empty) and window update is unnecessary for small bodies.
	writeFrame(conn, 0x4, 0x0, 0, nil) // SETTINGS
	deadline := time.Now().Add(30 * time.Second)
	_ = conn.SetReadDeadline(deadline)
	for {
		hdr := make([]byte, 9)
		if _, err := io.ReadFull(br, hdr); err != nil {
			return
		}
		length := int(hdr[0])<<16 | int(hdr[1])<<8 | int(hdr[2])
		typ, flags := hdr[3], hdr[4]
		streamID := uint32(hdr[5]&0x7f)<<24 | uint32(hdr[6])<<16 | uint32(hdr[7])<<8 | uint32(hdr[8])
		payload := make([]byte, length)
		if _, err := io.ReadFull(br, payload); err != nil {
			return
		}
		switch typ {
		case 0x4: // SETTINGS
			if flags&0x1 == 0 {
				writeFrame(conn, 0x4, 0x1, 0, nil) // SETTINGS ACK
			}
		case 0x1, 0x9: // HEADERS / CONTINUATION
			path, method, ok := decodeHPACKHeaders(payload)
			if !ok || path == "" {
				path = "/"
			}
			if method == "" {
				method = "GET"
			}
			req, _ := http.NewRequest(method, "http://h2c"+path, nil)
			rec := &h2Recorder{header: make(http.Header), status: 200}
			handler.ServeHTTP(rec, req)
			// HEADERS frame: :status indexed/literal, encoded without Huffman.
			headerBlock := encodeHPACKStatus(rec.status)
			writeFrame(conn, 0x1, 0x4|0x1, streamID, headerBlock) // END_HEADERS|END_STREAM off; DATA follows
			if len(rec.body) > 0 {
				writeFrame(conn, 0x0, 0x1, streamID, rec.body) // DATA, END_STREAM
			} else {
				// No body: send empty DATA with END_STREAM.
				writeFrame(conn, 0x0, 0x1, streamID, nil)
			}
			return // single request per h2c connection in this minimal impl
		case 0x8: // WINDOW_UPDATE, ignore
		case 0x6: // PING
			if flags&0x1 == 0 {
				writeFrame(conn, 0x6, 0x1, 0, payload)
			}
		case 0x7: // GOAWAY
			return
		}
	}
}

func writeFrame(w io.Writer, typ, flags byte, streamID uint32, payload []byte) {
	h := []byte{byte(len(payload) >> 16), byte(len(payload) >> 8), byte(len(payload)), typ, flags,
		byte(streamID >> 24), byte(streamID >> 16), byte(streamID >> 8), byte(streamID)}
	_, _ = w.Write(h)
	if len(payload) > 0 {
		_, _ = w.Write(payload)
	}
}

type h2Recorder struct {
	header http.Header
	body   []byte
	status int
}

func (r *h2Recorder) Header() http.Header  { return r.header }
func (r *h2Recorder) WriteHeader(code int) { r.status = code }
func (r *h2Recorder) Write(b []byte) (int, error) {
	r.body = append(r.body, b...)
	return len(b), nil
}

// --- minimal HPACK decoding (indexed + literal, no Huffman) -----------------

func decodeHPACKHeaders(block []byte) (path, method string, ok bool) {
	i := 0
	readInt := func(prefix int) (int, bool) {
		if i >= len(block) {
			return 0, false
		}
		mask := byte(1<<prefix - 1)
		v := int(block[i] & mask)
		i++
		if v < int(mask) {
			return v, true
		}
		shift := 0
		for {
			if i >= len(block) {
				return 0, false
			}
			b := block[i]
			i++
			v += int(b&0x7f) << shift
			shift += 7
			if b&0x80 == 0 {
				break
			}
		}
		return v, true
	}
	readStr := func() (string, bool) {
		if i >= len(block) {
			return "", false
		}
		huffman := block[i]&0x80 != 0
		n, ok2 := readInt(7)
		if !ok2 || huffman || i+n > len(block) {
			return "", false // Huffman-coded strings unsupported (see Limitations)
		}
		s := string(block[i : i+n])
		i += n
		return s, true
	}
	// Static table entries we care about: 2=:method GET, 3=:method POST,
	// 4=:path /, 5=:path /index.html
	for i < len(block) {
		b := block[i]
		switch {
		case b&0x80 != 0: // indexed
			idx, ok2 := readInt(7)
			if !ok2 {
				return path, method, false
			}
			switch idx {
			case 2:
				method = "GET"
			case 3:
				method = "POST"
			case 4:
				path = "/"
			case 5:
				path = "/index.html"
			}
		case b&0xc0 == 0x40: // literal with incremental indexing
			_, _ = readInt(6)
			name, ok2 := readStr()
			if !ok2 {
				return path, method, false
			}
			val, ok3 := readStr()
			if !ok3 {
				return path, method, false
			}
			if name == ":path" {
				path = val
			} else if name == ":method" {
				method = val
			}
		default: // dynamic table size update / literal without indexing
			_, _ = readInt(4)
			name, ok2 := readStr()
			if !ok2 {
				// Indexed-name form: name was encoded as an integer index.
				continue
			}
			val, ok3 := readStr()
			if !ok3 {
				return path, method, false
			}
			if name == ":path" {
				path = val
			} else if name == ":method" {
				method = val
			}
		}
	}
	return path, method, true
}

func encodeHPACKStatus(status int) []byte {
	// Literal header field without indexing, indexed name 8 (:status in the
	// static table), value as a plain (non-Huffman) string.
	s := fmt.Sprintf("%d", status)
	out := []byte{0x08, byte(len(s))}
	out = append(out, s...)
	return out
}

// peekReader lets us sniff the h2c preface without consuming HTTP/1.1 bytes.
type peekReader struct {
	conn net.Conn
	buf  []byte
	off  int
}

func newPeekReader(c net.Conn) *peekReader { return &peekReader{conn: c} }

func (p *peekReader) Peek(n int) ([]byte, error) {
	for len(p.buf)-p.off < n {
		chunk := make([]byte, 4096)
		_ = p.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		m, err := p.conn.Read(chunk)
		if m > 0 {
			p.buf = append(p.buf, chunk[:m]...)
		}
		if err != nil {
			return nil, err
		}
	}
	return p.buf[p.off : p.off+n], nil
}

func (p *peekReader) Read(b []byte) (int, error) {
	if p.off < len(p.buf) {
		n := copy(b, p.buf[p.off:])
		p.off += n
		return n, nil
	}
	_ = p.conn.SetReadDeadline(time.Time{})
	return p.conn.Read(b)
}

func (p *peekReader) Discard(n int) (int, error) {
	if _, err := p.Peek(n); err != nil {
		return 0, err
	}
	p.off += n
	return n, nil
}

type peekConn struct {
	net.Conn
	r *peekReader
}

func (c *peekConn) Read(b []byte) (int, error) { return c.r.Read(b) }
