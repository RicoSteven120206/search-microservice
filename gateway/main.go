// Gateway (Go): pintu masuk tunggal untuk client.
//
//	GET  /api/products?limit=&offset=
//	GET  /api/products/{id}
//	POST /api/products
//	GET  /api/search?q=laptop&limit=10&engine=rust|fts|fts_norank|scan
//
// engine (default rust):
//
//	rust        -> search service Rust. Jika down/timeout, fallback ke Postgres FTS.
//	fts         -> Postgres full-text search + GIN index + ranking
//	fts_norank  -> Postgres full-text search + GIN index, tanpa ranking
//	scan        -> Postgres ILIKE (tanpa index)
//
// Header respons X-Search-Engine menunjukkan engine yang BENAR-BENAR dipakai,
// dan X-Search-Fallback: true muncul jika Rust gagal lalu dialihkan.
package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"
)

type Gateway struct {
	catalogURL string
	searchURL  string
	client     *http.Client
	upstreamTO time.Duration // batas waktu ke catalog
	searchTO   time.Duration // batas waktu ke Rust sebelum fallback
}

func (g *Gateway) forward(w http.ResponseWriter, r *http.Request, target string) {
	ctx, cancel := context.WithTimeout(r.Context(), g.upstreamTO)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, r.Method, target, r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	req.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	resp, err := g.client.Do(req)
	if err != nil {
		http.Error(w, `{"error":"upstream tidak tersedia"}`, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (g *Gateway) search(w http.ResponseWriter, r *http.Request) {
	q := url.QueryEscape(r.URL.Query().Get("q"))
	limit := "10"
	if l := r.URL.Query().Get("limit"); l != "" {
		limit = url.QueryEscape(l)
	}
	engine := r.URL.Query().Get("engine")
	if engine == "" {
		engine = "rust"
	}

	mode := engine
	if engine == "rust" {
		ctx, cancel := context.WithTimeout(r.Context(), g.searchTO)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, g.searchURL+"/search?q="+q+"&limit="+limit, nil)
		resp, err := g.client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			defer resp.Body.Close()
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Search-Engine", "rust")
			io.Copy(w, resp.Body)
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		log.Printf("search service Rust gagal (%v), fallback ke Postgres FTS", err)
		w.Header().Set("X-Search-Fallback", "true")
		mode = "fts"
	}

	w.Header().Set("X-Search-Engine", "postgres-"+mode)
	g.forward(w, r, g.catalogURL+"/products/search?mode="+url.QueryEscape(mode)+"&q="+q+"&limit="+limit)
}

func main() {
	g := &Gateway{
		catalogURL: getenv("CATALOG_URL", "http://localhost:8081"),
		searchURL:  getenv("SEARCH_URL", "http://localhost:8082"),
		client:     &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 200}},
		upstreamTO: getDuration("UPSTREAM_TIMEOUT", 30*time.Second),
		searchTO:   getDuration("SEARCH_TIMEOUT", 2*time.Second),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /api/search", g.search)
	mux.HandleFunc("GET /api/products", func(w http.ResponseWriter, r *http.Request) {
		g.forward(w, r, g.catalogURL+"/products?"+r.URL.RawQuery)
	})
	mux.HandleFunc("GET /api/products/{id}", func(w http.ResponseWriter, r *http.Request) {
		g.forward(w, r, g.catalogURL+"/products/"+r.PathValue("id"))
	})
	mux.HandleFunc("POST /api/products", func(w http.ResponseWriter, r *http.Request) {
		g.forward(w, r, g.catalogURL+"/products")
	})

	addr := ":" + getenv("PORT", "8080")
	log.Println("gateway listening on", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}