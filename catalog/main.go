package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Product struct {
	ID uint64 `json:"id"`
	Name string `json:"name"`
	Description string `json:"description"`
}

type Store struct {
	mu sync.RWMutex
	nextID uint64
	items map[uint64]Product
}

func NewStore() *Store { return &Store{nextID: 1, items: map[uint64]Product{}} }

func (s *Store) Create(p Product) Product {
	s.mu.Lock()
	defer s.mu.Unlock()
	p.ID = s.nextID
	s.nextID++
	s.items[p.ID] = p
	return p
}

func (s *Store) All() []Product {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Product, 0, len(s.items))
	for i := uint64(1); i < s.nextID; i++ {
		if p, ok := s.items[i]; ok {
			out = append(out, p)
		}
	}
	return out
}

func (s *Store) Get(id uint64) (Product, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.items[id]
	return p, ok
}

func (s *Store) NaiveSearch(q string, limit int) []Product {
	q = strings.ToLower(q)
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Product
	for i := uint64(1); i < s.nextID && len(out) < limit; i++ {
		p, ok := s.items[i]
		if !ok {
			continue
		}
		if strings.Contains(strings.ToLower(p.Name+" "+p.Description), q) {
			out = append(out, p)
		}
	}
	return out
}

type Indexer struct {
	baseURL string
	client *http.Client
}

func (ix *Indexer) post(path string, v any) error {
	body, _ := json.Marshal(v)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ix.baseURL+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := ix.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("search service status %d", resp.StatusCode)
	}
	return nil
}

func (ix *Indexer) syncAll(all []Product, batch int) error {
	for i := 0; i < len(all); i += batch {
		end := min(i+batch, len(all))
		if err := ix.post("/index/bulk", all[i:end]); err != nil {
			return err
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func main() {
	store := NewStore()
	ix := &Indexer {
		baseURL: getenv("SEARCH_URL", "http://localhost:8082"),
		client: &http.Client{},
	}

	for _, p := range seed() {
		store.Create(p)
	}
	go func() {
		for {
			if err := ix.syncAll(store.All(), 5000); err == nil {
				log.Println("initial index sync OK")
				return
			}
			log.Println("search service belum siap, coba lagi 2 detik...")
			time.Sleep(2 * time.Second)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("Program Success Running")) })

	mux.HandleFunc("GET /products", func(w http.ResponseWriter, r *http.Request) { 
		writeJSON(w, 200, store.All())
	})

	mux.HandleFunc("GET /products/search", func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 10
		}
		writeJSON(w, 200, map[string]any{"hits": store.NaiveSearch(r.URL.Query().Get("q"), limit)})
	})

	mux.HandleFunc("GET /products/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseUint(r.PathValue("id"), 10, 64)
		p, ok := store.Get(id)
		if !ok {
			writeJSON(w, 404, map[string]string{"error": "not found"})
			return
		}
		writeJSON(w, 200, p)
	})

	mux.HandleFunc("POST /products", func(w http.ResponseWriter, r *http.Request) {
		var p Product
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil || strings.TrimSpace(p.Name) == "" {
			writeJSON(w, 400, map[string]string{"error": "name wajib diisi"})
			return
		}
		p = store.Create(p)
		if err := ix.post("/index", p); err != nil {
			log.Printf("gagal index produk %d: %v", p.ID, err)
		}
		writeJSON(w, 201, p)
	})

	mux.HandleFunc("POST /admin/reindex", func(w http.ResponseWriter, r *http.Request) {
		if err := ix.syncAll(store.All(), 5000); err != nil {
			writeJSON(w, 502, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]int{"reindexed": len(store.All())})
	})

	addr := ":" + getenv("PORT", "8081")
	log.Println("catalog listening on", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func seed() []Product {
	base := baseSeed()
	n, _ := strconv.Atoi(os.Getenv("SEED_COUNT"))
	words := []string{"laptop", "mouse", "keyboard", "monitor", "headset", "kursi", "meja", "webcam", "speaker", "tablet", "kamera"}
	adj := []string{"gaming", "kantor", "murah", "premium", "wireless", "portable", "ergonomis", "compact"}
	for i := 0; i < n; i++ {
		w, a := words[i%len(words)], adj[(i/len(words))%len(adj)]
		base = append(base, Product{
			Name: fmt.Sprintf("%s %s seri %d", w, a, i),
			Description: fmt.Sprintf("Produk %s %s nomor %d untuk kebutuhan sehari-hari dan profesional", w, a, i),
		})
	}
	return base
}

func baseSeed() []Product {
	return []Product{
		{Name: "Laptop Gaming ASUS ROG", Description: "Laptop gaming performa tinggi dengan RTX dan layar 165Hz"},
		{Name: "Laptop Ultrabook Lenovo", Description: "Laptop tipis ringan untuk kerja dan kuliah"},
		{Name: "Mouse Wireless Logitech", Description: "Mouse nirkabel ergonomis baterai awet"},
		{Name: "Keyboard Mekanikal", Description: "Keyboard mekanikal switch biru untuk gaming dan mengetik"},
		{Name: "Monitor 27 inch 4K", Description: "Monitor 4K IPS untuk desain dan produktivitas"},
		{Name: "Headset Gaming", Description: "Headset gaming surround dengan mikrofon noise cancelling"},
		{Name: "Kursi Ergonomis", Description: "Kursi kantor ergonomis dengan sandaran lumbar"},
		{Name: "Webcam Full HD", Description: "Webcam 1080p untuk meeting online dan streaming"},
	}
}