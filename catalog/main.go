// Catalog service (Go): pemilik data produk, disimpan di PostgreSQL.
//
// Tugas:
//  1. CRUD produk di Postgres (source of truth).
//  2. Seed data (ribuan sampai jutaan) lewat SQL generate_series.
//  3. Mengirim data ke search service Rust (index di memori).
//  4. Menyediakan 3 mode pencarian di Postgres sebagai pembanding TANPA Rust:
//     mode=scan        ILIKE '%kata%' (tanpa index, seq scan)
//     mode=fts         full-text search + GIN index + ranking ts_rank
//     mode=fts_norank  full-text search + GIN index, tanpa ranking
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Product struct {
	ID          int64   `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Score       float32 `json:"score,omitempty"`
}

const schema = `
CREATE TABLE IF NOT EXISTS products (
	id          BIGSERIAL PRIMARY KEY,
	name        TEXT NOT NULL,
	description TEXT NOT NULL DEFAULT '',
	tsv         tsvector GENERATED ALWAYS AS
	            (to_tsvector('simple', name || ' ' || description)) STORED
)`

// ---------------------------------------------------------------- status

type Status struct {
	State       atomic.Value // "starting" | "seeding" | "indexing" | "ready"
	RustIndexed atomic.Int64
}

func (s *Status) set(v string) { s.State.Store(v); log.Println("status:", v) }

// ---------------------------------------------------------------- seed

// seedDB menambah data sampai jumlah baris = target. Aman dijalankan berulang
// (hanya menambah selisihnya). Data dummy dibuat langsung di dalam Postgres
// sehingga 1 juta baris selesai dalam hitungan detik, bukan menit.
func seedDB(ctx context.Context, pool *pgxpool.Pool, target int64) error {
	var count int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM products`).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		_, err := pool.Exec(ctx, `INSERT INTO products (name, description) VALUES
			('Laptop Gaming ASUS ROG',   'Laptop gaming performa tinggi dengan RTX dan layar 165Hz'),
			('Laptop Ultrabook Lenovo',  'Laptop tipis ringan untuk kerja dan kuliah'),
			('Mouse Wireless Logitech',  'Mouse nirkabel ergonomis baterai awet'),
			('Keyboard Mekanikal',       'Keyboard mekanikal switch biru untuk gaming dan mengetik'),
			('Monitor 27 inch 4K',       'Monitor 4K IPS untuk desain dan produktivitas'),
			('Headset Gaming',           'Headset gaming surround dengan mikrofon noise cancelling'),
			('Kursi Ergonomis',          'Kursi kantor ergonomis dengan sandaran lumbar'),
			('Webcam Full HD',           'Webcam 1080p untuk meeting online dan streaming')`)
		if err != nil {
			return err
		}
		count = 8
	}

	const chunk = 500_000
	for count < target {
		from, to := count+1, min(count+chunk, target)
		start := time.Now()
		// Distribusi data dummy:
		//  - kata kategori & sifat: 10 dan 8 nilai  -> kata "umum" (10-12% baris)
		//  - merkNNN : 1000 nilai                   -> kata "sedang" (0,1% baris)
		//  - zebra   : 10 baris per 1 juta          -> kata "langka"
		_, err := pool.Exec(ctx, `
			INSERT INTO products (name, description)
			SELECT
				initcap(w.word) || ' ' || a.adj || ' merk' || (i % 1000) || ' seri ' || i
					|| CASE WHEN i % 100000 = 7 THEN ' zebra' ELSE '' END,
				'Produk ' || w.word || ' ' || a.adj || ' nomor ' || i
					|| ' untuk kebutuhan sehari-hari dan profesional'
			FROM generate_series($1::bigint, $2::bigint) AS i,
			LATERAL (SELECT (ARRAY['laptop','mouse','keyboard','monitor','headset',
			                       'kursi','webcam','speaker','tablet','kamera'])[1 + (i % 10)::int] AS word) w,
			LATERAL (SELECT (ARRAY['gaming','kantor','murah','premium','wireless',
			                       'portable','ergonomis','compact'])[1 + ((i / 10) % 8)::int] AS adj) a`,
			from, to)
		if err != nil {
			return err
		}
		count = to
		log.Printf("seed: %d/%d baris (%.1fs untuk batch ini)", count, target, time.Since(start).Seconds())
	}
	return nil
}

func ensureIndex(ctx context.Context, pool *pgxpool.Pool) error {
	start := time.Now()
	if _, err := pool.Exec(ctx, `CREATE INDEX IF NOT EXISTS products_tsv_idx ON products USING GIN (tsv)`); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `ANALYZE products`); err != nil {
		return err
	}
	log.Printf("GIN index + ANALYZE siap (%.1fs)", time.Since(start).Seconds())
	return nil
}

// ---------------------------------------------------------------- search

var wordRe = regexp.MustCompile(`[\p{L}\p{N}]+`)

// tsQuery: "laptop gaming" -> "laptop | gaming" (OR, sama seperti engine Rust).
func tsQuery(q string) string {
	return strings.Join(wordRe.FindAllString(strings.ToLower(q), -1), " | ")
}

func escapeLike(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

func search(ctx context.Context, pool *pgxpool.Pool, mode, q string, limit int) ([]Product, error) {
	var rows pgx.Rows
	var err error
	switch mode {
	case "scan":
		like := "%" + escapeLike(strings.TrimSpace(q)) + "%"
		rows, err = pool.Query(ctx, `
			SELECT id, name, description, 0::float4 FROM products
			WHERE name ILIKE $1 OR description ILIKE $1
			LIMIT $2`, like, limit)
	case "fts", "fts_norank":
		tq := tsQuery(q)
		if tq == "" {
			return []Product{}, nil
		}
		if mode == "fts" {
			rows, err = pool.Query(ctx, `
				SELECT id, name, description, ts_rank(tsv, query)
				FROM products, to_tsquery('simple', $1) AS query
				WHERE tsv @@ query
				ORDER BY 4 DESC, id
				LIMIT $2`, tq, limit)
		} else {
			rows, err = pool.Query(ctx, `
				SELECT id, name, description, 0::float4
				FROM products, to_tsquery('simple', $1) AS query
				WHERE tsv @@ query
				LIMIT $2`, tq, limit)
		}
	default:
		return nil, fmt.Errorf("mode tidak dikenal: %q (pakai scan|fts|fts_norank)", mode)
	}
	if err != nil {
		return nil, err
	}
	return collect(rows)
}

func collect(rows pgx.Rows) ([]Product, error) {
	defer rows.Close()
	out := []Product{}
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.Score); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------- rust sync

// Indexer mengirim data ke search service (Rust).
type Indexer struct {
	baseURL string
	client  *http.Client
}

func (ix *Indexer) post(path string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

// syncFromDB membaca seluruh tabel per batch (keyset pagination: WHERE id > last)
// dan mengirimkannya ke Rust. Keyset jauh lebih cepat dari OFFSET untuk tabel besar.
func (ix *Indexer) syncFromDB(ctx context.Context, pool *pgxpool.Pool, st *Status) error {
	const batch = 5000
	var last int64
	var total int64
	st.RustIndexed.Store(0)
	for {
		rows, err := pool.Query(ctx,
			`SELECT id, name, description, 0::float4 FROM products WHERE id > $1 ORDER BY id LIMIT $2`,
			last, batch)
		if err != nil {
			return err
		}
		items, err := collect(rows)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		if err := ix.post("/index/bulk", items); err != nil {
			return err
		}
		last = items[len(items)-1].ID
		total += int64(len(items))
		st.RustIndexed.Store(total)
	}
}

func (ix *Indexer) syncWithRetry(ctx context.Context, pool *pgxpool.Pool, st *Status) {
	for attempt := 1; ; attempt++ {
		start := time.Now()
		err := ix.syncFromDB(ctx, pool, st)
		if err == nil {
			log.Printf("sync ke Rust OK: %d dokumen (%.1fs)", st.RustIndexed.Load(), time.Since(start).Seconds())
			return
		}
		if attempt == 1 || attempt%10 == 0 {
			log.Printf("sync ke Rust gagal (%v), coba lagi tiap 2 detik...", err)
		}
		time.Sleep(2 * time.Second)
	}
}

// ---------------------------------------------------------------- http

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func atoiDefault(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil && n > 0 {
		return n
	}
	return def
}

func main() {
	ctx := context.Background()
	st := &Status{}
	st.set("starting")

	dsn := getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/shop")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxConns = int32(atoiDefault(os.Getenv("DB_MAX_CONNS"), 20))

	// Tunggu Postgres siap (di Docker, Postgres butuh beberapa detik untuk start).
	var pool *pgxpool.Pool
	for i := 0; ; i++ {
		pool, err = pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				break
			}
		}
		if i >= 30 {
			log.Fatalf("tidak bisa konek ke database: %v", err)
		}
		log.Println("menunggu database...", err)
		time.Sleep(2 * time.Second)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, schema); err != nil {
		log.Fatal(err)
	}

	ix := &Indexer{baseURL: getenv("SEARCH_URL", "http://localhost:8082"), client: &http.Client{}}

	// Seed, index, lalu sinkronisasi ke Rust berjalan di background
	// supaya API sudah bisa dipakai selama proses ini.
	go func() {
		st.set("seeding")
		target, _ := strconv.ParseInt(getenv("SEED_COUNT", "100000"), 10, 64)
		if err := seedDB(ctx, pool, target); err != nil {
			log.Fatalf("seed gagal: %v", err)
		}
		if err := ensureIndex(ctx, pool); err != nil {
			log.Fatalf("index gagal: %v", err)
		}
		if getenv("SYNC_RUST", "true") == "true" {
			st.set("indexing")
			ix.syncWithRetry(ctx, pool, st)
		}
		st.set("ready")
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })

	mux.HandleFunc("GET /admin/status", func(w http.ResponseWriter, r *http.Request) {
		var rows int64
		pool.QueryRow(r.Context(), `SELECT count(*) FROM products`).Scan(&rows)
		writeJSON(w, 200, map[string]any{
			"state": st.State.Load(), "db_rows": rows, "rust_indexed": st.RustIndexed.Load(),
		})
	})

	mux.HandleFunc("GET /products", func(w http.ResponseWriter, r *http.Request) {
		limit := min(atoiDefault(r.URL.Query().Get("limit"), 20), 100)
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		rows, err := pool.Query(r.Context(),
			`SELECT id, name, description, 0::float4 FROM products ORDER BY id LIMIT $1 OFFSET $2`, limit, offset)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		items, err := collect(rows)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, items)
	})

	mux.HandleFunc("GET /products/search", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		mode := q.Get("mode")
		if mode == "" {
			mode = "fts"
		}
		limit := min(atoiDefault(q.Get("limit"), 10), 100)
		hits, err := search(r.Context(), pool, mode, q.Get("q"), limit)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"total": len(hits), "hits": hits})
	})

	mux.HandleFunc("GET /products/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
		var p Product
		err := pool.QueryRow(r.Context(),
			`SELECT id, name, description FROM products WHERE id = $1`, id).Scan(&p.ID, &p.Name, &p.Description)
		if err == pgx.ErrNoRows {
			writeJSON(w, 404, map[string]string{"error": "not found"})
			return
		} else if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
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
		err := pool.QueryRow(r.Context(),
			`INSERT INTO products (name, description) VALUES ($1, $2) RETURNING id`,
			p.Name, p.Description).Scan(&p.ID)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		// Gagal index ke Rust tidak menggagalkan request: DB = source of truth.
		if err := ix.post("/index", p); err != nil {
			log.Printf("gagal index produk %d ke Rust: %v (jalankan /admin/reindex)", p.ID, err)
		}
		writeJSON(w, 201, p)
	})

	// Bangun ulang index Rust dari database (mis. setelah Rust restart).
	mux.HandleFunc("POST /admin/reindex", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		if err := ix.syncFromDB(r.Context(), pool, st); err != nil {
			writeJSON(w, 502, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"reindexed": st.RustIndexed.Load(), "seconds": time.Since(start).Seconds()})
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