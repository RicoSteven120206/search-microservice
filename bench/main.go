// Benchmark: membandingkan engine pencarian lewat gateway.
//
// Contoh:
//
//	go run . -c 16 -d 8s
//	go run . -engines rust,fts,scan -queries zebra,laptop -c 32 -d 10s
//
// Setiap kombinasi (query x engine) dijalankan selama -d dengan -c koneksi paralel.
// Respons yang bukan HTTP 200, atau yang dialihkan (fallback) dari Rust,
// dihitung terpisah supaya angka Rust tidak tercampur dengan engine lain.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

type result struct {
	lat       []time.Duration
	errors    int
	fallbacks int
}

func run(base, engine, q string, c int, d time.Duration) (result, float64) {
	client := &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: c},
		Timeout:   60 * time.Second,
	}
	target := fmt.Sprintf("%s/api/search?q=%s&engine=%s&limit=10", base, url.QueryEscape(q), engine)

	// warm-up kecil (cache OS/Postgres, koneksi keep-alive)
	for i := 0; i < 3; i++ {
		if resp, err := client.Get(target); err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		res result
	)
	deadline := time.Now().Add(d)
	start := time.Now()
	for w := 0; w < c; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				t := time.Now()
				resp, err := client.Get(target)
				var fb, bad bool
				if err != nil {
					bad = true
				} else {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					bad = resp.StatusCode != 200
					fb = resp.Header.Get("X-Search-Fallback") == "true"
				}
				el := time.Since(t)
				mu.Lock()
				switch {
				case bad:
					res.errors++
				case fb:
					res.fallbacks++
				default:
					res.lat = append(res.lat, el)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return res, time.Since(start).Seconds()
}

func main() {
	base := flag.String("base", "http://localhost:8080", "URL gateway")
	engines := flag.String("engines", "rust,fts,fts_norank,scan", "daftar engine, pisahkan koma")
	queries := flag.String("queries", "zebra,merk42,laptop,laptop gaming", "daftar query, pisahkan koma")
	c := flag.Int("c", 16, "koneksi paralel")
	d := flag.Duration("d", 8*time.Second, "durasi tiap pengujian")
	flag.Parse()

	fmt.Printf("| Query | Engine | Req/detik | p50 | p95 | p99 | error | fallback |\n")
	fmt.Printf("|---|---|---:|---:|---:|---:|---:|---:|\n")
	for _, q := range strings.Split(*queries, ",") {
		for _, e := range strings.Split(*engines, ",") {
			res, secs := run(*base, e, q, *c, *d)
			if len(res.lat) == 0 {
				fmt.Printf("| `%s` | %s | - | - | - | - | %d | %d |\n", q, e, res.errors, res.fallbacks)
				continue
			}
			sort.Slice(res.lat, func(i, j int) bool { return res.lat[i] < res.lat[j] })
			p := func(x float64) time.Duration { return res.lat[int(float64(len(res.lat)-1)*x)].Round(10 * time.Microsecond) }
			fmt.Printf("| `%s` | %s | %.0f | %v | %v | %v | %d | %d |\n",
				q, e, float64(len(res.lat))/secs, p(.50), p(.95), p(.99), res.errors, res.fallbacks)
		}
	}
}