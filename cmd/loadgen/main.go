// Команда loadgen — нагрузочный тест: N пользователей одновременно пытаются купить товар,
// которого меньше, чем покупателей. В конце проверяет, что продано ровно столько, сколько было.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	base := flag.String("url", "http://localhost:8080", "API base URL")
	stock := flag.Int("stock", 100, "units for sale")
	users := flag.Int("users", 10000, "distinct buyers")
	attempts := flag.Int("attempts", 2, "requests per buyer")
	workers := flag.Int("concurrency", 200, "concurrent HTTP workers")
	flag.Parse()

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConns: *workers, MaxIdleConnsPerHost: *workers},
	}

	saleID, err := createSale(client, *base, *stock)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create sale:", err)
		os.Exit(1)
	}
	fmt.Printf("sale %d created: stock=%d buyers=%d requests=%d concurrency=%d\n",
		saleID, *stock, *users, *users**attempts, *workers)

	type job struct{ user int }
	jobs := make(chan job)
	var (
		mu        sync.Mutex
		latencies = make([]time.Duration, 0, *users**attempts)
		codes     sync.Map
		netErrors atomic.Int64
	)

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				url := fmt.Sprintf("%s/api/v1/sales/%d/reservations", *base, saleID)
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, url, nil)
				req.Header.Set("X-User-ID", fmt.Sprint(j.user))
				t := time.Now()
				resp, err := client.Do(req)
				d := time.Since(t)
				if err != nil {
					netErrors.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				v, _ := codes.LoadOrStore(resp.StatusCode, new(atomic.Int64))
				v.(*atomic.Int64).Add(1)
				mu.Lock()
				latencies = append(latencies, d)
				mu.Unlock()
			}
		}()
	}
	for a := 0; a < *attempts; a++ {
		for u := 1; u <= *users; u++ {
			jobs <- job{user: u}
		}
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	slices.Sort(latencies)
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*p)]
	}

	fmt.Printf("\nduration: %v, throughput: %.0f req/s\n", elapsed.Round(time.Millisecond), float64(len(latencies))/elapsed.Seconds())
	fmt.Printf("latency p50=%v p95=%v p99=%v max=%v\n", pct(.5), pct(.95), pct(.99), pct(1))
	fmt.Println("responses:")
	codes.Range(func(k, v any) bool {
		fmt.Printf("  %d %s: %d\n", k, http.StatusText(k.(int)), v.(*atomic.Int64).Load())
		return true
	})
	if n := netErrors.Load(); n > 0 {
		fmt.Printf("  network errors: %d\n", n)
	}

	var created int64
	if v, ok := codes.Load(http.StatusCreated); ok {
		created = v.(*atomic.Int64).Load()
	}
	reserved, available, err := getSale(client, *base, saleID)
	if err != nil {
		fmt.Fprintln(os.Stderr, "get sale:", err)
		os.Exit(1)
	}
	fmt.Printf("\nreservations created: %d, postgres reserved: %d, redis available: %d\n", created, reserved, available)
	if created == int64(*stock) && reserved == *stock && available == 0 {
		fmt.Println("OK: sold exactly the available stock, no oversell")
		return
	}
	fmt.Println("FAIL: stock invariant violated")
	os.Exit(1)
}

func createSale(c *http.Client, base string, stock int) (int64, error) {
	body, _ := json.Marshal(map[string]any{"name": "loadtest", "total_stock": stock})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+"/api/v1/sales", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	var out struct {
		ID int64 `json:"id"`
	}
	return out.ID, json.NewDecoder(resp.Body).Decode(&out)
}

func getSale(c *http.Client, base string, id int64) (reserved, available int, err error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, fmt.Sprintf("%s/api/v1/sales/%d", base, id), nil)
	if err != nil {
		return 0, 0, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close() //nolint:errcheck
	var out struct {
		Reserved  int `json:"reserved"`
		Available int `json:"available"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out.Reserved, out.Available, err
}
