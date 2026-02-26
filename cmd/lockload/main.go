package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type acquireResp struct {
	Acquired         bool   `json:"acquired"`
	LockName         string `json:"lock_name"`
	OwnerID          string `json:"owner_id,omitempty"`
	LeaseID          string `json:"lease_id,omitempty"`
	FencingToken     int64  `json:"fencing_token,omitempty"`
	LeaseExpiryMS    int64  `json:"lease_expiry_ms,omitempty"`
	CurrentOwnerID   string `json:"current_owner_id,omitempty"`
	CurrentExpiryMS  int64  `json:"current_expiry_ms,omitempty"`
	RecommendedRetry int64  `json:"recommended_retry_ms,omitempty"`
}

type renewResp struct {
	Renewed       bool  `json:"renewed"`
	LeaseExpiryMS int64 `json:"lease_expiry_ms,omitempty"`
	Reason        string `json:"reason,omitempty"`
}

type releaseResp struct {
	Released bool   `json:"released"`
	Reason   string `json:"reason,omitempty"`
}

var (
    errTimeout int64
    errConn    int64
    errHTTP500 int64
    errOther   int64
)

// ProtectedResource simulates a downstream system guarded by fencing tokens.
// It accepts a write only if token >= lastToken, then sets lastToken=token.
type ProtectedResource struct {
	mu        sync.Mutex
	lastToken int64
	accepted  int64
	rejected  int64
}

func (p *ProtectedResource) TryWrite(token int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if token < p.lastToken {
		p.rejected++
		return false
	}
	p.lastToken = token
	p.accepted++
	return true
}

func (p *ProtectedResource) Stats() (accepted, rejected, last int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted, p.rejected, p.lastToken
}

func main() {
	var (
		baseURL   = flag.String("url", "http://localhost:8080", "LockServer base URL")
		lockName  = flag.String("lock", "hotlock", "lock name")
		clients   = flag.Int("clients", 50, "number of concurrent clients")
		duration  = flag.Duration("duration", 20*time.Second, "test duration")
		ttl       = flag.Duration("ttl", 800*time.Millisecond, "lease ttl")
		hold      = flag.Duration("hold", 30*time.Millisecond, "time spent in critical section")
		jitter    = flag.Duration("jitter", 30*time.Millisecond, "extra random sleep while holding")
		failRate  = flag.Float64("failrate", 0.03, "probability to sleep past ttl (simulate GC pause / stall)")
	)
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	httpc := &http.Client{Timeout: 10 * time.Second}
	pr := &ProtectedResource{}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var (
		acqOK   int64
		acqFail int64
		writeOK int64
		writeStaleRejected int64
		releaseOK int64
		errCount int64

		latMu sync.Mutex
		lat   []time.Duration
	)

	wg := sync.WaitGroup{}
	start := time.Now()

	for i := 0; i < *clients; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			owner := fmt.Sprintf("c-%d", i)

			for ctx.Err() == nil {
				// acquire loop (bounded retry client-side)
				ar, ok, err := acquire(ctx, httpc, *baseURL, *lockName, owner, *ttl)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				if !ok {
					atomic.AddInt64(&acqFail, 1)
					sleep := time.Duration(ar.RecommendedRetry) * time.Millisecond
					if sleep <= 0 {
						sleep = 20 * time.Millisecond
					}
					time.Sleep(sleep)
					continue
				}

				atomic.AddInt64(&acqOK, 1)

				// record acquire latency if server returned quickly (we measure client-side)
				// (Optional: move measurement into acquire() with start time)
				// critical section
				// Failure injection: sometimes sleep past TTL to simulate lease expiry mid-CS
				if rand.Float64() < *failRate {
					time.Sleep(*ttl + 50*time.Millisecond)
				} else {
					time.Sleep(*hold + time.Duration(rand.Int63n(int64(*jitter)+1)))
				}

				// attempt downstream write guarded by fencing token
				if pr.TryWrite(ar.FencingToken) {
					atomic.AddInt64(&writeOK, 1)
				} else {
					atomic.AddInt64(&writeStaleRejected, 1)
				}

				// release (may fail if lease expired and someone else acquired; that’s fine)
				rr, err := release(ctx, httpc, *baseURL, *lockName, owner, ar.LeaseID, ar.FencingToken)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
				} else if rr.Released {
					atomic.AddInt64(&releaseOK, 1)
				}

				// small think time to avoid tight loop
				time.Sleep(5 * time.Millisecond)

				// store some latency samples (optional; to keep memory bounded, sample)
				_ = latMu
				_ = lat
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	accepted, rejected, last := pr.Stats()

	fmt.Println("=== LockServer Contention Test ===")
	fmt.Printf("duration: %s, clients: %d, lock: %s\n", elapsed, *clients, *lockName)
	fmt.Printf("acquire_success: %d\n", acqOK)
	fmt.Printf("acquire_fail:    %d\n", acqFail)
	fmt.Printf("release_success: %d\n", releaseOK)
	fmt.Printf("writes_accepted: %d\n", accepted)
	fmt.Printf("stale_rejected:  %d\n", rejected)
	fmt.Printf("last_token:      %d\n", last)
	fmt.Printf("errors:          %d\n", errCount)

	// The key correctness assertion (manual):
	// stale_rejected should be > 0 when failrate > 0 under contention,
	// and writes_accepted should never include stale tokens because ProtectedResource blocks them.
	_ = latMu
}

func acquire(ctx context.Context, c *http.Client, baseURL, lock, owner string, ttl time.Duration) (acquireResp, bool, error) {
	body := map[string]interface{}{
		"owner_id": owner,
		"ttl_ms":   ttl.Milliseconds(),
	}
	b, _ := json.Marshal(body)

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/v1/locks/%s/acquire", baseURL, lock), bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		return acquireResp{}, false, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)

	var ar acquireResp
	if err := json.Unmarshal(data, &ar); err != nil {
		return acquireResp{}, false, fmt.Errorf("decode acquire: %v body=%s", err, string(data))
	}

	if resp.StatusCode == http.StatusOK && ar.Acquired {
		return ar, true, nil
	}
	if resp.StatusCode == http.StatusConflict && !ar.Acquired {
		return ar, false, nil
	}
	return ar, false, fmt.Errorf("acquire unexpected status=%d body=%s", resp.StatusCode, string(data))
}

func release(ctx context.Context, c *http.Client, baseURL, lock, owner, leaseID string, token int64) (releaseResp, error) {
	body := map[string]interface{}{
		"owner_id":      owner,
		"lease_id":      leaseID,
		"fencing_token": token,
	}
	b, _ := json.Marshal(body)

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/v1/locks/%s/release", baseURL, lock), bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		return releaseResp{}, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	var rr releaseResp
	if err := json.Unmarshal(data, &rr); err != nil {
		return releaseResp{}, fmt.Errorf("decode release: %v body=%s", err, string(data))
	}
	if resp.StatusCode != http.StatusOK {
		return rr, fmt.Errorf("release unexpected status=%d body=%s", resp.StatusCode, string(data))
	}
	return rr, nil
}