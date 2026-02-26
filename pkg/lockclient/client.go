package lockclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	http    *http.Client
	rng     *rand.Rand
}

func New(baseURL string, hc *http.Client) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{
		baseURL: baseURL,
		http:    hc,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// ---- Wire format (matches your HTTP API) ----

type acquireReq struct {
	OwnerID string `json:"owner_id"`
	TTLMS   int64  `json:"ttl_ms"`
}
type acquireResp struct {
	Acquired          bool   `json:"acquired"`
	LockName          string `json:"lock_name"`
	OwnerID           string `json:"owner_id,omitempty"`
	LeaseID           string `json:"lease_id,omitempty"`
	FencingToken      int64  `json:"fencing_token,omitempty"`
	LeaseExpiryMS     int64  `json:"lease_expiry_ms,omitempty"`
	CurrentOwnerID    string `json:"current_owner_id,omitempty"`
	CurrentExpiryMS   int64  `json:"current_expiry_ms,omitempty"`
	RecommendedRetry  int64  `json:"recommended_retry_ms,omitempty"`
	Reason            string `json:"reason,omitempty"` // HELD | BUSY_RETRY (if you added it)
}

type renewReq struct {
	OwnerID      string `json:"owner_id"`
	LeaseID      string `json:"lease_id"`
	FencingToken int64  `json:"fencing_token"`
	ExtendByMS   int64  `json:"extend_by_ms"`
}
type renewResp struct {
	Renewed       bool   `json:"renewed"`
	Reason        string `json:"reason,omitempty"` // NOT_OWNER_OR_EXPIRED | BUSY_RETRY
	LeaseExpiryMS int64  `json:"lease_expiry_ms,omitempty"`
}

type releaseReq struct {
	OwnerID      string `json:"owner_id"`
	LeaseID      string `json:"lease_id"`
	FencingToken int64  `json:"fencing_token"`
}
type releaseResp struct {
	Released bool   `json:"released"`
	Reason   string `json:"reason,omitempty"` // NOT_OWNER | BUSY_RETRY
}

// ---- Low-level operations ----

func (c *Client) AcquireOnce(ctx context.Context, lockName, ownerID string, ttl time.Duration) (Lease, *NotAcquiredError, error) {
	if lockName == "" || ownerID == "" {
		return Lease{}, nil, fmt.Errorf("lockName and ownerID required")
	}
	if ttl <= 0 {
		return Lease{}, nil, fmt.Errorf("ttl must be > 0")
	}

	path := fmt.Sprintf("%s/v1/locks/%s/acquire", c.baseURL, lockName)
	reqBody := acquireReq{OwnerID: ownerID, TTLMS: ttl.Milliseconds()}

	var out acquireResp
	code, raw, err := c.doJSON(ctx, http.MethodPost, path, reqBody, &out)
	if err != nil {
		return Lease{}, nil, err
	}

	if code == http.StatusOK && out.Acquired {
		return Lease{
			LockName:      lockName,
			OwnerID:       out.OwnerID,
			LeaseID:       out.LeaseID,
			FencingToken:  out.FencingToken,
			LeaseExpiryMS: out.LeaseExpiryMS,
		}, nil, nil
	}

	if code == http.StatusConflict {
		return Lease{}, &NotAcquiredError{
			LockName:         lockName,
			Reason:           out.Reason,
			RecommendedRetry: out.RecommendedRetry,
			CurrentOwnerID:   out.CurrentOwnerID,
			CurrentExpiryMS:  out.CurrentExpiryMS,
		}, nil
	}

	return Lease{}, nil, &UnexpectedStatusError{
		Method: http.MethodPost,
		Path:   path,
		Code:   code,
		Body:   raw,
	}
}

func (c *Client) RenewOnce(ctx context.Context, l Lease, extendBy time.Duration) (int64 /*new expiry ms*/, bool /*renewed*/, string /*reason*/, error) {
	if l.LockName == "" || l.OwnerID == "" || l.LeaseID == "" || l.FencingToken <= 0 {
		return 0, false, "", fmt.Errorf("invalid lease")
	}
	if extendBy <= 0 {
		return 0, false, "", fmt.Errorf("extendBy must be > 0")
	}

	path := fmt.Sprintf("%s/v1/locks/%s/renew", c.baseURL, l.LockName)
	reqBody := renewReq{
		OwnerID:      l.OwnerID,
		LeaseID:      l.LeaseID,
		FencingToken: l.FencingToken,
		ExtendByMS:   extendBy.Milliseconds(),
	}

	var out renewResp
	code, raw, err := c.doJSON(ctx, http.MethodPost, path, reqBody, &out)
	if err != nil {
		return 0, false, "", err
	}

	if code == http.StatusOK {
		return out.LeaseExpiryMS, out.Renewed, out.Reason, nil
	}

	return 0, false, "", &UnexpectedStatusError{Method: http.MethodPost, Path: path, Code: code, Body: raw}
}

func (c *Client) ReleaseOnce(ctx context.Context, l Lease) (bool, string, error) {
	if l.LockName == "" || l.OwnerID == "" || l.LeaseID == "" || l.FencingToken <= 0 {
		return false, "", fmt.Errorf("invalid lease")
	}

	path := fmt.Sprintf("%s/v1/locks/%s/release", c.baseURL, l.LockName)
	reqBody := releaseReq{
		OwnerID:      l.OwnerID,
		LeaseID:      l.LeaseID,
		FencingToken: l.FencingToken,
	}

	var out releaseResp
	code, raw, err := c.doJSON(ctx, http.MethodPost, path, reqBody, &out)
	if err != nil {
		return false, "", err
	}

	if code == http.StatusOK {
		return out.Released, out.Reason, nil
	}

	return false, "", &UnexpectedStatusError{Method: http.MethodPost, Path: path, Code: code, Body: raw}
}

// doJSON sends JSON and optionally decodes JSON response.
// Returns status code and raw body (trimmed) for debugging.
func (c *Client) doJSON(ctx context.Context, method, url string, req any, resp any) (int, string, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return 0, "", err
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(b))
	if err != nil {
		return 0, "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	rsp, err := c.http.Do(httpReq)
	if err != nil {
		return 0, "", err
	}
	defer rsp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(rsp.Body, 1<<20))
	raw := strings.TrimSpace(string(body))

	if resp != nil && len(body) > 0 {
		_ = json.Unmarshal(body, resp) // tolerate non-JSON error bodies
	}
	return rsp.StatusCode, raw, nil
}

// ---- Retry wrapper ----

func (c *Client) AcquireWithRetry(ctx context.Context, lockName, ownerID string, opt AcquireOptions) (Lease, error) {
	if opt.TTL <= 0 {
		return Lease{}, fmt.Errorf("AcquireOptions.TTL required")
	}
	if opt.MaxRetries <= 0 {
		opt.MaxRetries = 50
	}
	if opt.MinRetry <= 0 {
		opt.MinRetry = 25 * time.Millisecond
	}
	if opt.MaxRetry <= 0 {
		opt.MaxRetry = 1 * time.Second
	}
	if opt.JitterFrac <= 0 {
		opt.JitterFrac = 0.2
	}

	start := time.Now()
	var lastNA *NotAcquiredError

	for attempt := 0; attempt <= opt.MaxRetries; attempt++ {
		if opt.MaxTotalWait > 0 && time.Since(start) > opt.MaxTotalWait {
			if lastNA != nil {
				return Lease{}, lastNA
			}
			return Lease{}, context.DeadlineExceeded
		}

		lease, na, err := c.AcquireOnce(ctx, lockName, ownerID, opt.TTL)
		if err != nil {
			return Lease{}, err
		}
		if na == nil {
			return lease, nil
		}

		lastNA = na
		// Backoff: honor server recommended retry if present; clamp and add jitter.
		sleep := time.Duration(na.RecommendedRetry) * time.Millisecond
		if sleep <= 0 {
			// exponential-ish based on attempt
			sleep = time.Duration(float64(opt.MinRetry) * math.Pow(1.5, float64(attempt)))
		}
		if sleep < opt.MinRetry {
			sleep = opt.MinRetry
		}
		if sleep > opt.MaxRetry {
			sleep = opt.MaxRetry
		}
		sleep = addJitter(c.rng, sleep, opt.JitterFrac)

		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Lease{}, ctx.Err()
		case <-timer.C:
		}
	}

	if lastNA != nil {
		return Lease{}, lastNA
	}
	return Lease{}, fmt.Errorf("acquire failed")
}

func addJitter(r *rand.Rand, d time.Duration, frac float64) time.Duration {
	if frac <= 0 {
		return d
	}
	// jitter range: [d*(1-frac), d*(1+frac)]
	j := (r.Float64()*2 - 1) * frac
	out := time.Duration(float64(d) * (1 + j))
	if out < 0 {
		return 0
	}
	return out
}