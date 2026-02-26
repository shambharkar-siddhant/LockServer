package lockclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAcquireWithRetry_SucceedsAfterConflicts(t *testing.T) {
	var calls int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/locks/testlock/acquire" {
			http.NotFound(w, r)
			return
		}
		calls++

		// First 2 calls: conflict
		if calls <= 2 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{
				"acquired": false,
				"lock_name": "testlock",
				"current_owner_id": "someone",
				"recommended_retry_ms": 10,
				"reason": "HELD"
			}`))
			return
		}

		// 3rd call: success
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"acquired": true,
			"lock_name": "testlock",
			"owner_id": "me",
			"lease_id": "L1",
			"fencing_token": 42,
			"lease_expiry_ms": 12345
		}`))
	}))
	defer srv.Close()

	c := New(srv.URL, &http.Client{Timeout: 2 * time.Second})

	lease, err := c.AcquireWithRetry(context.Background(), "testlock", "me", AcquireOptions{
		TTL:          200 * time.Millisecond,
		MaxRetries:   10,
		MaxTotalWait: 1 * time.Second,
		MinRetry:     5 * time.Millisecond,
		MaxRetry:     50 * time.Millisecond,
		JitterFrac:   0, // deterministic
	})
	if err != nil {
		t.Fatalf("expected success, got err=%v", err)
	}
	if lease.FencingTokenValue() != 42 || lease.LeaseID != "L1" {
		t.Fatalf("unexpected lease: %+v", lease)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}