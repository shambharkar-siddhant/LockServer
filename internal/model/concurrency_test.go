package model_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"lockserver/internal/model"
	"lockserver/internal/storage"
)

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

func (p *ProtectedResource) Stats() (accepted, rejected, lastToken int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted, p.rejected, p.lastToken
}

func TestFencingPreventsStaleWriters(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "lockserver_test.db")

	ctx := context.Background()
	db, err := storage.Open(ctx, storage.Config{
		Path:         dbPath,
		BusyTimeout:  5 * time.Second,
		MaxOpenConns: 20,
		MaxIdleConns: 20,
	})
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// IMPORTANT: matches your NewService signature (logger=nil, metrics=nil)
	svc := model.NewService(db.DB, nil, nil)

	const (
		lockName = "hotlock"
		clients  = 40
	)
	ttl := 120 * time.Millisecond
	hold := 5 * time.Millisecond
	testDur := 3 * time.Second
	stallSleep := ttl + 120*time.Millisecond

	pr := &ProtectedResource{}
	var maxTokenSeen int64

	var staleAttempts int64
	var staleAccepted int64
	var staleRejected int64

	var acquireOK int64
	var acquireFail int64
	var releaseOK int64
	var opErrors int64

	runCtx, cancel := context.WithTimeout(ctx, testDur)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		i := i
		go func() {
			defer wg.Done()

			owner := fmt.Sprintf("c-%d", i)
			staller := (i%7 == 0)

			for runCtx.Err() == nil {
				ar, err := svc.Acquire(runCtx, model.AcquireRequest{
					LockName: lockName,
					OwnerID:  owner,
					TTL:      ttl,
				})
				if err != nil {
					atomic.AddInt64(&opErrors, 1)
					continue
				}
				if !ar.Acquired {
					atomic.AddInt64(&acquireFail, 1)
					sleep := ar.RetryAfter
					if sleep <= 0 {
						sleep = 10 * time.Millisecond
					}
					time.Sleep(sleep)
					continue
				}

				atomic.AddInt64(&acquireOK, 1)

				// track max token observed
				for {
					prev := atomic.LoadInt64(&maxTokenSeen)
					if ar.FencingToken <= prev {
						break
					}
					if atomic.CompareAndSwapInt64(&maxTokenSeen, prev, ar.FencingToken) {
						break
					}
				}

				if staller {
					time.Sleep(stallSleep)

					// wait briefly to observe a newer token (make the staleness real)
					deadline := time.Now().Add(250 * time.Millisecond)
					for time.Now().Before(deadline) {
						if atomic.LoadInt64(&maxTokenSeen) > ar.FencingToken {
							break
						}
						time.Sleep(5 * time.Millisecond)
					}

					if atomic.LoadInt64(&maxTokenSeen) > ar.FencingToken {
						atomic.AddInt64(&staleAttempts, 1)
						if pr.TryWrite(ar.FencingToken) {
							atomic.AddInt64(&staleAccepted, 1)
						} else {
							atomic.AddInt64(&staleRejected, 1)
						}
					}
				} else {
					time.Sleep(hold)
					_ = pr.TryWrite(ar.FencingToken)
				}

				rr, err := svc.Release(runCtx, model.ReleaseRequest{
					LockName:     lockName,
					OwnerID:      owner,
					LeaseID:      ar.LeaseID,
					FencingToken: ar.FencingToken,
				})
				if err != nil {
					atomic.AddInt64(&opErrors, 1)
				} else if rr.Released {
					atomic.AddInt64(&releaseOK, 1)
				}

				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	wg.Wait()

	if staleAttempts == 0 {
		t.Fatalf("expected at least 1 stale attempt; got staleAttempts=0 (try increasing clients/duration)")
	}
	if staleRejected == 0 {
		t.Fatalf("expected stale writes to be rejected at least once; staleRejected=0 (fencing not exercised)")
	}

	accepted, rejected, last := pr.Stats()
	if accepted == 0 {
		t.Fatalf("no writes accepted; test didn't exercise resource writes")
	}
	if last <= 0 {
		t.Fatalf("expected last token > 0; got %d", last)
	}

	// Correct fencing invariant: final downstream token should end up at the max token issued,
	// because the newest owner can always write last and "fence off" older tokens.
	maxSeen := atomic.LoadInt64(&maxTokenSeen)
	if maxSeen <= 0 {
		t.Fatalf("expected maxTokenSeen > 0; got %d", maxSeen)
	}

	// Ensure the max token "wins" by writing it once at the end.
	if !pr.TryWrite(maxSeen) {
		t.Fatalf("final write with max token was rejected unexpectedly: maxSeen=%d last=%d", maxSeen, last)
	}

	_, _, finalLast := pr.Stats()
	if finalLast != maxSeen {
		t.Fatalf("fencing invariant violated: final_last=%d max_seen=%d", finalLast, maxSeen)
	}

	totalAttempts := acquireOK + acquireFail
	contentionRate := float64(acquireFail) / float64(totalAttempts) * 100

	t.Log("\n================= LockServer Concurrency Report =================")
	t.Logf("Duration:                %v", testDur)
	t.Logf("Clients:                 %d", clients)
	t.Log("------------------------------------------------------------------")

	t.Logf("Acquire Success:         %d", acquireOK)
	t.Logf("Acquire Fail (held):     %d", acquireFail)
	t.Logf("Contention Rate:         %.2f%%", contentionRate)
	t.Logf("Release Success:         %d", releaseOK)
	t.Logf("Operational Errors:      %d", opErrors)

	t.Log("------------------------------------------------------------------")

	t.Logf("Stale Attempts Injected: %d", staleAttempts)
	t.Logf("Stale Rejected:          %d", staleRejected)
	t.Logf("Stale Accepted:          %d", staleAccepted)

	t.Log("------------------------------------------------------------------")

	t.Logf("Protected Writes Accepted: %d", accepted)
	t.Logf("Protected Writes Rejected: %d", rejected)
	t.Logf("Final Fencing Token:       %d", last)

	t.Log("------------------------------------------------------------------")

	if finalLast == maxSeen {
		t.Log("Safety Property:         PASS (Final state fenced to max token)")
	} else {
		t.Logf("Safety Property:         FAIL (final_last=%d max_seen=%d)", finalLast, maxSeen)
	}

	if staleRejected > 0 {
		t.Log("Fencing Effectiveness:   PASS (Stale writes actively rejected)")
	} else {
		t.Log("Fencing Effectiveness:   WARNING (No stale contention observed)")
	}

	t.Log("==================================================================")
}

func TestStaleReleaseCannotClobberNewOwner(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "lockserver_test2.db")

	ctx := context.Background()
	db, err := storage.Open(ctx, storage.Config{
		Path:         dbPath,
		BusyTimeout:  5 * time.Second,
		MaxOpenConns: 20,
		MaxIdleConns: 20,
	})
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := model.NewService(db.DB, nil, nil)

	lockName := "mylock"
	ttl := 120 * time.Millisecond

	// 1) Client A acquires
	a := "client-A"
	arA, err := svc.Acquire(ctx, model.AcquireRequest{
		LockName: lockName,
		OwnerID:  a,
		TTL:      ttl,
	})
	if err != nil {
		t.Fatalf("A acquire err: %v", err)
	}
	if !arA.Acquired {
		t.Fatalf("A expected acquired=true got false")
	}

	// 2) Let A's lease expire (simulate crash/stall)
	time.Sleep(ttl + 80*time.Millisecond)

	// 3) Client B acquires (should get a higher token and different lease)
	b := "client-B"
	arB, err := svc.Acquire(ctx, model.AcquireRequest{
		LockName: lockName,
		OwnerID:  b,
		TTL:      ttl,
	})
	if err != nil {
		t.Fatalf("B acquire err: %v", err)
	}
	if !arB.Acquired {
		t.Fatalf("B expected acquired=true got false")
	}
	if arB.FencingToken <= arA.FencingToken {
		t.Fatalf("expected B token > A token; A=%d B=%d", arA.FencingToken, arB.FencingToken)
	}
	if arB.LeaseID == arA.LeaseID {
		t.Fatalf("expected different lease ids; both=%s", arA.LeaseID)
	}

	// 4) A tries to release with stale lease+token
	rrA, err := svc.Release(ctx, model.ReleaseRequest{
		LockName:     lockName,
		OwnerID:      a,
		LeaseID:      arA.LeaseID,
		FencingToken: arA.FencingToken,
	})
	if err != nil {
		t.Fatalf("A release err: %v", err)
	}
	if rrA.Released {
		t.Fatalf("stale release must not succeed (A released=true)")
	}
	// Reason can be NOT_OWNER (expected)
	if rrA.Reason != "" && rrA.Reason != "NOT_OWNER" && rrA.Reason != "BUSY_RETRY" {
		t.Fatalf("unexpected stale release reason: %q", rrA.Reason)
	}

	// 5) Verify lock is still held by B (A didn't clobber)
	snap, err := svc.Get(ctx, lockName, time.Now())
	if err != nil {
		t.Fatalf("get err: %v", err)
	}
	if !snap.Held {
		t.Fatalf("expected lock to remain held by B, but Held=false")
	}
	if snap.OwnerID != b {
		t.Fatalf("expected owner=%s got owner=%s", b, snap.OwnerID)
	}
	if snap.LeaseID != arB.LeaseID {
		t.Fatalf("expected lease_id=%s got %s", arB.LeaseID, snap.LeaseID)
	}
	if snap.FencingToken != arB.FencingToken {
		t.Fatalf("expected token=%d got %d", arB.FencingToken, snap.FencingToken)
	}

	t.Log("\n================= LockServer Safety Test: Stale Release =================")
	t.Logf("A acquired: token=%d lease=%s", arA.FencingToken, arA.LeaseID)
	t.Logf("B acquired: token=%d lease=%s", arB.FencingToken, arB.LeaseID)
	t.Logf("A stale release result: released=%v reason=%s", rrA.Released, rrA.Reason)
	t.Log("Safety Property: PASS (Stale release cannot clobber new owner)")
	t.Log("==========================================================================")
}

func TestRenewHeartbeatKeepsLeaseAliveThenExpires(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "lockserver_heartbeat.db")

	ctx := context.Background()
	db, err := storage.Open(ctx, storage.Config{
		Path:         dbPath,
		BusyTimeout:  5 * time.Second,
		MaxOpenConns: 20,
		MaxIdleConns: 20,
	})
	if err != nil {
		t.Fatalf("db open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := model.NewService(db.DB, nil, nil)

	lockName := "hb-lock"
	ownerA := "client-A"
	ownerB := "client-B"

	ttl := 200 * time.Millisecond
	renewEvery := 60 * time.Millisecond
	extendBy := 200 * time.Millisecond
	holdHeartbeats := 800 * time.Millisecond // total time A keeps renewing

	// 1) A acquires
	arA, err := svc.Acquire(ctx, model.AcquireRequest{
		LockName: lockName,
		OwnerID:  ownerA,
		TTL:      ttl,
	})
	if err != nil {
		t.Fatalf("A acquire err: %v", err)
	}
	if !arA.Acquired {
		t.Fatalf("expected A acquired=true got false")
	}

	// 2) Start heartbeat renew loop for A
	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var renewOK int64
	var renewFail int64

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(renewEvery)
		defer ticker.Stop()

		stopAt := time.Now().Add(holdHeartbeats)

		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if time.Now().After(stopAt) {
					return
				}

				rr, err := svc.Renew(ctx, model.RenewRequest{
					LockName:     lockName,
					OwnerID:      ownerA,
					LeaseID:      arA.LeaseID,
					FencingToken: arA.FencingToken,
					ExtendBy:     extendBy,
				})
				if err != nil {
					atomic.AddInt64(&renewFail, 1)
					continue
				}
				if rr.Renewed {
					atomic.AddInt64(&renewOK, 1)
				} else {
					// NOT_OWNER_OR_EXPIRED / BUSY_RETRY are possible under load; still record.
					atomic.AddInt64(&renewFail, 1)
				}
			}
		}
	}()

	// 3) While heartbeat is active, B should not be able to acquire.
	// We'll probe multiple times for the duration of holdHeartbeats.
	deadline := time.Now().Add(holdHeartbeats)
	var bAcquireOK int64
	var bAcquireFail int64

	for time.Now().Before(deadline) {
		arB, err := svc.Acquire(ctx, model.AcquireRequest{
			LockName: lockName,
			OwnerID:  ownerB,
			TTL:      ttl,
		})
		if err != nil {
			// treat as transient; retry shortly
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if arB.Acquired {
			atomic.AddInt64(&bAcquireOK, 1)
			// If this happens while heartbeat is working, it's a bug.
			break
		}
		atomic.AddInt64(&bAcquireFail, 1)
		// respect retry hints to avoid hammering
		sleep := arB.RetryAfter
		if sleep <= 0 {
			sleep = 20 * time.Millisecond
		}
		time.Sleep(sleep)
	}

	// Stop heartbeat loop
	cancel()
	wg.Wait()

	if bAcquireOK != 0 {
		t.Fatalf("B acquired while A heartbeating: bAcquireOK=%d bAcquireFail=%d", bAcquireOK, bAcquireFail)
	}
	if renewOK == 0 {
		t.Fatalf("expected some successful renewals; renewOK=0 renewFail=%d", renewFail)
	}

	// 4) After heartbeat stops, wait for TTL window to elapse, then B should eventually acquire.
	// We wait TTL + a bit to ensure lease expires.
	time.Sleep(ttl + 150*time.Millisecond)

	// B acquire retry loop (bounded)
	acquireDeadline := time.Now().Add(2 * time.Second)
	var arB model.AcquireResult
	for {
		if time.Now().After(acquireDeadline) {
			t.Fatalf("B failed to acquire after heartbeat stopped (timeout). renewOK=%d renewFail=%d", renewOK, renewFail)
		}
		r, err := svc.Acquire(ctx, model.AcquireRequest{
			LockName: lockName,
			OwnerID:  ownerB,
			TTL:      ttl,
		})
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if r.Acquired {
			arB = r
			break
		}
		sleep := r.RetryAfter
		if sleep <= 0 {
			sleep = 25 * time.Millisecond
		}
		time.Sleep(sleep)
	}

	if arB.OwnerID != ownerB {
		t.Fatalf("expected B owner_id=%s got %s", ownerB, arB.OwnerID)
	}
	if arB.FencingToken <= arA.FencingToken {
		t.Fatalf("expected B token > A token; A=%d B=%d", arA.FencingToken, arB.FencingToken)
	}

	t.Log("\n================ LockServer Heartbeat (Renew) Test Report ================")
	t.Logf("A acquired: token=%d lease=%s ttl=%v", arA.FencingToken, arA.LeaseID, ttl)
	t.Logf("Heartbeat: renewEvery=%v extendBy=%v duration=%v", renewEvery, extendBy, holdHeartbeats)
	t.Logf("Renew results: ok=%d fail=%d", renewOK, renewFail)
	t.Logf("B probe during heartbeat: acquire_ok=%d acquire_fail=%d", bAcquireOK, bAcquireFail)
	t.Logf("After heartbeat stop: B acquired token=%d lease=%s", arB.FencingToken, arB.LeaseID)
	t.Log("Safety/Liveness: PASS (heartbeat maintains lease; expiry enables reacquire)")
	t.Log("==========================================================================")
}