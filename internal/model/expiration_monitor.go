package model

import (
	"context"
	"database/sql"
	"time"

	"lockserver/internal/obs"
)

type ExpirationMonitor struct {
	db       *sql.DB
	logger   *obs.Logger
	metrics  *obs.Metrics
	interval time.Duration
}

// NewExpirationMonitor creates a periodic sweeper that:
// 1) counts unexpired held locks -> sets gauge
// 2) clears expired locks (optional hygiene) -> increments expired_total by rows changed
func NewExpirationMonitor(db *sql.DB, logger *obs.Logger, metrics *obs.Metrics, interval time.Duration) *ExpirationMonitor {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &ExpirationMonitor{
		db:       db,
		logger:   logger,
		metrics:  metrics,
		interval: interval,
	}
}

func (m *ExpirationMonitor) Run(ctx context.Context) {
	t := time.NewTicker(m.interval)
	defer t.Stop()

	// Run once immediately
	m.sweepOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.sweepOnce(ctx)
		}
	}
}

func (m *ExpirationMonitor) sweepOnce(ctx context.Context) {
	start := time.Now()
	nowNs := time.Now().UnixNano()

	var heldCount int64
	// Count currently held locks (unexpired + owner set)
	// (lease_expiry_ns > now and owner_id not null)
	err := m.db.QueryRowContext(ctx, `
SELECT COUNT(*) 
FROM locks
WHERE owner_id IS NOT NULL
  AND lease_expiry_ns > ?;
`, nowNs).Scan(&heldCount)

	if err == nil && m.metrics != nil && m.metrics.LocksHeld != nil {
		m.metrics.LocksHeld.Set(float64(heldCount))
	}

	// Hygiene: clear expired leases so /get is cleaner (optional but nice)
	// Only clear rows where owner_id is set AND lease expired.
	res, err2 := m.db.ExecContext(ctx, `
UPDATE locks
SET owner_id = NULL,
    lease_id = NULL,
    lease_expiry_ns = 0,
    version = version + 1,
    updated_at_ns = ?
WHERE owner_id IS NOT NULL
  AND lease_expiry_ns > 0
  AND lease_expiry_ns <= ?;
`, nowNs, nowNs)

	var cleared int64
	if err2 == nil && res != nil {
		cleared, _ = res.RowsAffected()
		if cleared > 0 && m.metrics != nil && m.metrics.ExpiredTotal != nil {
			m.metrics.ExpiredTotal.Add(float64(cleared))
		}
	}

	// Structured log
	if m.logger != nil {
		fields := map[string]interface{}{
			"op":          "expire_sweep",
			"held":        heldCount,
			"cleared":     cleared,
			"latency_ms":  time.Since(start).Milliseconds(),
		}
		if err != nil {
			fields["count_err"] = err.Error()
		}
		if err2 != nil {
			fields["clear_err"] = err2.Error()
		}
		// Only log at Info if something interesting happened or errors
		if cleared > 0 || err != nil || err2 != nil {
			m.logger.Info(fields)
		}
	}
}