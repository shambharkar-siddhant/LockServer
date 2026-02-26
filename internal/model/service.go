package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"lockserver/internal/obs"
	"github.com/mattn/go-sqlite3"
)

type Service struct {
    db      *sql.DB
    logger  *obs.Logger
    metrics *obs.Metrics
}

func NewService(db *sql.DB, logger *obs.Logger, metrics *obs.Metrics) *Service {
    return &Service{
        db:      db,
        logger:  logger,
        metrics: metrics,
    }
}

func (s *Service) observeLatency(op string, start time.Time) {
	if s.metrics == nil {
		return
	}
	s.metrics.OpLatencyMS.WithLabelValues(op).Observe(float64(time.Since(start).Milliseconds()))
}

func (s *Service) incResult(op, result string) {
	if s.metrics == nil {
		return
	}
	switch op {
	case "acquire":
		s.metrics.AcquireTotal.WithLabelValues(result).Inc()
	case "renew":
		s.metrics.RenewTotal.WithLabelValues(result).Inc()
	case "release":
		s.metrics.ReleaseTotal.WithLabelValues(result).Inc()
	}
}

func (s *Service) now(reqNow time.Time) time.Time {
	if !reqNow.IsZero() {
		return reqNow
	}
	return time.Now()
}

func isSQLiteBusy(err error) bool {
    if err == nil {
        return false
    }
    if se, ok := err.(sqlite3.Error); ok {
        return se.Code == sqlite3.ErrBusy ||
               se.Code == sqlite3.ErrLocked
    }
    return false
}

func (s *Service) Acquire(ctx context.Context, req AcquireRequest) (AcquireResult, error) {
	if req.LockName == "" || req.OwnerID == "" {
		return AcquireResult{}, fmt.Errorf("lock_name and owner_id required")
	}
	if req.TTL <= 0 {
		return AcquireResult{}, fmt.Errorf("ttl must be > 0")
	}
	start := time.Now()

	// for logging
	var (
		logAcquired      bool
		logCurrentOwner  string
		logLeaseID       string
		logToken         int64
		logErrMsg        string
	)
	defer func() {
		if s.logger == nil {
			return
		}
		fields := map[string]interface{}{
			"op":         "acquire",
			"lock":       req.LockName,
			"owner":      req.OwnerID,
			"acquired":   logAcquired,
			"lease_id":   logLeaseID,
			"token":      logToken,
			"cur_owner":  logCurrentOwner,
			"latency_ms": time.Since(start).Milliseconds(),
		}
		if logErrMsg != "" {
			fields["error"] = logErrMsg
			s.logger.Error(fields)
		} else {
			s.logger.Info(fields)
		}
	}()

	now := s.now(req.Now)
	nowNs := now.UnixNano()
	expiryNs := now.Add(req.TTL).UnixNano()

	leaseID := uuid.NewString()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("acquire").Inc()
			}
			s.incResult("acquire", "busy")
			s.observeLatency("acquire", start)
			return AcquireResult{
				Acquired:   false,
				LockName:   req.LockName,
				RetryAfter: 50 * time.Millisecond,
			}, nil
		}
		logErrMsg = err.Error()
		return AcquireResult{}, err
	}

	defer func() { _ = tx.Rollback() }()

	// Read current lock row (if any)
	var (
		curOwner sql.NullString
		curLease sql.NullString
		curToken int64
		curExpNs int64
		curVer   int64
	)
	err = tx.QueryRowContext(ctx, `
SELECT owner_id, lease_id, fencing_token, lease_expiry_ns, version
FROM locks WHERE lock_name = ?;
`, req.LockName).Scan(&curOwner, &curLease, &curToken, &curExpNs, &curVer)

	notFound := errors.Is(err, sql.ErrNoRows)
	if err != nil && !notFound {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("acquire").Inc()
			}
			return AcquireResult{
				Acquired:   false,
				LockName:   req.LockName,
				RetryAfter: 50 * time.Millisecond,
			}, nil
		}
		logErrMsg = err.Error()
		return AcquireResult{}, err
	}

	// If lock exists and is unexpired => fail acquire
	if !notFound && curExpNs > nowNs && curOwner.Valid {
		logAcquired = false
		if curOwner.Valid {
			logCurrentOwner = curOwner.String
		}
		retry := recommendedRetry(nowNs, curExpNs)
		s.incResult("acquire", "fail")
		s.observeLatency("acquire", start)
		return AcquireResult{
			Acquired:       false,
			LockName:       req.LockName,
			CurrentOwnerID: curOwner.String,
			CurrentExpiry:  time.Unix(0, curExpNs),
			RetryAfter:     retry,
		}, tx.Commit()
	}

	// Ensure token row exists
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tokens(lock_name, last_token) VALUES(?, 0)
ON CONFLICT(lock_name) DO NOTHING;
`, req.LockName); err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("acquire").Inc()
			}
			s.incResult("acquire", "busy")
			s.observeLatency("acquire", start)
			return AcquireResult{
				Acquired:   false,
				LockName:   req.LockName,
				RetryAfter: 50 * time.Millisecond,
			}, nil
		}
		logErrMsg = err.Error()
		return AcquireResult{}, err
	}

	// Increment token (monotonic fencing token)
	if _, err := tx.ExecContext(ctx, `
UPDATE tokens SET last_token = last_token + 1 WHERE lock_name = ?;
`, req.LockName); err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("acquire").Inc()
			}
			s.incResult("acquire", "busy")
			s.observeLatency("acquire", start)
			return AcquireResult{
				Acquired:   false,
				LockName:   req.LockName,
				RetryAfter: 50 * time.Millisecond,
			}, nil
		}
		logErrMsg = err.Error()
		return AcquireResult{}, err
	}

	var newToken int64
	if err := tx.QueryRowContext(ctx, `
SELECT last_token FROM tokens WHERE lock_name = ?;
`, req.LockName).Scan(&newToken); err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("acquire").Inc()
			}
			s.incResult("acquire", "busy")
			s.observeLatency("acquire", start)
			return AcquireResult{
				Acquired:   false,
				LockName:   req.LockName,
				RetryAfter: 50 * time.Millisecond,
			}, nil
		}
		logErrMsg = err.Error()
		return AcquireResult{}, err
	}

	// Upsert lock row to new owner (atomic)
	_, err = tx.ExecContext(ctx, `
INSERT INTO locks(lock_name, owner_id, lease_id, fencing_token, lease_expiry_ns, version, created_at_ns, updated_at_ns)
VALUES(?, ?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(lock_name) DO UPDATE SET
  owner_id = excluded.owner_id,
  lease_id = excluded.lease_id,
  fencing_token = excluded.fencing_token,
  lease_expiry_ns = excluded.lease_expiry_ns,
  version = locks.version + 1,
  updated_at_ns = excluded.updated_at_ns;
`, req.LockName, req.OwnerID, leaseID, newToken, expiryNs, nowNs, nowNs)
	if err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("acquire").Inc()
			}
			s.incResult("acquire", "busy")
			s.observeLatency("acquire", start)
			return AcquireResult{
				Acquired:   false,
				LockName:   req.LockName,
				RetryAfter: 50 * time.Millisecond,
			}, nil
		}
		logErrMsg = err.Error()
		return AcquireResult{}, err
	}

	if err := tx.Commit(); err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("acquire").Inc()
			}
			s.incResult("acquire", "busy")
			s.observeLatency("acquire", start)
			return AcquireResult{
				Acquired:   false,
				LockName:   req.LockName,
				RetryAfter: 50 * time.Millisecond,
			}, nil
		}
		logErrMsg = err.Error()
		return AcquireResult{}, err
	}

	logAcquired = true
	logLeaseID = leaseID
	logToken = newToken

	s.incResult("acquire", "success")
	s.observeLatency("acquire", start)

	return AcquireResult{
		Acquired:     true,
		LockName:     req.LockName,
		OwnerID:      req.OwnerID,
		LeaseID:      leaseID,
		FencingToken: newToken,
		LeaseExpiry:  time.Unix(0, expiryNs),
	}, nil
}

func (s *Service) Renew(ctx context.Context, req RenewRequest) (RenewResult, error) {
	if req.LockName == "" || req.OwnerID == "" || req.LeaseID == "" {
		return RenewResult{}, fmt.Errorf("lock_name, owner_id, lease_id required")
	}
	if req.FencingToken <= 0 {
		return RenewResult{}, fmt.Errorf("fencing_token must be > 0")
	}
	if req.ExtendBy <= 0 {
		return RenewResult{}, fmt.Errorf("extend_by must be > 0")
	}

	start := time.Now()
	var logRenewed bool
	var logErrMsg string
	defer func() {
		if s.logger == nil {
			return
		}
		fields := map[string]interface{}{
			"op":         "renew",
			"lock":       req.LockName,
			"owner":      req.OwnerID,
			"lease_id":   req.LeaseID,
			"token":      req.FencingToken,
			"renewed":    logRenewed,
			"latency_ms": time.Since(start).Milliseconds(),
		}
		if logErrMsg != "" {
			fields["error"] = logErrMsg
			s.logger.Error(fields)
		} else {
			s.logger.Info(fields)
		}
	}()

	now := s.now(req.Now)
	nowNs := now.UnixNano()
	newExpNs := now.Add(req.ExtendBy).UnixNano()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("renew").Inc()
			}
			return RenewResult{
				Renewed: false,
				Reason:  "BUSY_RETRY",
			}, nil
		}
		logErrMsg = err.Error()
		return RenewResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Renew only if still owner, same lease+token, and not expired at evaluation time
	res, err := tx.ExecContext(ctx, `
UPDATE locks
SET lease_expiry_ns = CASE
  WHEN lease_expiry_ns > ? THEN MAX(lease_expiry_ns, ?)
  ELSE lease_expiry_ns
END,
    version = version + 1,
    updated_at_ns = ?
WHERE lock_name = ?
  AND owner_id = ?
  AND lease_id = ?
  AND fencing_token = ?
  AND lease_expiry_ns > ?;
`, nowNs, newExpNs, nowNs, req.LockName, req.OwnerID, req.LeaseID, req.FencingToken, nowNs)
	if err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("renew").Inc()
			}
			return RenewResult{
				Renewed: false,
				Reason:  "BUSY_RETRY",
			}, nil
		}
		logErrMsg = err.Error()
		return RenewResult{}, err
	}

	aff, _ := res.RowsAffected()
	if aff != 1 {
		_ = tx.Commit()
		return RenewResult{Renewed: false, Reason: "NOT_OWNER_OR_EXPIRED"}, nil
	}

	var expNs int64
	if err := tx.QueryRowContext(ctx, `SELECT lease_expiry_ns FROM locks WHERE lock_name = ?;`, req.LockName).Scan(&expNs); err != nil {
		logErrMsg = err.Error()
		return RenewResult{}, err
	}

	if err := tx.Commit(); err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("renew").Inc()
			}
			return RenewResult{
				Renewed: false,
				Reason:  "BUSY_RETRY",
			}, nil
		}
		logErrMsg = err.Error()
		return RenewResult{}, err
	}
	logRenewed = true
	return RenewResult{Renewed: true, LeaseExpiry: time.Unix(0, expNs)}, nil
}

func (s *Service) Release(ctx context.Context, req ReleaseRequest) (ReleaseResult, error) {
	if req.LockName == "" || req.OwnerID == "" || req.LeaseID == "" {
		return ReleaseResult{}, fmt.Errorf("lock_name, owner_id, lease_id required")
	}
	if req.FencingToken <= 0 {
		return ReleaseResult{}, fmt.Errorf("fencing_token must be > 0")
	}

	start := time.Now()
	var logReleased bool
	var logErrMsg string
	defer func() {
		if s.logger == nil {
			return
		}
		fields := map[string]interface{}{
			"op":         "release",
			"lock":       req.LockName,
			"owner":      req.OwnerID,
			"lease_id":   req.LeaseID,
			"token":      req.FencingToken,
			"released":   logReleased,
			"latency_ms": time.Since(start).Milliseconds(),
		}
		if logErrMsg != "" {
			fields["error"] = logErrMsg
			s.logger.Error(fields)
		} else {
			s.logger.Info(fields)
		}
	}()

	now := s.now(req.Now)
	nowNs := now.UnixNano()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("release").Inc()
			}
			return ReleaseResult{
				Released: false,
				Reason:   "BUSY_RETRY",
			}, nil
		}
		logErrMsg = err.Error()
		return ReleaseResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
UPDATE locks
SET owner_id = NULL,
    lease_id = NULL,
    lease_expiry_ns = 0,
    version = version + 1,
    updated_at_ns = ?
WHERE lock_name = ?
  AND owner_id = ?
  AND lease_id = ?
  AND fencing_token = ?;
`, nowNs, req.LockName, req.OwnerID, req.LeaseID, req.FencingToken)
	if err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("release").Inc()
			}
			return ReleaseResult{
				Released: false,
				Reason:   "BUSY_RETRY",
			}, nil
		}
		logErrMsg = err.Error()
		return ReleaseResult{}, err
	}

	aff, _ := res.RowsAffected()
	if err := tx.Commit(); err != nil {
		if isSQLiteBusy(err) {
			if s.metrics != nil {
				s.metrics.DBBusyTotal.WithLabelValues("release").Inc()
			}
			return ReleaseResult{
				Released: false,
				Reason:   "BUSY_RETRY",
			}, nil
		}
		logErrMsg = err.Error()
		return ReleaseResult{}, err
	}

	if aff == 1 {
		logReleased = true
		return ReleaseResult{Released: true}, nil
	}
	return ReleaseResult{Released: false, Reason: "NOT_OWNER"}, nil
}

func (s *Service) Get(ctx context.Context, lockName string, now time.Time) (LockSnapshot, error) {
	if lockName == "" {
		return LockSnapshot{}, fmt.Errorf("lock_name required")
	}
	n := s.now(now).UnixNano()

	var (
		owner sql.NullString
		lease sql.NullString
		token int64
		expNs int64
		ver   int64
	)

	err := s.db.QueryRowContext(ctx, `
SELECT owner_id, lease_id, fencing_token, lease_expiry_ns, version
FROM locks WHERE lock_name = ?;
`, lockName).Scan(&owner, &lease, &token, &expNs, &ver)
	if errors.Is(err, sql.ErrNoRows) {
		return LockSnapshot{LockName: lockName, Held: false}, nil
	}
	if err != nil {
		return LockSnapshot{}, err
	}

	held := owner.Valid && expNs > n
	return LockSnapshot{
		LockName:     lockName,
		Held:         held,
		OwnerID:      owner.String,
		LeaseID:      lease.String,
		FencingToken: token,
		LeaseExpiry:  time.Unix(0, expNs),
		Version:      ver,
	}, nil
}

func recommendedRetry(nowNs, expiryNs int64) time.Duration {
	// Simple policy for v0: retry around expiry/4 with floor and ceiling.
	until := time.Duration(expiryNs-nowNs) * time.Nanosecond
	if until < 0 {
		until = 0
	}
	h := until / 4
	if h < 25*time.Millisecond {
		h = 25 * time.Millisecond
	}
	if h > 1*time.Second {
		h = 1 * time.Second
	}
	// jitter added later in client SDK
	return h
}