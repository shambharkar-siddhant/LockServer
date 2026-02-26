package lockclient

import "time"

// Lease is what the SDK returns on successful acquire.
// Keep it immutable-ish: consumers should pass it back to Renew/Release.
type Lease struct {
	LockName      string
	OwnerID       string
	LeaseID       string
	FencingToken  int64
	LeaseExpiryMS int64 // server-provided expiry; useful for debugging/telemetry
}

// FencingToken exposes the token for downstream writes.
func (l Lease) FencingTokenValue() int64 { return l.FencingToken }

// AcquireOptions controls retry behavior and TTL.
type AcquireOptions struct {
	TTL           time.Duration // required
	MaxRetries    int           // bounded retry; 0 => default
	MaxTotalWait  time.Duration // optional global cap; 0 => no cap
	MinRetry      time.Duration // default 25ms
	MaxRetry      time.Duration // default 1s
	JitterFrac    float64       // default 0.2 (20%)
}

// HeartbeatOptions controls renew behavior.
type HeartbeatOptions struct {
	Interval time.Duration // required; typically TTL/3
	ExtendBy time.Duration // required; typically TTL
}