package model

import "time"

type AcquireRequest struct {
	LockName string
	OwnerID  string
	TTL      time.Duration
	Now      time.Time // injected for testability; if zero, service uses time.Now()
}

type AcquireResult struct {
	Acquired       bool
	LockName       string
	OwnerID        string
	LeaseID        string
	FencingToken   int64
	LeaseExpiry    time.Time
	CurrentOwnerID string
	CurrentExpiry  time.Time
	RetryAfter     time.Duration
}

type RenewRequest struct {
	LockName    string
	OwnerID     string
	LeaseID     string
	FencingToken int64
	ExtendBy    time.Duration
	Now         time.Time
}

type RenewResult struct {
	Renewed    bool
	LeaseExpiry time.Time
	Reason     string // NOT_OWNER_OR_EXPIRED
}

type ReleaseRequest struct {
	LockName     string
	OwnerID      string
	LeaseID      string
	FencingToken int64
	Now          time.Time
}

type ReleaseResult struct {
	Released bool
	Reason   string // NOT_OWNER
}

type LockSnapshot struct {
	LockName     string
	Held         bool
	OwnerID      string
	LeaseID      string
	FencingToken int64
	LeaseExpiry  time.Time
	Version      int64
}