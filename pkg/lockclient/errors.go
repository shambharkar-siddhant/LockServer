package lockclient

import "fmt"

type NotAcquiredError struct {
	LockName          string
	Reason            string // HELD | BUSY_RETRY (from server, if provided)
	RecommendedRetry  int64  // ms
	CurrentOwnerID    string
	CurrentExpiryMS   int64
}

func (e *NotAcquiredError) Error() string {
	return fmt.Sprintf("lock not acquired: lock=%s reason=%s retry_ms=%d current_owner=%s",
		e.LockName, e.Reason, e.RecommendedRetry, e.CurrentOwnerID)
}

type UnexpectedStatusError struct {
	Method string
	Path   string
	Code   int
	Body   string
}

func (e *UnexpectedStatusError) Error() string {
	return fmt.Sprintf("unexpected status: %s %s -> %d body=%q", e.Method, e.Path, e.Code, e.Body)
}