package lockclient

import (
	"context"
	"time"
)

// Heartbeat runs renew periodically until ctx is cancelled.
// It returns a channel that emits the last error (if any) and then closes on exit.
// Semantics:
// - if Renew returns NOT_OWNER_OR_EXPIRED: heartbeat stops (lease is dead)
// - BUSY_RETRY: continue (transient)
// - ctx cancel: stop cleanly
func (c *Client) StartHeartbeat(ctx context.Context, l Lease, opt HeartbeatOptions) <-chan error {
	errCh := make(chan error, 1)

	if opt.Interval <= 0 {
		opt.Interval = 200 * time.Millisecond
	}
	if opt.ExtendBy <= 0 {
		opt.ExtendBy = 500 * time.Millisecond
	}

	go func() {
		defer close(errCh)

		t := time.NewTicker(opt.Interval)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_, renewed, reason, err := c.RenewOnce(ctx, l, opt.ExtendBy)
				if err != nil {
					// transient network errors: surface but keep running? choose one.
					// For infra SDK, better to surface and continue; caller may cancel.
					select {
					case errCh <- err:
					default:
					}
					continue
				}
				if renewed {
					continue
				}
				// If we lost ownership, stop: lease is no longer valid.
				if reason == "NOT_OWNER_OR_EXPIRED" {
					select {
					case errCh <- context.Canceled: // signals "lease dead"
					default:
					}
					return
				}
				// BUSY_RETRY or other non-fatal reasons: continue.
			}
		}
	}()

	return errCh
}