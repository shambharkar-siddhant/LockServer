package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"lockserver/internal/model"
	"context"
	"github.com/google/uuid"
)


type Server struct {
	svc *model.Service
	mux *http.ServeMux
}

type contextKey string

const requestIDKey contextKey = "req_id"

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = uuid.NewString()
		}
		ctx := context.WithValue(r.Context(), requestIDKey, reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func NewServer(svc *model.Service) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return withRequestID(s.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Lock endpoints (simple path parsing to avoid extra router deps)
	s.mux.HandleFunc("/v1/locks/", s.handleLocks)
}

func (s *Server) handleLocks(w http.ResponseWriter, r *http.Request) {
	// Expected:
	// /v1/locks/{name}
	// /v1/locks/{name}/acquire
	// /v1/locks/{name}/renew
	// /v1/locks/{name}/release
	path := strings.TrimPrefix(r.URL.Path, "/v1/locks/")
	path = strings.Trim(path, "/")
	if path == "" {
		writeErr(w, http.StatusBadRequest, "lock_name required")
		return
	}

	parts := strings.Split(path, "/")
	lockName := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}
	if len(parts) > 2 {
		writeErr(w, http.StatusNotFound, "invalid path")
		return
	}

	switch r.Method {
	case http.MethodGet:
		if action != "" {
			writeErr(w, http.StatusNotFound, "invalid path")
			return
		}
		s.handleGet(w, r, lockName)
		return

	case http.MethodPost:
		switch action {
		case "acquire":
			s.handleAcquire(w, r, lockName)
			return
		case "renew":
			s.handleRenew(w, r, lockName)
			return
		case "release":
			s.handleRelease(w, r, lockName)
			return
		default:
			writeErr(w, http.StatusNotFound, "unknown action")
			return
		}

	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
}

// --- Handlers ---

type acquireReq struct {
	OwnerID string `json:"owner_id"`
	TTLMS   int64  `json:"ttl_ms"`
}

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
	Reason 			 string `json:"reason,omitempty"` 
}

func (s *Server) handleAcquire(w http.ResponseWriter, r *http.Request, lockName string) {
	var req acquireReq
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.OwnerID == "" {
		writeErr(w, http.StatusBadRequest, "owner_id required")
		return
	}
	if req.TTLMS <= 0 || req.TTLMS > 10*60*1000 {
		writeErr(w, http.StatusBadRequest, "ttl_ms must be in (0, 600000]")
		return
	}

	res, err := s.svc.Acquire(r.Context(), model.AcquireRequest{
		LockName: lockName,
		OwnerID:  req.OwnerID,
		TTL:      time.Duration(req.TTLMS) * time.Millisecond,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := acquireResp{
		Acquired: res.Acquired,
		LockName: lockName,
	}
	if res.Acquired {
		out.OwnerID = res.OwnerID
		out.LeaseID = res.LeaseID
		out.FencingToken = res.FencingToken
		out.LeaseExpiryMS = res.LeaseExpiry.UnixNano() / int64(time.Millisecond)
		writeJSON(w, http.StatusOK, out)
		return
	}

	// Not acquired
	out.CurrentOwnerID = res.CurrentOwnerID
	out.CurrentExpiryMS = res.CurrentExpiry.UnixNano() / int64(time.Millisecond)
	out.RecommendedRetry = int64(res.RetryAfter / time.Millisecond)

	if res.CurrentOwnerID != "" {
		out.Reason = "HELD"
	} else {
		out.Reason = "BUSY_RETRY"
	}
	writeJSON(w, http.StatusConflict, out)
}

type renewReq struct {
	OwnerID       string `json:"owner_id"`
	LeaseID       string `json:"lease_id"`
	FencingToken  int64  `json:"fencing_token"`
	ExtendByMS    int64  `json:"extend_by_ms"`
}

type renewResp struct {
	Renewed      bool  `json:"renewed"`
	LeaseExpiryMS int64 `json:"lease_expiry_ms,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request, lockName string) {
	var req renewReq
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.OwnerID == "" || req.LeaseID == "" || req.FencingToken <= 0 {
		writeErr(w, http.StatusBadRequest, "owner_id, lease_id, fencing_token required")
		return
	}
	if req.ExtendByMS <= 0 || req.ExtendByMS > 10*60*1000 {
		writeErr(w, http.StatusBadRequest, "extend_by_ms must be in (0, 600000]")
		return
	}

	res, err := s.svc.Renew(r.Context(), model.RenewRequest{
		LockName:     lockName,
		OwnerID:      req.OwnerID,
		LeaseID:      req.LeaseID,
		FencingToken: req.FencingToken,
		ExtendBy:     time.Duration(req.ExtendByMS) * time.Millisecond,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := renewResp{Renewed: res.Renewed, Reason: res.Reason}
	if res.Renewed {
		out.LeaseExpiryMS = res.LeaseExpiry.UnixNano() / int64(time.Millisecond)
		writeJSON(w, http.StatusOK, out)
		return
	}
	writeJSON(w, http.StatusConflict, out)
}

type releaseReq struct {
	OwnerID      string `json:"owner_id"`
	LeaseID      string `json:"lease_id"`
	FencingToken int64  `json:"fencing_token"`
}

type releaseResp struct {
	Released bool   `json:"released"`
	Reason   string `json:"reason,omitempty"`
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request, lockName string) {
	var req releaseReq
	if err := readJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.OwnerID == "" || req.LeaseID == "" || req.FencingToken <= 0 {
		writeErr(w, http.StatusBadRequest, "owner_id, lease_id, fencing_token required")
		return
	}

	res, err := s.svc.Release(r.Context(), model.ReleaseRequest{
		LockName:     lockName,
		OwnerID:      req.OwnerID,
		LeaseID:      req.LeaseID,
		FencingToken: req.FencingToken,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := releaseResp{Released: res.Released, Reason: res.Reason}
	writeJSON(w, http.StatusOK, out) // idempotent
}

type getResp struct {
	LockName     string `json:"lock_name"`
	Held         bool   `json:"held"`
	OwnerID      string `json:"owner_id,omitempty"`
	LeaseID      string `json:"lease_id,omitempty"`
	FencingToken int64  `json:"fencing_token,omitempty"`
	LeaseExpiryMS int64 `json:"lease_expiry_ms,omitempty"`
	Version      int64  `json:"version,omitempty"`
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, lockName string) {
	snap, err := s.svc.Get(r.Context(), lockName, time.Time{})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := getResp{
		LockName: lockName,
		Held:     snap.Held,
	}
	if snap.Held {
		out.OwnerID = snap.OwnerID
		out.LeaseID = snap.LeaseID
		out.FencingToken = snap.FencingToken
		out.LeaseExpiryMS = snap.LeaseExpiry.UnixNano() / int64(time.Millisecond)
		out.Version = snap.Version
	}
	writeJSON(w, http.StatusOK, out)
}

// --- helpers ---

func readJSON(r *http.Request, dst interface{}) error {
	if r.Body == nil {
		return errors.New("missing body")
	}
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}