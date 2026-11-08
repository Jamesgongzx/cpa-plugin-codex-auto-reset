package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Auth health is observational: it never alters host eligibility or reset scheduling.
const healthInterval = 15 * time.Minute
const healthRetention = 14 * 24 * time.Hour
const healthMaxBytes = 128 << 20

type healthSummary struct {
	Generation             string    `json:"generation"`
	State                  string    `json:"state"`
	Code                   string    `json:"code"`
	LastChecked            time.Time `json:"last_checked"`
	LastAttempt            time.Time `json:"last_attempt"`
	LastRejected           time.Time `json:"last_rejected"`
	LastRejectedGeneration string    `json:"last_rejected_generation,omitempty"`
	LastSuccess            time.Time `json:"last_success"`
	LastSuccessGeneration  string    `json:"last_success_generation,omitempty"`
	FirstRejected          time.Time `json:"first_rejected"`
	LastRecovered          time.Time `json:"last_recovered"`
	PendingRecovery        bool      `json:"pending_recovery,omitempty"`
	WeeklyRemaining        int       `json:"weekly_remaining"`
	WeeklyObserved         time.Time `json:"weekly_observed"`
	WeeklyReset            time.Time `json:"weekly_reset"`
	CreditsAvailable       int       `json:"credits_available"`
	CreditsObserved        time.Time `json:"credits_observed"`
	lastStarted            time.Time
	lastObservedStarted    time.Time
	notBefore              time.Time
	acquisitionFailed      bool
}
type healthEvent struct {
	Time       time.Time `json:"time"`
	Account    string    `json:"account"`
	Generation string    `json:"generation"`
	Source     string    `json:"source"`
	Operation  string    `json:"operation"`
	Phase      string    `json:"phase,omitempty"`
	Code       string    `json:"code"`
	HTTPStatus int       `json:"http_status,omitempty"`
	Result     string    `json:"result,omitempty"`
}
type healthStore struct {
	mu       sync.Mutex
	dir      string
	now      func() time.Time
	rows     map[string]healthSummary
	report   func(error)
	maxBytes int64
}

func healthHash(s string) string {
	if s == "" {
		return ""
	}
	h := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(h[:])
}
func newHealthStore(dir string, now func() time.Time) (*healthStore, error) {
	if now == nil {
		now = time.Now
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	s := &healthStore{dir: dir, now: now, rows: map[string]healthSummary{}, report: func(err error) { fmt.Fprintf(os.Stderr, "codex-auto-reset: auth evidence write failed: %v\n", err) }, maxBytes: healthMaxBytes}
	raw, err := os.ReadFile(filepath.Join(dir, "health-summary.json"))
	if err == nil {
		if err = json.Unmarshal(raw, &s.rows); err != nil {
			return nil, fmt.Errorf("invalid health summary: %w", err)
		}
		if s.rows == nil {
			s.rows = map[string]healthSummary{}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return s, nil
}
func (s *healthStore) snapshot() map[string]healthSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]healthSummary, len(s.rows))
	for k, v := range s.rows {
		out[k] = v
	}
	return out
}

// InvalidResponseError means a provider response was received but its body failed parsing.
type InvalidResponseError struct{ Err error }

func (e *InvalidResponseError) Error() string { return "invalid provider response" }
func (e *InvalidResponseError) Unwrap() error { return e.Err }
func classifyHealth(err error) (string, int) {
	if err == nil {
		return "ok", 200
	}
	var he *HTTPStatusError
	if errors.As(err, &he) {
		if he.Code == 401 {
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if json.Unmarshal(he.Body, &body) == nil {
				switch body.Error.Code {
				case "token_revoked", "token_expired", "invalid_token", "token_invalidated", "refresh_token_invalidated", "refresh_token_reused", "refresh_token_expired", "invalid_grant":
					return body.Error.Code, 401
				}
			}
			return "auth_failure", 401
		}
		if he.Code == 403 {
			return "permission_denied", 403
		}
		if he.Code == 429 {
			return "quota_exceeded", 429
		}
		if he.Code >= 500 {
			return "server_error", he.Code
		}
		return "http_error", he.Code
	}
	var invalid *InvalidResponseError
	if errors.As(err, &invalid) {
		return "invalid_response", 200
	}
	return "transport_error", 0
}
func (s *healthStore) observe(id string, creds CodexCredentials, source, op string, snap Snapshot, err error) {
	acquired := s.now()
	s.beginGeneration(id, creds, acquired)
	s.observeStarted(id, creds, source, op, "", "", snap, err, acquired)
}
func (s *healthStore) observeStarted(id string, creds CodexCredentials, source, op, phase, result string, snap Snapshot, err error, started time.Time) {
	s.observeAt(id, creds, source, op, phase, result, snap, err, started)
}
func (s *healthStore) observeAt(id string, creds CodexCredentials, source, op, phase, result string, snap Snapshot, err error, requestStarted time.Time) {
	code, status := classifyHealth(err)
	now := s.now().UTC()
	account, gen := healthHash(id), healthHash(creds.AccessToken)
	if account == "" || gen == "" {
		return
	}
	ev := healthEvent{Time: now, Account: account, Generation: gen, Source: source, Operation: op, Phase: phase, Code: code, HTTPStatus: status, Result: result}
	s.mu.Lock()
	defer s.mu.Unlock()
	// A different generation is fenced; the same token can be reloaded while
	// a request is in flight. Order those observations by request start instead.
	row := s.rows[account]
	// Preserve every observed 401 as history even when its request is too old
	// to change the current credential's health.
	if status == 401 && !now.Before(row.LastRejected) {
		row.LastRejected = now
		row.LastRejectedGeneration = gen
		s.rows[account] = row
	}
	if row.Generation == gen && !row.acquisitionFailed && !requestStarted.Before(row.notBefore) && !requestStarted.Before(row.lastObservedStarted) {
		row.lastObservedStarted = requestStarted
		row.LastAttempt = now
		row.LastChecked = now
		row.Code = code
		switch code {
		case "ok":
			if row.State == "rejected" || row.PendingRecovery {
				row.LastRecovered = now
			}
			row.PendingRecovery = false
			row.State = "valid"
			row.LastSuccess = now
			row.LastSuccessGeneration = gen
			row.FirstRejected = time.Time{}
		case "auth_failure", "token_revoked", "token_expired", "invalid_token", "token_invalidated", "refresh_token_invalidated", "refresh_token_reused", "refresh_token_expired", "invalid_grant":
			if row.FirstRejected.IsZero() {
				row.FirstRejected = now
			}
			row.State = "rejected"
		}
		if row.State == "" {
			row.State = "unknown"
		}
		if code == "ok" && op == "usage" && !snap.WeeklyObservedAt.IsZero() {
			row.WeeklyRemaining = snap.WeeklyPct
			row.WeeklyObserved = snap.WeeklyObservedAt.UTC()
			row.WeeklyReset = snap.WeeklyResetAt.UTC()
		}
		if code == "ok" && op == "credits" {
			row.CreditsAvailable = snap.AvailableCount
			row.CreditsObserved = now
		}
		s.rows[account] = row
	}

	if e := s.appendEvent(ev); e != nil {
		s.report(e)
	}
	if e := s.save(); e != nil {
		s.report(e)
	}
}

// beginGeneration is called immediately after credential acquisition. Its
// timestamp is captured before the host load begins, so a delayed old load or
// a later request using an old FSM credential cannot replace a newer grant.
func (s *healthStore) beginGeneration(id string, creds CodexCredentials, acquired time.Time) {
	account, gen := healthHash(id), healthHash(creds.AccessToken)
	if account == "" || gen == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.rows[account]
	if acquired.Before(row.lastStarted) {
		return
	}
	row.lastStarted = acquired
	if row.acquisitionFailed {
		row.notBefore = acquired
		row.acquisitionFailed = false
	}
	if row.Generation != gen {
		row.notBefore = acquired
		row.lastObservedStarted = time.Time{}
		row.PendingRecovery = row.PendingRecovery || row.State == "rejected"
		row.Generation = gen
		row.State = "unknown"
		row.Code = "credential_loaded"
		row.FirstRejected = time.Time{}
		row.LastAttempt = s.now().UTC()
	}
	s.rows[account] = row
	if err := s.save(); err != nil {
		s.report(err)
	}
}
func (s *healthStore) credentialUnavailable(id string, acquired time.Time) {
	now := s.now().UTC()
	account := healthHash(id)
	if account == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.rows[account]
	if !acquired.Before(row.lastStarted) {
		row.lastStarted = acquired
		row.acquisitionFailed = true
		row.notBefore = acquired
		row.PendingRecovery = row.PendingRecovery || row.State == "rejected"
		row.LastAttempt = now
		row.Code = "credential_unavailable"
		row.State = "unknown"
		s.rows[account] = row
	}
	if err := s.appendEvent(healthEvent{Time: now, Account: account, Source: "monitor", Operation: "credentials", Code: "credential_unavailable"}); err != nil {
		s.report(err)
	}
	if err := s.save(); err != nil {
		s.report(err)
	}
}

func (s *healthStore) save() error {
	raw, err := json.Marshal(s.rows)
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, "health-summary.tmp")
	if err = os.WriteFile(tmp, raw, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.dir, "health-summary.json"))
}
func (s *healthStore) appendEvent(ev healthEvent) error {
	// Rotate at 1 MiB, then prune oldest segments even during a high-traffic day.
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	type file struct {
		name string
		size int64
		date time.Time
	}
	var files []file
	var total int64
	for _, entry := range entries {
		n := entry.Name()
		if !strings.HasPrefix(n, "health-") || !strings.HasSuffix(n, ".jsonl") {
			continue
		}
		var date string
		var segment int
		if _, e := fmt.Sscanf(n, "health-%8s-%06d.jsonl", &date, &segment); e != nil {
			continue
		}
		d, e := time.Parse("20060102", date)
		if e != nil {
			continue
		}
		info, e := entry.Info()
		if e != nil {
			return e
		}
		files = append(files, file{n, info.Size(), d})
		total += info.Size()
	}
	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	prefix := "health-" + ev.Time.Format("20060102") + "-"
	seq := 0
	for _, item := range files {
		if strings.HasPrefix(item.name, prefix) {
			var n int
			if _, e := fmt.Sscanf(item.name, prefix+"%06d.jsonl", &n); e == nil && n >= seq {
				seq = n
				if item.size+int64(len(raw)) > 1<<20 {
					seq++
				}
			}
		}
	}
	name := fmt.Sprintf("%s%06d.jsonl", prefix, seq)
	path := filepath.Join(s.dir, name)
	for _, item := range files {
		if item.name != name && (item.date.Before(ev.Time.Add(-healthRetention)) || total+int64(len(raw)) > s.maxBytes) {
			if err = os.Remove(filepath.Join(s.dir, item.name)); err != nil {
				return err
			}
			total -= item.size
		}
	}
	if total+int64(len(raw)) > s.maxBytes {
		return errors.New("health journal size limit reached")
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	if e := f.Close(); err == nil {
		err = e
	}
	return err
}

type observedClient struct {
	inner             ResetClient
	store             *healthStore
	id, source, phase string
	acquired          time.Time
}

func (c *observedClient) GetUsage(creds CodexCredentials) (Snapshot, error) {
	start := c.acquired
	if start.IsZero() {
		start = time.Now()
		c.store.beginGeneration(c.id, creds, start)
	}
	requestStarted := time.Now()
	v, err := c.inner.GetUsage(creds)
	c.store.observeAt(c.id, creds, c.source, "usage", c.phase, "", v, err, requestStarted)
	return v, err
}
func (c *observedClient) ListCredits(creds CodexCredentials) (Snapshot, error) {
	start := c.acquired
	if start.IsZero() {
		start = time.Now()
		c.store.beginGeneration(c.id, creds, start)
	}
	requestStarted := time.Now()
	v, err := c.inner.ListCredits(creds)
	c.store.observeAt(c.id, creds, c.source, "credits", c.phase, "", v, err, requestStarted)
	return v, err
}
func (c *observedClient) recordVerification(creds CodexCredentials, result string) {
	switch result {
	case "verified", "partial", "mismatch", "error":
	default:
		return
	}
	code := "ok"
	if result != "verified" {
		code = "verification_failed"
	}
	ev := healthEvent{Time: c.store.now().UTC(), Account: healthHash(c.id), Generation: healthHash(creds.AccessToken), Source: "autoreset", Operation: "verification", Phase: "VERIFYING", Code: code, Result: result}
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	if err := c.store.appendEvent(ev); err != nil {
		c.store.report(err)
	}
}
func (c *observedClient) Consume(creds CodexCredentials, rid, cid string) (ConsumeResponse, error) {
	start := c.acquired
	if start.IsZero() {
		start = time.Now()
		c.store.beginGeneration(c.id, creds, start)
	}
	requestStarted := time.Now()
	v, err := c.inner.Consume(creds, rid, cid)
	result := ""
	switch v.Code {
	case ConsumeCodeReset, ConsumeCodeAlreadyRedeemed, ConsumeCodeNoCredit, ConsumeCodeNothingToReset:
		result = string(v.Code)
	}
	c.store.observeAt(c.id, creds, c.source, "consume", c.phase, result, Snapshot{}, err, requestStarted)
	return v, err
}

type healthMonitor struct {
	store     *healthStore
	enumerate func() ([]string, error)
	loader    CredsLoader
	stop      chan struct{}
	done      chan struct{}
	mu        sync.Mutex
	running   bool
	lastCycle time.Time
	lastError string
}

func newHealthMonitor(s *healthStore, enumerate func() ([]string, error), loader CredsLoader) *healthMonitor {
	return &healthMonitor{store: s, enumerate: enumerate, loader: loader, stop: make(chan struct{}), done: make(chan struct{})}
}
func (m *healthMonitor) cycle() {
	ids, err := m.enumerate()
	if err != nil {
		m.mu.Lock()
		m.lastError = "account_enumeration_failed"
		m.lastCycle = time.Now().UTC()
		m.mu.Unlock()
		return
	}
	failed := false
	for _, id := range ids {
		select {
		case <-m.stop:
			return
		default:
		}
		acquired := time.Now()
		creds, client, e := m.loader(id)
		if e != nil {
			failed = true
			m.store.credentialUnavailable(id, acquired)
			continue
		}
		m.store.beginGeneration(id, creds, acquired)
		wrapped := &observedClient{inner: client, store: m.store, id: id, source: "monitor", acquired: acquired}
		_, _ = wrapped.GetUsage(creds)
	}
	m.mu.Lock()
	m.lastCycle = time.Now().UTC()
	m.lastError = ""
	if failed {
		m.lastError = "credential_load_failed"
	}
	m.mu.Unlock()
}
func (m *healthMonitor) run() {
	m.mu.Lock()
	m.running = true
	m.mu.Unlock()
	defer func() { m.mu.Lock(); m.running = false; m.mu.Unlock(); close(m.done) }()
	m.cycle()
	ticker := time.NewTicker(healthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.cycle()
		}
	}
}
func (m *healthMonitor) stopAndWait() { close(m.stop); <-m.done }
func (m *healthMonitor) status() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return map[string]any{"running": m.running, "interval": "15m", "last_cycle": m.lastCycle, "last_error": m.lastError, "accounts": m.store.snapshot()}
}
