package main

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHealthEpisodesAndPersistence(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	s, err := newHealthStore(dir, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	creds := CodexCredentials{AccessToken: "secret-one"}
	id := "account-one"
	s.observe(id, creds, "monitor", "usage", Snapshot{WeeklyPct: 30, WeeklyObservedAt: now, WeeklyResetAt: now.Add(time.Hour)}, nil)
	now = now.Add(time.Minute)
	s.observe(id, creds, "monitor", "usage", Snapshot{}, &HTTPStatusError{Code: 401, Body: []byte(`{"error":{"code":"token_revoked"}}`)})
	first := s.snapshot()[healthHash(id)]
	if first.State != "rejected" || first.FirstRejected.IsZero() || first.LastSuccess.IsZero() || first.WeeklyRemaining != 30 {
		t.Fatalf("first rejection: %+v", first)
	}
	now = now.Add(time.Minute)
	s.observe(id, creds, "monitor", "usage", Snapshot{}, &HTTPStatusError{Code: 401})
	if got := s.snapshot()[healthHash(id)]; !got.FirstRejected.Equal(first.FirstRejected) {
		t.Fatal("first rejection changed")
	}
	now = now.Add(time.Minute)
	s.observe(id, creds, "monitor", "usage", Snapshot{}, errors.New("network"))
	if got := s.snapshot()[healthHash(id)]; got.State != "rejected" || got.Code != "transport_error" {
		t.Fatalf("network erased rejection: %+v", got)
	}
	s2, err := newHealthStore(dir, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.snapshot()[healthHash(id)]; got.State != "rejected" || got.WeeklyRemaining != 30 {
		t.Fatalf("not persisted: %+v", got)
	}
	now = now.Add(time.Minute)
	s2.observe(id, CodexCredentials{AccessToken: "secret-two"}, "monitor", "usage", Snapshot{}, errors.New("network"))
	if got := s2.snapshot()[healthHash(id)]; got.State != "unknown" || !got.FirstRejected.IsZero() {
		t.Fatalf("old rejection attached to new token: %+v", got)
	}
	now = now.Add(time.Minute)
	s2.observe(id, CodexCredentials{AccessToken: "secret-two"}, "monitor", "usage", Snapshot{WeeklyObservedAt: now}, nil)
	if got := s2.snapshot()[healthHash(id)]; got.State != "valid" {
		t.Fatalf("recovery: %+v", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, _ := os.ReadFile(filepath.Join(dir, e.Name()))
		if strings.Contains(string(b), "secret-one") || strings.Contains(string(b), id) {
			t.Fatalf("secret or identity leaked in %s", e.Name())
		}
	}
}

func TestHealthMonitorChecksAllWithUsageOnly(t *testing.T) {
	dir := t.TempDir()
	s, _ := newHealthStore(dir, time.Now)
	calls := []string{}
	mon := newHealthMonitor(s, func() ([]string, error) { return []string{"enabled", "disabled"}, nil }, func(id string) (CodexCredentials, ResetClient, error) {
		return CodexCredentials{AccessToken: id}, &healthFake{id: id, calls: &calls}, nil
	})
	mon.cycle()
	if strings.Join(calls, ",") != "usage:enabled,usage:disabled" {
		t.Fatalf("calls %v", calls)
	}
	if len(s.snapshot()) != 2 {
		t.Fatal("missing account")
	}
}

type healthFake struct {
	id    string
	calls *[]string
}

func (f *healthFake) GetUsage(CodexCredentials) (Snapshot, error) {
	*f.calls = append(*f.calls, "usage:"+f.id)
	return Snapshot{}, nil
}
func (f *healthFake) ListCredits(CodexCredentials) (Snapshot, error) {
	*f.calls = append(*f.calls, "credits:"+f.id)
	return Snapshot{}, nil
}
func (f *healthFake) Consume(CodexCredentials, string, string) (ConsumeResponse, error) {
	*f.calls = append(*f.calls, "consume:"+f.id)
	return ConsumeResponse{}, nil
}

func TestHealthClassifiesResponses(t *testing.T) {
	cases := []struct {
		err    error
		code   string
		status int
	}{
		{nil, "ok", 200},
		{&HTTPStatusError{Code: 401, Body: []byte(`{"error":{"code":"token_revoked","message":"secret"}}`)}, "token_revoked", 401},
		{&HTTPStatusError{Code: 401}, "auth_failure", 401},
		{&HTTPStatusError{Code: 403}, "permission_denied", 403},
		{&HTTPStatusError{Code: 429}, "quota_exceeded", 429},
		{&HTTPStatusError{Code: 503}, "server_error", 503},
		{&InvalidResponseError{Err: errors.New("secret response")}, "invalid_response", 200},
		{errors.New("secret network"), "transport_error", 0},
	}
	for _, tc := range cases {
		code, status := classifyHealth(tc.err)
		if code != tc.code || status != tc.status {
			t.Errorf("got %s/%d want %s/%d", code, status, tc.code, tc.status)
		}
	}
}

func TestHealthJournalRetentionAndFailures(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	s, _ := newHealthStore(dir, func() time.Time { return now })
	creds := CodexCredentials{AccessToken: "test-token"}
	s.observe("acct", creds, "monitor", "usage", Snapshot{}, nil)
	first := filepath.Join(dir, "health-20261108-000000.jsonl")
	if _, err := os.Stat(first); err != nil {
		t.Fatal(err)
	}
	now = now.Add(15 * 24 * time.Hour)
	s.observe("acct", creds, "monitor", "usage", Snapshot{}, nil)
	if _, err := os.Stat(first); !os.IsNotExist(err) {
		t.Fatalf("old history not pruned: %v", err)
	}
	// A failed journal append is reported, and it is not disguised as a check error.
	s.dir = filepath.Join(dir, "missing", "child")
	reported := 0
	s.report = func(error) { reported++ }
	s.observe("acct", creds, "monitor", "usage", Snapshot{}, nil)
	if reported == 0 {
		t.Fatal("failed writes were not reported")
	}
}

func TestHealthOlderRequestCannotOverwriteNewGeneration(t *testing.T) {
	now := time.Now()
	s, _ := newHealthStore(t.TempDir(), func() time.Time { return now })
	a := CodexCredentials{AccessToken: "old"}
	b := CodexCredentials{AccessToken: "new"}
	s.observeStarted("acct", b, "monitor", "usage", "", "", Snapshot{}, nil, now)
	s.observeStarted("acct", a, "autoreset", "usage", "IDLE", "", Snapshot{}, &HTTPStatusError{Code: 401}, now.Add(-time.Second))
	row := s.snapshot()[healthHash("acct")]
	if row.State != "valid" || row.Generation != healthHash("new") {
		t.Fatalf("late old request overwrote new state: %+v", row)
	}
}

func TestHealthMonitorStopsAfterInFlightCall(t *testing.T) {
	s, _ := newHealthStore(t.TempDir(), time.Now)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	monitor := newHealthMonitor(s, func() ([]string, error) { return []string{"first", "second"}, nil }, func(id string) (CodexCredentials, ResetClient, error) {
		return CodexCredentials{AccessToken: id}, &blockingHealthFake{started: started, release: release}, nil
	})
	go monitor.run()
	<-started
	go func() { monitor.stopAndWait(); close(done) }()
	select {
	case <-done:
		t.Fatal("stop returned before in-flight request finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitor failed to stop")
	}
	if len(s.snapshot()) != 1 {
		t.Fatal("monitor started another request after stopping")
	}
}

type blockingHealthFake struct {
	started chan struct{}
	release chan struct{}
}

func (f *blockingHealthFake) GetUsage(CodexCredentials) (Snapshot, error) {
	close(f.started)
	<-f.release
	return Snapshot{}, nil
}
func (f *blockingHealthFake) ListCredits(CodexCredentials) (Snapshot, error) {
	panic("unexpected credits")
}
func (f *blockingHealthFake) Consume(CodexCredentials, string, string) (ConsumeResponse, error) {
	panic("unexpected consume")
}

func TestObservedHTTPInvalidBodyAndConsumeOutcome(t *testing.T) {
	s, _ := newHealthStore(t.TempDir(), time.Now)
	calls := 0
	inner, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if strings.HasSuffix(r.URL.Path, "/consume") {
			w.Write([]byte(`{"code":"reset","windows_reset":1}`))
			return
		}
		w.Write([]byte(`not-json`))
	}))
	c := &observedClient{inner: inner, store: s, id: "account", source: "autoreset", phase: "VERIFYING"}
	creds := CodexCredentials{AccessToken: "test-secret"}
	_, err := c.GetUsage(creds)
	if err == nil {
		t.Fatal("invalid response accepted")
	}
	row := s.snapshot()[healthHash("account")]
	if row.State != "valid" || row.Code != "invalid_response" {
		t.Fatalf("invalid body status: %+v", row)
	}
	if _, err = c.Consume(creds, "rid", "credit"); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls %d", calls)
	}
	files, _ := filepath.Glob(filepath.Join(s.dir, "health-*.jsonl"))
	raw, _ := os.ReadFile(files[0])
	if !strings.Contains(string(raw), `"result":"reset"`) || !strings.Contains(string(raw), `"phase":"VERIFYING"`) {
		t.Fatalf("missing safe consume and phase evidence: %s", raw)
	}
	for _, secret := range []string{"test-secret", "rid", "credit\""} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("leaked %q", secret)
		}
	}
}

func TestHealthJournalSizeBound(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	s, _ := newHealthStore(dir, func() time.Time { return now })
	s.maxBytes = (1 << 20) + 1000
	previous := filepath.Join(dir, "health-20261108-000000.jsonl")
	f, err := os.OpenFile(previous, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}
	f.Close()
	s.observe("a", CodexCredentials{AccessToken: "t"}, "monitor", "usage", Snapshot{}, nil)
	if _, err = os.Stat(filepath.Join(dir, "health-20261108-000001.jsonl")); err != nil {
		t.Fatalf("rotation missing: %v", err)
	}
	s.maxBytes = 500
	s.observe("a", CodexCredentials{AccessToken: "t"}, "monitor", "usage", Snapshot{}, nil)
	if _, err = os.Stat(previous); !os.IsNotExist(err) {
		t.Fatalf("size pruning missed oldest: %v", err)
	}
}
