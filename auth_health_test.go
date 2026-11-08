package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	if got := s2.snapshot()[healthHash(id)]; got.State != "valid" || got.LastRecovered.IsZero() || got.PendingRecovery {
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
		{&HTTPStatusError{Code: 401, Body: []byte(`{"error":{"code":"token_expired"}}`)}, "token_expired", 401},
		{&HTTPStatusError{Code: 401, Body: []byte(`{"error":{"code":"private-do-not-log"}}`)}, "auth_failure", 401},
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
	s.beginGeneration("acct", b, now)
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
	s.observe("account", creds, "monitor", "usage", Snapshot{}, &HTTPStatusError{Code: 401})
	_, err := c.GetUsage(creds)
	if err == nil {
		t.Fatal("invalid response accepted")
	}
	row := s.snapshot()[healthHash("account")]
	if row.State != "rejected" || row.Code != "invalid_response" || !row.LastSuccess.IsZero() {
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

func TestHealthFailureDoesNotDisableResetConfiguration(t *testing.T) {
	for _, kind := range []string{"malformed_summary", "invalid_directory"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			// Explicit state and migration paths keep the user's home untouched.
			healthPath := filepath.Join(root, "auth-health")
			if kind == "malformed_summary" {
				if err := os.Mkdir(healthPath, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(healthPath, "health-summary.json"), []byte("{broken evidence"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(healthPath, []byte("not a directory"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			raw, _ := json.Marshal(lifecycleRequest{ConfigYAML: []byte("state_dir: " + root + "\nenabled_accounts: []\n")})
			if err := configurePluginWithMigration(raw, func(string) {}, filepath.Join(root, "absent-legacy-state.json")); err != nil {
				t.Fatalf("diagnostics stopped reset configuration: %v", err)
			}
			t.Cleanup(cliproxyPluginShutdown)
			if globalWorker == nil {
				t.Fatal("reset worker was not started")
			}
			_, b := globalHandlers.statusJSON()
			var status struct {
				AuthHealth struct {
					Running   bool   `json:"running"`
					LastError string `json:"last_error"`
				} `json:"auth_health"`
			}
			if err := json.Unmarshal(b, &status); err != nil {
				t.Fatal(err)
			}
			if status.AuthHealth.Running || status.AuthHealth.LastError != "evidence_unavailable" {
				t.Fatalf("unavailable health not exposed: %s", b)
			}
			if kind == "malformed_summary" {
				got, err := os.ReadFile(filepath.Join(healthPath, "health-summary.json"))
				if err != nil || string(got) != "{broken evidence" {
					t.Fatalf("corrupted evidence not preserved: %q %v", got, err)
				}
			} else {
				got, err := os.ReadFile(healthPath)
				if err != nil || string(got) != "not a directory" {
					t.Fatalf("invalid path not preserved: %q %v", got, err)
				}
			}
			// Reconfiguration retries only after the operator preserves/repairs the path.
			if kind == "malformed_summary" {
				if err := os.Rename(filepath.Join(healthPath, "health-summary.json"), filepath.Join(healthPath, "health-summary.corrupt")); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Rename(healthPath, healthPath+".preserved"); err != nil {
					t.Fatal(err)
				}
			}
			if err := configurePluginWithMigration(raw, func(string) {}, filepath.Join(root, "absent-legacy-state.json")); err != nil {
				t.Fatalf("repair reconfiguration failed: %v", err)
			}
			if globalHandlers.health == nil || globalHandlers.healthUnavailable {
				t.Fatal("health monitor did not recover after repair")
			}
		})
	}
}

func TestHealthReplacementRecoverySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	clock := func() time.Time { return now }
	s, _ := newHealthStore(dir, clock)
	s.observe("acct", CodexCredentials{AccessToken: "old"}, "monitor", "usage", Snapshot{}, &HTTPStatusError{Code: 401})
	now = now.Add(time.Second)
	s.observe("acct", CodexCredentials{AccessToken: "new"}, "monitor", "usage", Snapshot{}, errors.New("network"))
	s, err := newHealthStore(dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	row := s.snapshot()[healthHash("acct")]
	if row.State != "unknown" || !row.PendingRecovery || !row.FirstRejected.IsZero() {
		t.Fatalf("wrong new-generation state: %+v", row)
	}
	now = now.Add(time.Second)
	s.observe("acct", CodexCredentials{AccessToken: "new"}, "monitor", "usage", Snapshot{}, nil)
	row = s.snapshot()[healthHash("acct")]
	if row.State != "valid" || row.PendingRecovery || row.LastRecovered.IsZero() {
		t.Fatalf("recovery not recorded: %+v", row)
	}
}

func TestHealthRejectsProviderRedirect(t *testing.T) {
	hits := 0
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++; w.Write([]byte(`{}`)) }))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, http.StatusFound) }))
	defer origin.Close()
	client := &OpenAIClient{BaseURL: origin.URL, HTTP: origin.Client()}
	_, err := client.GetUsage(CodexCredentials{AccessToken: "secret"})
	code, status := classifyHealth(err)
	if hits != 0 || code != "http_error" || status != 302 {
		t.Fatalf("redirect followed or misclassified: hits=%d code=%s status=%d", hits, code, status)
	}
}

func TestHealthOlderCredentialAfterNewerAcquisition(t *testing.T) {
	now := time.Now()
	s, _ := newHealthStore(t.TempDir(), func() time.Time { return now })
	old := CodexCredentials{AccessToken: "old"}
	newer := CodexCredentials{AccessToken: "new"}
	oldAcquired := now
	s.beginGeneration("acct", old, oldAcquired)
	now = now.Add(time.Second)
	newAcquired := now
	s.beginGeneration("acct", newer, newAcquired)
	s.observeStarted("acct", newer, "monitor", "usage", "", "", Snapshot{}, nil, newAcquired)
	// A second old-FSM request starts after the new check but retains the old acquisition timestamp.
	now = now.Add(time.Second)
	s.observeStarted("acct", old, "autoreset", "usage", "VERIFYING", "", Snapshot{}, &HTTPStatusError{Code: 401}, oldAcquired)
	row := s.snapshot()[healthHash("acct")]
	if row.State != "valid" || row.Generation != healthHash("new") {
		t.Fatalf("old acquisition replaced new grant: %+v", row)
	}
}

func TestHealthCredentialLoadFailureBecomesUnknown(t *testing.T) {
	for _, prior := range []string{"valid", "rejected"} {
		t.Run(prior, func(t *testing.T) {
			now := time.Now()
			s, _ := newHealthStore(t.TempDir(), func() time.Time { return now })
			creds := CodexCredentials{AccessToken: "token"}
			var err error
			if prior == "rejected" {
				err = &HTTPStatusError{Code: 401}
			}
			s.observe("acct", creds, "monitor", "usage", Snapshot{}, err)
			old := s.snapshot()[healthHash("acct")]
			now = now.Add(time.Minute)
			s.credentialUnavailable("acct", now)
			got := s.snapshot()[healthHash("acct")]
			if got.State != "unknown" || got.Code != "credential_unavailable" || !got.LastChecked.Equal(old.LastChecked) || !got.LastAttempt.Equal(now) {
				t.Fatalf("missing unknown status: %+v", got)
			}
			if prior == "valid" && !got.LastSuccess.Equal(old.LastSuccess) {
				t.Fatal("last success lost")
			}
			if prior == "rejected" && (!got.LastRejected.Equal(old.LastRejected) || !got.PendingRecovery) {
				t.Fatal("rejection history lost")
			}
		})
	}
}

func TestHealthRecordsActualVerificationOutcome(t *testing.T) {
	for _, tc := range []struct {
		name    string
		credits Snapshot
		usage   Snapshot
		err     error
		want    string
	}{
		{name: "verified", credits: Snapshot{AvailableCount: 1}, usage: Snapshot{WeeklyPct: 100}, want: "verified"},
		{name: "partial", credits: Snapshot{AvailableCount: 1}, usage: Snapshot{WeeklyPct: 20}, want: "partial"},
		{name: "mismatch", credits: Snapshot{AvailableCount: 2}, usage: Snapshot{WeeklyPct: 100}, want: "mismatch"},
		{name: "error", err: errors.New("secret error"), want: "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newHealthStore(t.TempDir(), time.Now)
			creds := CodexCredentials{AccessToken: "secret"}
			c := &observedClient{inner: &verificationFake{credits: tc.credits, usage: tc.usage, err: tc.err}, store: s, id: "account", source: "autoreset", phase: "VERIFYING"}
			f := NewAccountFSM("account", creds, DefaultConfig(), c, time.Now, nil)
			f.state = StateVERIFYING
			f.attempt = ResetAttempt{TargetCreditID: "sensitive-id", PreSnapshot: Snapshot{AvailableCount: 2, WeeklyPct: 20}}
			f.Step()
			files, _ := filepath.Glob(filepath.Join(s.dir, "health-*.jsonl"))
			raw, _ := os.ReadFile(files[0])
			if !strings.Contains(string(raw), `"operation":"verification"`) || !strings.Contains(string(raw), `"result":"`+tc.want+`"`) {
				t.Fatalf("missing decision: %s", raw)
			}
			if strings.Contains(string(raw), "sensitive-id") || strings.Contains(string(raw), "secret error") {
				t.Fatal("private details leaked")
			}
		})
	}
}

type verificationFake struct {
	credits, usage Snapshot
	err            error
}

func (f *verificationFake) ListCredits(CodexCredentials) (Snapshot, error) { return f.credits, f.err }
func (f *verificationFake) GetUsage(CodexCredentials) (Snapshot, error)    { return f.usage, f.err }
func (f *verificationFake) Consume(CodexCredentials, string, string) (ConsumeResponse, error) {
	panic("unexpected consume")
}

func TestHealthSameCredentialReacquiredDoesNotHideLaterRejection(t *testing.T) {
	now := time.Now()
	s, _ := newHealthStore(t.TempDir(), func() time.Time { return now })
	creds := CodexCredentials{AccessToken: "same"}
	first := now
	s.beginGeneration("acct", creds, first)
	now = now.Add(time.Second)
	s.beginGeneration("acct", creds, now)
	s.observeAt("acct", creds, "monitor", "usage", "", "", Snapshot{}, nil, now)
	now = now.Add(time.Second)
	s.observeAt("acct", creds, "autoreset", "usage", "VERIFYING", "", Snapshot{}, &HTTPStatusError{Code: 401}, now)
	row := s.snapshot()[healthHash("acct")]
	if row.State != "rejected" {
		t.Fatalf("current token rejection was lost after same-token reload: %+v", row)
	}
	// A response from an earlier-started request must not erase the later rejection.
	now = now.Add(time.Second)
	s.observeAt("acct", creds, "monitor", "usage", "", "", Snapshot{}, nil, first)
	if row = s.snapshot()[healthHash("acct")]; row.State != "rejected" {
		t.Fatalf("stale success erased rejection: %+v", row)
	}
}

func TestHealthLoadFailureFencesOlderInFlightObservations(t *testing.T) {
	now := time.Now()
	s, _ := newHealthStore(t.TempDir(), func() time.Time { return now })
	creds := CodexCredentials{AccessToken: "same"}
	requestStart := now
	s.beginGeneration("acct", creds, now)
	s.observeAt("acct", creds, "monitor", "usage", "", "", Snapshot{}, nil, now)
	now = now.Add(time.Second)
	s.credentialUnavailable("acct", now)
	now = now.Add(time.Second)
	s.beginGeneration("acct", creds, now)
	s.observeAt("acct", creds, "autoreset", "usage", "", "", Snapshot{}, nil, requestStart)
	if got := s.snapshot()[healthHash("acct")]; got.State != "unknown" {
		t.Fatalf("old in-flight success hid credential load failure: %+v", got)
	}
	now = now.Add(time.Second)
	s.observeAt("acct", creds, "monitor", "usage", "", "", Snapshot{}, nil, now)
	if got := s.snapshot()[healthHash("acct")]; got.State != "valid" {
		t.Fatalf("new successful check not accepted: %+v", got)
	}
}

func TestHealthRecordsLateRejectionsWithoutOverridingCurrentState(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement=%t", replacement), func(t *testing.T) {
			dir := t.TempDir()
			now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			s, err := newHealthStore(dir, clock)
			if err != nil {
				t.Fatal(err)
			}
			old := CodexCredentials{AccessToken: "old"}
			current := old
			if replacement {
				current = CodexCredentials{AccessToken: "new"}
			}
			firstStarted := now
			s.beginGeneration("acct", old, firstStarted)
			now = now.Add(time.Second)
			s.beginGeneration("acct", current, now)
			s.observeAt("acct", current, "monitor", "usage", "", "", Snapshot{}, nil, now)
			now = now.Add(time.Second)
			s.observeAt("acct", old, "autoreset", "usage", "VERIFYING", "", Snapshot{}, &HTTPStatusError{Code: 401}, firstStarted)
			row := s.snapshot()[healthHash("acct")]
			if row.State != "valid" || row.Generation != healthHash(current.AccessToken) || !row.FirstRejected.IsZero() {
				t.Fatalf("current health changed: %+v", row)
			}
			if !row.LastRejected.Equal(now) || row.LastRejectedGeneration != healthHash(old.AccessToken) {
				t.Fatalf("late rejection not attributed: %+v", row)
			}
			reloaded, err := newHealthStore(dir, clock)
			if err != nil {
				t.Fatal(err)
			}
			row = reloaded.snapshot()[healthHash("acct")]
			if !row.LastRejected.Equal(now) || row.LastRejectedGeneration != healthHash(old.AccessToken) || row.State != "valid" {
				t.Fatalf("history did not survive restart: %+v", row)
			}
		})
	}
}
