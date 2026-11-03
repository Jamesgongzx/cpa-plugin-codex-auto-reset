package main

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// expiryInventoryClient exercises the real parser with server-side expiry
// and its matching available_count change, without applying the failed POST.
type expiryInventoryClient struct {
	*fakeClient
	now         func() time.Time
	omitExpired bool
}

func (c *expiryInventoryClient) ListCredits(CodexCredentials) (Snapshot, error) {
	c.listCalls++
	credits := make([]Credit, 0, len(c.credits))
	available := 0
	for _, source := range c.credits {
		credit := *source
		if credit.Status == "available" && !credit.ExpiresAt.IsZero() && !credit.ExpiresAt.After(c.now()) {
			credit.Status = "expired"
		}
		if credit.Status == "available" {
			available++
		}
		if !c.omitExpired || credit.Status != "expired" {
			credits = append(credits, credit)
		}
	}
	raw, err := json.Marshal(map[string]any{"credits": credits, "available_count": available})
	if err != nil {
		return Snapshot{}, err
	}
	return parseResetCreditsResponse(raw, c.now())
}

func TestRecovery_ExpiryCannotResolveAmbiguousFullQuota(t *testing.T) {
	for _, phase := range []string{"in_process", "restart_pending_retry", "restart_before_verification"} {
		for _, omitExpired := range []bool{false, true} {
			name := phase
			if omitExpired {
				name += "/target_omitted"
			} else {
				name += "/target_reported_expired"
			}
			t.Run(name, func(t *testing.T) {
				fsm, client, clock, trigger := armedFSMFixture(t)
				fsm.Cfg.RefreshInterval = time.Minute
				clock.t = trigger
				client.weeklyPct = 100
				client.credits["b"].ExpiresAt = client.credits["a"].ExpiresAt.Add(time.Hour)
				wire := &expiryInventoryClient{fakeClient: client, now: clock.Now, omitExpired: omitExpired}
				fsm.Client = wire
				fsm.StepAndSettle()
				fsm.StepAndSettle()
				path := filepath.Join(t.TempDir(), "state.json")
				fsm.Checkpoint = fileCheckpoint(t, path, fsm.Cfg)
				client.failFirstN, client.failErr = 1, errors.New("ambiguous response; reset was not applied")
				fsm.StepAndSettle()
				original := fsm.Runtime().Attempt
				if err := fsm.Checkpoint(fsm.Runtime()); err != nil {
					t.Fatal(err)
				}
				clock.t = client.credits["a"].ExpiresAt.Add(time.Second)
				if phase == "restart_before_verification" {
					fsm.StepAndSettle()
					if fsm.State() != StateVERIFYING || fsm.Runtime().Attempt.Accepted {
						t.Fatal("fixture did not reach verification without a known consume result")
					}
					if err := fsm.Checkpoint(fsm.Runtime()); err != nil {
						t.Fatal(err)
					}
				}
				var worker *Worker
				if phase != "in_process" {
					worker = restoredRecoveryWorker(t, path, fsm, wire, clock)
					fsm = worker.fsms[fsm.AuthID]
				}
				for i := 0; i < 8; i++ {
					if worker != nil {
						worker.stepAccountSafe(fsm.AuthID)
					} else {
						fsm.StepAndSettle()
					}
					if record := fsm.TakePendingHistory(); record != nil && record.Success {
						t.Fatal("natural expiry with unchanged 100% quota was recorded as a successful reset")
					}
					if err := fsm.Checkpoint(fsm.Runtime()); err != nil {
						t.Fatal(err)
					}
					clock.t = fsm.NextWake()
				}
				fsm.ForceConfirm()
				pending := fsm.Runtime().Attempt
				if fsm.State() != StateRECONCILING || pending.RedeemRequestID != original.RedeemRequestID || pending.TargetCreditID != original.TargetCreditID || client.consumeCalls != 1 || client.credits["b"].Status != "available" {
					t.Fatalf("expiry released the original attempt: state=%s request=%s credit=%s calls=%d", fsm.State(), pending.RedeemRequestID, pending.TargetCreditID, client.consumeCalls)
				}
			})
		}
	}
}

func TestRecovery_ConsumptionAndQuotaEvidenceRemainSeparate(t *testing.T) {
	for _, tc := range []struct {
		name         string
		accepted     bool
		targetStatus string
		pre, post    int
		wantSuccess  bool
	}{
		{"known_acceptance_full_quota_without_status", true, "missing", 100, 100, true},
		{"target_redeemed_changed_quota", false, "redeemed", 25, 100, true},
		{"target_redeemed_full_quota", false, "redeemed", 100, 100, true},
		{"missing_target_changed_quota", false, "missing", 25, 100, false},
		{"unknown_target_status_changed_quota", false, "processing", 25, 100, false},
		{"accepted_but_unknown_quota", true, "redeemed", 25, -1, false},
		{"redeemed_but_unchanged_partial_quota", false, "redeemed", 25, 25, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fsm, client, clock, trigger := armedFSMFixture(t)
			clock.t = trigger
			client.weeklyPct = tc.pre
			fsm.Client = &expiryInventoryClient{fakeClient: client, now: clock.Now}
			fsm.StepAndSettle()
			fsm.StepAndSettle()
			if !tc.accepted {
				client.failFirstN, client.failErr = 1, errors.New("consume result unknown")
			}
			fsm.StepAndSettle()
			if fsm.Runtime().Attempt.Accepted != tc.accepted {
				t.Fatal("fixture acceptance does not match the actual consume result")
			}
			if tc.targetStatus == "missing" {
				delete(client.credits, "a")
			} else {
				client.credits["a"].Status = tc.targetStatus
			}
			client.weeklyPct = tc.post
			fsm.Restore(fsm.Runtime())
			fsm.StepAndSettle()
			record := fsm.TakePendingHistory()
			success := record != nil && record.Success
			if success != tc.wantSuccess || client.consumeCalls != 1 {
				t.Fatalf("evidence result: success=%v want=%v state=%s calls=%d", success, tc.wantSuccess, fsm.State(), client.consumeCalls)
			}
			if !tc.wantSuccess && fsm.State() != StateRECONCILING {
				t.Fatal("unresolved evidence released the attempt")
			}
		})
	}
}

func TestRecovery_HeldAttemptLeavesOtherAccountsManualResetUsable(t *testing.T) {
	held, heldClient, clock := confirmedRecoveryFSM(t)
	heldClient.consumeResp.Code = "unrecognized_result"
	held.StepAndSettle()
	original := held.Runtime().Attempt
	otherClient := &fakeClient{
		credits:   map[string]*Credit{"other-credit": {ID: "other-credit", Status: "available"}},
		weeklyPct: 10, consumeResp: ConsumeResponse{Code: ConsumeCodeReset},
	}
	w := newTestWorker(held.Cfg, []string{held.AuthID, "other"}, func(string) (CodexCredentials, ResetClient, error) {
		return CodexCredentials{SeatFingerprint: "other-seat"}, otherClient, nil
	}, clock.Now)
	w.fsms[held.AuthID] = held
	w.ensureFSM("other")
	if err := w.ForceConfirm("other"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		w.stepAccountSafe("other")
	}
	if record := w.fsms["other"].TakePendingHistory(); record == nil || !record.Success || otherClient.consumeCalls != 1 {
		t.Fatal("held account prevented another account's manual reset")
	}
	if held.State() != StateRECONCILING || held.Runtime().Attempt.RedeemRequestID != original.RedeemRequestID || heldClient.consumeCalls != 1 {
		t.Fatal("another account's manual reset replaced the held attempt")
	}
}

type lostResponseClient struct {
	*fakeClient
	lost bool
}

func (c *lostResponseClient) Consume(creds CodexCredentials, requestID, creditID string) (ConsumeResponse, error) {
	response, err := c.fakeClient.Consume(creds, requestID, creditID)
	if !c.lost && err == nil {
		c.lost = true
		return ConsumeResponse{}, errors.New("response lost after upstream applied the reset")
	}
	return response, err
}

func fileCheckpoint(t *testing.T, path string, cfg Config) func(AccountRuntime) error {
	t.Helper()
	return func(runtime AccountRuntime) error {
		return saveState(path, &PluginState{Config: cfg, Accounts: map[string]AccountRuntime{runtime.AuthID: runtime}})
	}
}

func confirmedRecoveryFSM(t *testing.T) (*AccountFSM, *fakeClient, *fakeClock) {
	t.Helper()
	fsm, client, clock, trigger := armedFSMFixture(t)
	clock.t = trigger
	client.credits["b"].ExpiresAt = trigger.Add(2 * time.Hour)
	fsm.Step()
	fsm.Step()
	if fsm.State() != StateRESETTING {
		t.Fatalf("state = %s, want confirmed attempt", fsm.State())
	}
	return fsm, client, clock
}

func restoredRecoveryWorker(t *testing.T, path string, fsm *AccountFSM, client ResetClient, clock *fakeClock) *Worker {
	t.Helper()
	loaded, _, err := loadState(path)
	if err != nil {
		t.Fatal(err)
	}
	w := newTestWorker(fsm.Cfg, []string{fsm.AuthID}, func(string) (CodexCredentials, ResetClient, error) {
		return fsm.Creds, client, nil
	}, clock.Now)
	w.InitialAccounts = loaded.Accounts
	w.Checkpoint = fileCheckpoint(t, path, fsm.Cfg)
	w.ensureFSM(fsm.AuthID)
	return w
}

func TestRecovery_LostAppliedResponseReconcilesBeforeAnotherCredit(t *testing.T) {
	for _, syncAfterResponse := range []bool{false, true} {
		name := "dispatch_checkpoint_only"
		if syncAfterResponse {
			name = "post_response_state_saved"
		}
		t.Run(name, func(t *testing.T) {
			fsm, client, clock := confirmedRecoveryFSM(t)
			path := filepath.Join(t.TempDir(), "state.json")
			fsm.Checkpoint = fileCheckpoint(t, path, fsm.Cfg)
			lost := &lostResponseClient{fakeClient: client}
			fsm.Client = lost
			fsm.StepAndSettle()
			original := fsm.Runtime().Attempt
			if syncAfterResponse {
				if err := fsm.Checkpoint(fsm.Runtime()); err != nil {
					t.Fatal(err)
				}
			}
			w := restoredRecoveryWorker(t, path, fsm, lost, clock)
			resumed := w.fsms[fsm.AuthID]
			if resumed.State() != StateRECONCILING || resumed.Runtime().Attempt.RedeemRequestID != original.RedeemRequestID {
				t.Fatal("restart discarded the original unresolved request")
			}
			w.stepAccountSafe(fsm.AuthID)
			if client.consumeCalls != 1 || client.credits["b"].Status != "available" {
				t.Fatalf("reconciliation sent another consume: calls=%d targets=%v", client.consumeCalls, client.consumeIDs)
			}
			if record := resumed.TakePendingHistory(); resumed.State() != StateIDLE || record == nil || !record.Success || record.CreditID != original.TargetCreditID {
				t.Fatalf("applied request did not reconcile: state=%s record=%+v", resumed.State(), record)
			}
		})
	}
}

func TestRecovery_RestoredRetryKeepsRequestBackoffAndBudget(t *testing.T) {
	fsm, client, clock := confirmedRecoveryFSM(t)
	path := filepath.Join(t.TempDir(), "state.json")
	fsm.Checkpoint = fileCheckpoint(t, path, fsm.Cfg)
	client.failFirstN, client.failErr = 1, errors.New("request failed before application")
	fsm.StepAndSettle()
	original := fsm.Runtime()
	if err := fsm.Checkpoint(original); err != nil {
		t.Fatal(err)
	}
	w := restoredRecoveryWorker(t, path, fsm, client, clock)
	resumed := w.fsms[fsm.AuthID]
	w.stepAccountSafe(fsm.AuthID) // read-only reconciliation
	w.stepAccountSafe(fsm.AuthID) // an early tick must retain the backoff
	if client.consumeCalls != 1 || !resumed.NextWake().Equal(original.Attempt.NextRetryAt) {
		t.Fatal("restart bypassed the original retry backoff")
	}
	clock.t = resumed.NextWake()
	w.stepAccountSafe(fsm.AuthID)
	if client.consumeCalls != 2 || client.redeemIDs[0] != client.redeemIDs[1] || client.consumeIDs[0] != client.consumeIDs[1] {
		t.Fatalf("retry changed request or credit: requests=%v credits=%v", client.redeemIDs, client.consumeIDs)
	}
	if resumed.Runtime().Attempt.Dispatches != original.Attempt.Dispatches+1 || resumed.Runtime().Attempt.Attempt != original.Attempt.Attempt {
		t.Fatal("restart replenished or replaced the original attempt budget")
	}
}

func TestRecovery_DispatchCheckpointRetainsRetryBackoff(t *testing.T) {
	fsm, client, clock := confirmedRecoveryFSM(t)
	path := filepath.Join(t.TempDir(), "state.json")
	fsm.Checkpoint = fileCheckpoint(t, path, fsm.Cfg)
	client.failFirstN, client.failErr = 1, errors.New("response lost without a response-state save")
	fsm.StepAndSettle()
	retryAt := fsm.Runtime().Attempt.NextRetryAt
	w := restoredRecoveryWorker(t, path, fsm, client, clock)
	w.stepAccountSafe(fsm.AuthID)
	if !w.fsms[fsm.AuthID].NextWake().Equal(retryAt) {
		t.Fatalf("dispatch checkpoint lost the retry delay: wake=%v want=%v", w.fsms[fsm.AuthID].NextWake(), retryAt)
	}
}

func TestRecovery_DispatchCheckpointFailureDoesNotConsumeOrReplaceAttempt(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(map[bool]string{false: "write_failure", true: "missing_checkpoint"}[unavailable], func(t *testing.T) {
			fsm, client, clock := confirmedRecoveryFSM(t)
			original := fsm.Runtime().Attempt
			fsm.Checkpoint = func(AccountRuntime) error { return errors.New("checkpoint write failed") }
			if unavailable {
				fsm.Checkpoint = nil
			}
			fsm.StepAndSettle()
			if client.consumeCalls != 0 || fsm.Runtime().Attempt.Dispatches != 0 || fsm.Runtime().Attempt.RedeemRequestID != original.RedeemRequestID {
				t.Fatal("failed checkpoint dispatched or replaced the original attempt")
			}
			path := filepath.Join(t.TempDir(), "state.json")
			fsm.Checkpoint = fileCheckpoint(t, path, fsm.Cfg)
			clock.t = fsm.NextWake()
			fsm.StepAndSettle()
			loaded, _, err := loadState(path)
			if err != nil {
				t.Fatal(err)
			}
			persisted := loaded.Accounts[fsm.AuthID].Attempt
			if client.consumeCalls != 1 || persisted.RedeemRequestID != original.RedeemRequestID || persisted.TargetCreditID != original.TargetCreditID || persisted.Dispatches != 1 {
				t.Fatalf("checkpoint recovery did not send the original pinned request: %+v", persisted)
			}
		})
	}
}

func TestRecovery_ExhaustedBudgetCannotRestartIntoAnotherCredit(t *testing.T) {
	fsm, client, clock := confirmedRecoveryFSM(t)
	path := filepath.Join(t.TempDir(), "state.json")
	fsm.Checkpoint = fileCheckpoint(t, path, fsm.Cfg)
	client.failFirstN, client.failErr = 100, errors.New("unresolved transport failure")
	for client.consumeCalls < 1+len(resetRetryDelays) {
		clock.t = fsm.StepAndSettle()
	}
	// Keep only the checkpoint written before the last POST. Even without
	// its response sync, the final send has spent the original retry budget.
	w := restoredRecoveryWorker(t, path, fsm, client, clock)
	for i := 0; i < 3; i++ {
		w.stepAccountSafe(fsm.AuthID)
	}
	resumed := w.fsms[fsm.AuthID]
	resumed.ForceConfirm()
	if resumed.State() != StateRECONCILING || client.consumeCalls != 1+len(resetRetryDelays) || resumed.Runtime().Attempt.RedeemRequestID != client.redeemIDs[0] {
		t.Fatalf("exhausted restart replaced the attempt: state=%s targets=%v", resumed.State(), client.consumeIDs)
	}
	if client.credits["b"].Status != "available" {
		t.Fatal("second credit was consumed while the first outcome remained unknown")
	}
}

func TestRecovery_LegacyActiveStateWithoutAttemptRemainsHeld(t *testing.T) {
	for _, state := range []State{StateRESETTING, StateVERIFYING} {
		t.Run(string(state), func(t *testing.T) {
			fsm, client, clock := confirmedRecoveryFSM(t)
			path := filepath.Join(t.TempDir(), "state.json")
			if err := fileCheckpoint(t, path, fsm.Cfg)(AccountRuntime{AuthID: fsm.AuthID, State: state}); err != nil {
				t.Fatal(err)
			}
			w := restoredRecoveryWorker(t, path, fsm, client, clock)
			w.stepAccountSafe(fsm.AuthID)
			w.ForceConfirm(fsm.AuthID)
			w.stepAccountSafe(fsm.AuthID)
			if client.consumeCalls != 0 || w.fsms[fsm.AuthID].State() != StateRECONCILING {
				t.Fatal("legacy active state started a new attempt without knowing the previous outcome")
			}
		})
	}
}

func TestRecovery_UnknownResponseAndWeeklyEvidenceStayUnresolved(t *testing.T) {
	for _, kind := range []string{"unknown_response", "unknown_post_weekly"} {
		t.Run(kind, func(t *testing.T) {
			fsm, client, _ := confirmedRecoveryFSM(t)
			if kind == "unknown_response" {
				client.consumeResp.Code = "unrecognized_success"
			}
			fsm.StepAndSettle()
			original := fsm.Runtime().Attempt
			if kind == "unknown_post_weekly" {
				client.weeklyPct = -1
			}
			fsm.StepAndSettle()
			fsm.ForceConfirm()
			fsm.StepAndSettle()
			if record := fsm.TakePendingHistory(); fsm.State() != StateRECONCILING || record != nil && record.Success || client.consumeCalls != 1 || fsm.Runtime().Attempt.RedeemRequestID != original.RedeemRequestID {
				t.Fatalf("unverified result released the attempt: state=%s record=%+v", fsm.State(), record)
			}
			if kind == "unknown_post_weekly" {
				client.weeklyPct = 100
				fsm.StepAndSettle()
				if record := fsm.TakePendingHistory(); fsm.State() != StateIDLE || record == nil || !record.Success || client.consumeCalls != 1 {
					t.Fatal("measured delayed recovery could not finish the existing attempt")
				}
			}
		})
	}
}

func TestRecovery_ManualResetPreservesUnknownPreWeeklyBehavior(t *testing.T) {
	fsm, client, _ := confirmedRecoveryFSM(t)
	fsm.state, fsm.attempt = StateIDLE, ResetAttempt{}
	client.weeklyPct = -1
	fsm.ForceConfirm()
	fsm.StepAndSettle()
	fsm.StepAndSettle()
	fsm.StepAndSettle()
	if record := fsm.TakePendingHistory(); client.consumeCalls != 1 || record == nil || !record.Success {
		t.Fatal("manual reset with a known seat and measured recovery did not retain its prior behavior")
	}
}

func TestRecovery_UnknownSeatCannotAuthorizeAutomaticOrManualConsume(t *testing.T) {
	for _, manual := range []bool{false, true} {
		fsm, client, clock, trigger := armedFSMFixture(t)
		clock.t = trigger
		fsm.Creds.SeatFingerprint = ""
		if manual {
			fsm.ForceConfirm()
		} else {
			fsm.StepAndSettle()
		}
		fsm.StepAndSettle()
		fsm.StepAndSettle()
		if client.consumeCalls != 0 {
			t.Fatalf("unknown credential seat consumed a credit (manual=%v)", manual)
		}
	}
}

func TestRecovery_CredentialReloadRequiresSameSeat(t *testing.T) {
	for _, kind := range []string{"same_seat_rotation", "changed_subject", "unknown_subject"} {
		t.Run(kind, func(t *testing.T) {
			fsm, client, clock := confirmedRecoveryFSM(t)
			originalCreds := testSeatCredentials(t, "user-one", "workspace", "before")
			fsm.Creds = originalCreds
			fsm.attempt.SeatFingerprint = originalCreds.SeatFingerprint
			original := fsm.Runtime().Attempt
			current := testSeatCredentials(t, "user-one", "workspace", "after")
			if kind == "changed_subject" {
				current = testSeatCredentials(t, "user-two", "workspace", "other")
			} else if kind == "unknown_subject" {
				current.SeatFingerprint = ""
			}
			w := newTestWorker(fsm.Cfg, []string{fsm.AuthID}, func(string) (CodexCredentials, ResetClient, error) { return current, client, nil }, clock.Now)
			w.fsms[fsm.AuthID] = fsm
			beforeLists := client.listCalls
			w.stepAccountSafe(fsm.AuthID)
			if kind == "same_seat_rotation" {
				if client.consumeCalls != 1 || fsm.Creds.AccessToken != current.AccessToken {
					t.Fatal("same-seat host token rotation was refused")
				}
			} else {
				if client.consumeCalls != 0 || client.listCalls != beforeLists || fsm.State() != StateRECONCILING || fsm.Runtime().Attempt.RedeemRequestID != original.RedeemRequestID {
					t.Fatal("changed or unknown seat continued the original attempt")
				}
				current = originalCreds
				w.stepAccountSafe(fsm.AuthID)
				w.stepAccountSafe(fsm.AuthID)
				if client.consumeCalls != 1 || client.redeemIDs[0] != original.RedeemRequestID {
					t.Fatal("restored original seat could not finish its existing request")
				}
			}
		})
	}
}
