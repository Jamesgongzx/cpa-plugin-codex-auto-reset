package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ResetClient is the subset of OpenAIClient that the FSM needs. Defined here
// (next to the consumer) rather than in openai_client.go so the FSM is the
// dependency authority. Tests inject a fake; production injects *OpenAIClient.
type ResetClient interface {
	ListCredits(creds CodexCredentials) (Snapshot, error)
	GetUsage(creds CodexCredentials) (Snapshot, error)
	Consume(creds CodexCredentials, redeemRequestID, creditID string) (ConsumeResponse, error)
}

// LogFn is the FSM's logging callback. The worker wires it to the log ring.
type LogFn func(level, scope string, state State, msg, nextAction string, nextAt *time.Time, details map[string]any)

// AccountFSM is one account's state machine (spec §4). Deterministic given
// inputs; the simulation in sim/engine.go already validated the transitions.
//
// Concurrency: the worker goroutine drives Step(). External readers (worker's
// EachFSM, management handlers) may call State()/NextWake()/SetNextWake()/
// ForceConfirm() from other goroutines; all field access is guarded by mu.
type AccountFSM struct {
	AuthID string
	Creds  CodexCredentials
	Cfg    Config
	Client ResetClient
	Now    func() time.Time
	Logf   LogFn
	// Checkpoint writes this account into the existing plugin state file.
	// A missing or failed checkpoint must prevent a mutating request.
	Checkpoint func(AccountRuntime) error

	mu                sync.Mutex
	state             State
	attempt           ResetAttempt
	nextWake          time.Time
	verifyDue         time.Time
	scheduledCreditID string
	manualReset       bool
	// testPostStep, when non-nil, runs inside StepAndSettle after the tick
	// but BEFORE the settle decision, with mu still held. Tests use it to
	// prove that a concurrent ForceConfirm/TriggerCheck cannot interleave
	// into the (old, racy) gap between step and schedule write. Production
	// leaves it nil.
	testPostStep func()
	// pendingHistory is set in stepVERIFYING with the reset result. NOT cleared
	// by Reset() so the worker can collect it after the cycle ends. The worker
	// sets it back to nil after collecting.
	pendingHistory *ResetRecord
	// lastSnapshot is the most recent Snapshot observed by any state step
	// (IDLE patrol, CONFIRMING pre-reset, VERIFYING post-reset). The worker
	// mirrors it into AccountRuntime so the management UI can show live
	// credit counts, weekly %, and next expiry without an extra API call.
	lastSnapshot Snapshot
}

// NewAccountFSM builds an FSM in StateIDLE, ready for its first Step().
func NewAccountFSM(authID string, creds CodexCredentials, cfg Config, client ResetClient, now func() time.Time, logf LogFn) *AccountFSM {
	return &AccountFSM{
		AuthID: authID, Creds: creds, Cfg: cfg, Client: client, Now: now, Logf: logf,
		state: StateIDLE,
	}
}

// State returns the current FSM state (thread-safe).
func (f *AccountFSM) State() State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}

// NextWake returns the most recently scheduled wake time (thread-safe).
func (f *AccountFSM) NextWake() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nextWake
}

// LastSnapshot returns the most recent Snapshot observed by any state step.
// Thread-safe.
func (f *AccountFSM) LastSnapshot() Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSnapshot
}

// Runtime captures the attempt and its scheduling state under one lock.
func (f *AccountFSM) Runtime() AccountRuntime {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runtimeLocked()
}

func (f *AccountFSM) runtimeLocked() AccountRuntime {
	attempt := f.attempt
	attempt.PreSnapshot = cloneSnapshot(attempt.PreSnapshot)
	snapshot := cloneSnapshot(f.lastSnapshot)
	return AccountRuntime{
		AuthID: f.AuthID, State: f.state, NextWake: f.nextWake,
		Attempt: attempt, LastSnapshot: snapshot,
		ScheduledCreditID: f.scheduledCreditID, ManualReset: f.manualReset,
	}
}

func (f *AccountFSM) checkpointLocked() error {
	if f.Checkpoint == nil {
		return fmt.Errorf("reset checkpoint unavailable")
	}
	return f.Checkpoint(f.runtimeLocked())
}

// Restore keeps a dispatched request's original target, identity and budget.
// It reconciles before any same-ID replay, including a crash during dispatch.
func (f *AccountFSM) Restore(runtime AccountRuntime) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state, f.nextWake = runtime.State, runtime.NextWake
	f.attempt, f.lastSnapshot = runtime.Attempt, runtime.LastSnapshot
	f.scheduledCreditID, f.manualReset = runtime.ScheduledCreditID, runtime.ManualReset
	if runtime.AuthID != "" && runtime.AuthID != f.AuthID {
		f.state = StateRECONCILING
		f.attempt.RetryStopped = true
	}
	switch f.state {
	case StateRESETTING:
		if f.attempt.Dispatches > 0 || !f.validAttemptLocked() {
			f.state = StateRECONCILING
		}
	case StateVERIFYING:
		// VERIFYING can also follow a missing target after an ambiguous send.
		// Only the persisted recognized Consume result establishes acceptance.
		f.state = StateRECONCILING
	case StateIDLE, StateARMED, StateCONFIRMING, StateDONE, StateRECONCILING:
	default:
		f.state = StateRECONCILING
		f.attempt.RetryStopped = true
	}
	if f.attempt.RedeemRequestID != "" && (f.state == StateIDLE || f.state == StateARMED || f.state == StateCONFIRMING) {
		f.state = StateRECONCILING
	}
	if f.state == StateRECONCILING || f.nextWake.IsZero() {
		f.nextWake = f.now()
	}
}

func (f *AccountFSM) validAttemptLocked() bool {
	return f.attempt.RedeemRequestID != "" && f.attempt.TargetCreditID != "" && f.attempt.SeatFingerprint != ""
}

// UpdateCredentials accepts host token renewal for the same pinned seat.
// A changed or unknown subject leaves the original attempt intact.
func (f *AccountFSM) UpdateCredentials(creds CodexCredentials, client ResetClient) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.attempt.RedeemRequestID != "" && (f.attempt.SeatFingerprint == "" || creds.SeatFingerprint == "" || creds.SeatFingerprint != f.attempt.SeatFingerprint) {
		f.state = StateRECONCILING
		f.nextWake = f.now().Add(credentialRetryInterval)
		return fmt.Errorf("credential seat changed or is unknown; retaining the original reset attempt")
	}
	f.Creds, f.Client = creds, client
	return nil
}

// SetNextWake lets external callers (worker's TriggerCheck) override the
// scheduled wake. Thread-safe.
func (f *AccountFSM) SetNextWake(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextWake = t
}

// ForceConfirm requests a manual reset without interrupting consume or verify.
// Thread-safe; manual intent and its immediate wake are set together.
func (f *AccountFSM) ForceConfirm() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == StateRESETTING || f.state == StateVERIFYING || f.state == StateRECONCILING || f.attempt.RedeemRequestID != "" {
		return
	}
	f.scheduledCreditID = ""
	f.manualReset = true
	f.state = StateCONFIRMING
	f.nextWake = f.now()
}

// Step executes one FSM tick at f.Now() and returns the next scheduled wake.
// Returns zero time when the cycle terminated (DONE). The automatic flow
// mirrors sim/engine.go's step(), covering (R, L) relations and edge cases.
//
// Step takes the FSM mutex for the whole tick; HTTP calls happen under lock,
// which is fine because the FSM is driven by a single worker goroutine (no
// other caller Step()s the same FSM).
//
// Step is the raw tick surface used by tests and simulations. The worker
// drives FSMs via StepAndSettle, which additionally completes the DONE→IDLE
// transition atomically (see its comment for why that must not be split).
func (f *AccountFSM) Step() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stepLocked()
}

func (f *AccountFSM) stepLocked() time.Time {
	switch f.state {
	case StateIDLE:
		return f.stepIDLE()
	case StateARMED:
		return f.stepARMED()
	case StateCONFIRMING:
		return f.stepCONFIRMING()
	case StateRESETTING:
		return f.stepRESETTING()
	case StateVERIFYING:
		return f.stepVERIFYING()
	case StateRECONCILING:
		return f.stepRECONCILING()
	case StateDONE:
		return time.Time{}
	}
	return time.Time{}
}

// StepAndSettle executes one tick and, under the same mutex hold, settles the
// post-tick schedule: when the cycle terminated (DONE) it performs the
// DONE→IDLE reset (clearing the pinned target and manual intent) and
// schedules the next patrol at now+refresh_interval; otherwise it records the
// wake the tick returned as nextWake. Returns the settled wake.
//
// Step + settle in one critical section closes the strand race where a
// ForceConfirm or TriggerCheck landing between the worker's Step() return and
// its follow-up Reset()/SetNextWake() could be overwritten with a zero wake —
// leaving a CONFIRMING FSM that soonest() skips forever. Manual triggers take
// the same mutex, so they are now ordered entirely before or after the settle.
func (f *AccountFSM) StepAndSettle() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	wake := f.stepLocked()
	if f.testPostStep != nil {
		f.testPostStep() // test-only interleaving hook; mu is still held
	}
	if wake.IsZero() && f.state == StateDONE {
		if f.attempt.RedeemRequestID != "" {
			if err := f.checkpointLocked(); err != nil {
				wake = f.now().Add(credentialRetryInterval)
				f.log("error", "completed attempt checkpoint failed: "+err.Error(), "retry checkpoint before another cycle", &wake, nil)
				f.nextWake = wake
				return wake
			}
		}
		f.resetLocked()
		wake = f.now().Add(f.Cfg.RefreshInterval)
	}
	f.nextWake = wake
	return wake
}

func (f *AccountFSM) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

// triggerTime returns T = E - L (the moment the trigger window opens).
func (f *AccountFSM) triggerTime(c Credit) time.Time { return c.ExpiresAt.Add(-f.Cfg.TriggerLeadTime) }

// armThreshold returns M = T - R (the moment we enter ARMED).
func (f *AccountFSM) armThreshold(c Credit) time.Time {
	return f.triggerTime(c).Add(-f.Cfg.RefreshInterval)
}

func (f *AccountFSM) log(level, msg, nextAction string, nextAt *time.Time, details map[string]any) {
	if f.Logf != nil {
		f.Logf(level, f.AuthID, f.state, msg, nextAction, nextAt, details)
	}
}

func (f *AccountFSM) stepIDLE() time.Time {
	now := f.now()
	snap, err := f.Client.ListCredits(f.Creds)
	if err != nil {
		next := now.Add(f.Cfg.RefreshInterval)
		f.nextWake = next
		f.log("warn", fmt.Sprintf("list failed: %v", err), "retry patrol", &next, nil)
		return next
	}
	f.lastSnapshot = snap // mirror into the UI-facing snapshot
	// Also fetch weekly % so the UI card can display it. The /rate-limit-
	// reset-credits endpoint doesn't return weekly %, so we need /usage.
	if usage, err := f.Client.GetUsage(f.Creds); err == nil {
		snap.WeeklyPct = usage.WeeklyPct
		f.lastSnapshot = snap
	}
	avail := availableCreditsSorted(snap.Credits, now)
	if len(avail) == 0 {
		next := now.Add(f.Cfg.RefreshInterval)
		f.nextWake = next
		f.log("info", "no available credits", "patrol", &next, nil)
		return next
	}
	target := avail[0]
	if target.ExpiresAt.IsZero() {
		next := now.Add(f.Cfg.RefreshInterval)
		f.nextWake = next
		f.log("info", "no expiring credits to schedule", "patrol", &next, nil)
		return next
	}
	M := f.armThreshold(target)
	if now.Before(M) {
		next := now.Add(f.Cfg.RefreshInterval)
		f.nextWake = next
		f.log("info",
			fmt.Sprintf("credit %s expires in %s; not near trigger window (will arm %s before expiry point)", shortID(target.ID), target.ExpiresAt.Sub(now).Round(time.Hour), f.Cfg.RefreshInterval),
			"patrol", &next,
			map[string]any{"credit_id": target.ID, "expires_at": target.ExpiresAt})
		return next
	}
	// now >= M -> ARMED. Sleep to T (or now if T already passed: late-start,
	// spec §4.2.1). ARMED does NO extra GETs; it just transitions on wake.
	T := f.triggerTime(target)
	f.scheduledCreditID = target.ID
	f.state = StateARMED
	wake := T
	if !now.Before(T) {
		wake = now
	}
	f.nextWake = wake
	f.log("warn",
		fmt.Sprintf("credit %s armed; will reset at %s (expiry in %s)", shortID(target.ID), T.Format("15:04:05"), target.ExpiresAt.Sub(now).Round(time.Minute)),
		"sleep to trigger time", &wake,
		map[string]any{"credit_id": target.ID, "T": T})
	return wake
}

func (f *AccountFSM) stepARMED() time.Time {
	// A Check may wake ARMED early; CONFIRMING revalidates the trigger time.
	// Zero extra GETs in this state (revised design).
	f.state = StateCONFIRMING
	f.log("info", "armed wake, confirming target and trigger time", "confirm before reset", nil, nil)
	return f.now() // re-enter immediately
}

func (f *AccountFSM) stepCONFIRMING() time.Time {
	now := f.now()
	snap, err := f.Client.ListCredits(f.Creds)
	if err != nil {
		f.state = StateDONE
		f.log("error", fmt.Sprintf("confirm list failed: %v", err), "abandon", nil, nil)
		return time.Time{}
	}
	avail := availableCreditsSorted(snap.Credits, now)
	var target Credit
	if f.manualReset {
		if len(avail) > 0 {
			target = avail[0]
		}
	} else if f.scheduledCreditID != "" {
		for _, credit := range avail {
			if credit.ID == f.scheduledCreditID {
				target = credit
				break
			}
		}
	}
	if target.ID == "" {
		f.state = StateDONE
		f.log("warn", "target credit missing or unavailable before reset", "abandon", nil, nil)
		return time.Time{}
	}
	if !f.manualReset {
		if target.ExpiresAt.IsZero() {
			f.state = StateDONE
			f.log("info", "target credit no longer expires", "abandon", nil, nil)
			return time.Time{}
		}
		T := f.triggerTime(target)
		if now.Before(T) {
			f.state = StateARMED
			f.nextWake = T
			f.log("info", "target credit not yet due; re-armed", "sleep to trigger time", &T,
				map[string]any{"credit_id": target.ID, "T": T})
			return T
		}
	}
	// Fetch usage separately so PreSnapshot.WeeklyPct carries a real value.
	// The reset-credits endpoint does NOT return weekly % (parseResetCreditsResponse
	// sets WeeklyPct=-1); without this call, the triple-check's quota leg
	// (quotaUp) would be trivially true (any value > -1) and the safety
	// invariant would degrade to a two-leg check. Spec §4.2 mandates both calls.
	usage, err := f.Client.GetUsage(f.Creds)
	if err != nil {
		f.state = StateDONE
		f.log("error", fmt.Sprintf("confirm usage failed: %v", err), "abandon", nil, nil)
		return time.Time{}
	}
	if !f.manualReset && usage.WeeklyPct < 0 {
		f.state = StateDONE
		f.log("warn", "weekly quota is unknown; automatic reset not authorized", "retry patrol", nil, nil)
		return time.Time{}
	}
	if f.Creds.SeatFingerprint == "" {
		f.state = StateDONE
		f.log("error", "credential seat is unknown; reset not authorized", "reload credentials on next patrol", nil, nil)
		return time.Time{}
	}
	snap.WeeklyPct = usage.WeeklyPct
	f.attempt = ResetAttempt{
		RedeemRequestID: uuid.NewString(),
		TargetCreditID:  target.ID,
		PreSnapshot:     snap,
		StartedAt:       now,
		Attempt:         0,
		SeatFingerprint: f.Creds.SeatFingerprint,
	}
	f.state = StateRESETTING
	f.log("info",
		fmt.Sprintf("confirmed target credit %s (pre: credits=%d, weekly=%d%%); sending reset request",
			shortID(target.ID), snap.AvailableCount, snap.WeeklyPct),
		"send reset request", nil,
		map[string]any{
			"credit_id":         target.ID,
			"pre_credits":       snap.AvailableCount,
			"pre_weekly_pct":    snap.WeeklyPct,
			"target_expires_at": target.ExpiresAt,
			"redeem_request_id": f.attempt.RedeemRequestID,
		})
	return f.now()
}

func (f *AccountFSM) stepRESETTING() time.Time {
	now := f.now()
	if !f.validAttemptLocked() || f.Creds.SeatFingerprint != f.attempt.SeatFingerprint {
		return f.holdLocked("reset identity is missing or changed; retaining the original attempt")
	}
	if f.attempt.Accepted {
		f.state = StateVERIFYING
		return now
	}
	if f.attempt.RetryStopped || f.attempt.Dispatches >= 1+len(resetRetryDelays) {
		return f.holdLocked("reset retry budget stopped; reconciliation only")
	}
	if now.Before(f.attempt.NextRetryAt) {
		return f.attempt.NextRetryAt
	}
	credits, err := f.Client.ListCredits(f.Creds)
	if err != nil {
		return f.holdLocked("pre-dispatch credit check failed: " + err.Error())
	}
	if !f.targetAvailableLocked(credits) {
		if f.attempt.Dispatches == 0 {
			f.state = StateDONE
			f.log("warn", "pinned credit unavailable before first dispatch", "abandon unsent attempt", nil, nil)
			return time.Time{}
		}
		f.state = StateVERIFYING
		return now
	}
	// Checkpoint the actual send count before the POST. A process restart
	// cannot turn a sent initial request or retry into a fresh budget.
	previousRetryAt := f.attempt.NextRetryAt
	f.attempt.Dispatches++
	// Save the earliest next replay time before an unknown response can be
	// lost. Reconciliation may read sooner, but cannot bypass this backoff.
	f.attempt.NextRetryAt = now.Add(backoffForAttempt(f.attempt.Dispatches))
	f.nextWake = now
	if err := f.checkpointLocked(); err != nil {
		f.attempt.Dispatches--
		f.attempt.NextRetryAt = previousRetryAt
		next := now.Add(credentialRetryInterval)
		f.log("error", "reset checkpoint failed: "+err.Error(), "retry checkpoint without dispatch", &next, nil)
		return next
	}
	resp, err := f.Client.Consume(f.Creds, f.attempt.RedeemRequestID, f.attempt.TargetCreditID)
	switch classifyConsume(resp, err) {
	case decisionSuccess:
		f.attempt.Accepted = true
		f.state = StateVERIFYING
		f.verifyDue = now.Add(postResetVerifyDelay)
		f.nextWake = f.verifyDue
		f.log("info",
			fmt.Sprintf("reset request accepted (code=%s, windows_reset=%d); will verify in %v",
				resp.Code, resp.WindowsReset, postResetVerifyDelay),
			fmt.Sprintf("verify in %v", postResetVerifyDelay), &f.verifyDue,
			map[string]any{
				"redeem_request_id": f.attempt.RedeemRequestID,
				"credit_id":         f.attempt.TargetCreditID,
				"consume_code":      resp.Code,
				"windows_reset":     resp.WindowsReset,
			})
		return f.verifyDue
	case decisionStop:
		knownRejection := err == nil && (resp.Code == ConsumeCodeNoCredit || resp.Code == ConsumeCodeNothingToReset) || IsClientError(err)
		if !knownRejection || f.attempt.Dispatches > 1 {
			f.attempt.RetryStopped = true
			return f.holdLocked("consume outcome unresolved; retaining the original credit and request ID")
		}
		f.state = StateDONE
		msg := fmt.Sprintf("reset stopped: server returned %s", resp.Code)
		if err != nil {
			msg = fmt.Sprintf("reset stopped: %v", err)
		}
		f.log("warn", msg, "abandon", nil, map[string]any{"redeem_request_id": f.attempt.RedeemRequestID})
		return time.Time{}
	case decisionRetry:
		f.attempt.Attempt = f.attempt.Dispatches
		if f.attempt.Dispatches >= 1+len(resetRetryDelays) {
			f.attempt.RetryStopped = true
			return f.holdLocked(fmt.Sprintf("retries exhausted after %d sends; outcome unresolved", f.attempt.Dispatches))
		}
		backoff := backoffForAttempt(f.attempt.Attempt)
		f.attempt.NextRetryAt = now.Add(backoff)
		f.nextWake = f.attempt.NextRetryAt
		f.log("warn",
			fmt.Sprintf("reset failed (attempt %d): %v; retrying with same idempotency key", f.attempt.Attempt, err),
			fmt.Sprintf("retry in %v", backoff), &f.attempt.NextRetryAt,
			map[string]any{"redeem_request_id": f.attempt.RedeemRequestID, "attempt": f.attempt.Attempt})
		return f.attempt.NextRetryAt
	}
	return time.Time{}
}

func (f *AccountFSM) stepVERIFYING() time.Time {
	credits, err := f.Client.ListCredits(f.Creds)
	if err != nil {
		return f.holdLocked("verification credit check failed: " + err.Error())
	}
	return f.verifySnapshotLocked(credits, true)
}

// Reconciliation never selects a credit. A replay is permitted only by the
// existing same-request, same-credit idempotent protocol and original budget.
// Inventory presence alone is not proof that the previous POST had no effect.
func (f *AccountFSM) stepRECONCILING() time.Time {
	if !f.validAttemptLocked() || f.Creds.SeatFingerprint != f.attempt.SeatFingerprint {
		return f.holdLocked("cannot reconcile an incomplete attempt or unknown credential seat")
	}
	credits, err := f.Client.ListCredits(f.Creds)
	if err != nil {
		return f.holdLocked("reconciliation credit check failed: " + err.Error())
	}
	if !f.targetAvailableLocked(credits) {
		f.attempt.RetryStopped = true
		return f.verifySnapshotLocked(credits, false)
	}
	if f.attempt.Accepted || f.attempt.RetryStopped || f.attempt.Dispatches >= 1+len(resetRetryDelays) {
		return f.holdLocked("original credit still available; waiting for authoritative reset evidence")
	}
	f.state = StateRESETTING
	wake := f.now()
	if f.attempt.NextRetryAt.After(wake) {
		wake = f.attempt.NextRetryAt
	}
	f.log("warn", "reconciled pinned target; continuing the original same-ID retry protocol", "retry original request", &wake, nil)
	return wake
}

func (f *AccountFSM) targetAvailableLocked(snapshot Snapshot) bool {
	for _, credit := range availableCreditsSorted(snapshot.Credits, f.now()) {
		if credit.ID == f.attempt.TargetCreditID {
			return true
		}
	}
	return false
}

func (f *AccountFSM) holdLocked(message string) time.Time {
	f.state = StateRECONCILING
	next := f.now().Add(f.Cfg.RefreshInterval)
	f.log("warn", message, "reconcile original attempt; no replacement credit", &next,
		map[string]any{"redeem_request_id": f.attempt.RedeemRequestID, "credit_id": f.attempt.TargetCreditID})
	return next
}

func (f *AccountFSM) verifySnapshotLocked(credits Snapshot, recordFailure bool) time.Time {
	usage, err := f.Client.GetUsage(f.Creds)
	if err != nil {
		return f.holdLocked("verification usage check failed: " + err.Error())
	}
	// Update lastSnapshot with post-reset data so the UI shows the new state.
	credits.WeeklyPct = usage.WeeklyPct
	f.lastSnapshot = credits

	// Consumption evidence is separate from quota evidence. Expiry, omission
	// or a count change cannot establish that an ambiguous POST consumed the
	// target. A recognized response for this attempt or the exact target's
	// returned redeemed status is required under the upstream protocol.
	targetStatus := credits.CreditStatuses[f.attempt.TargetCreditID]
	consumptionKnown := f.attempt.Accepted || targetStatus == "redeemed"
	targetGone := true
	for _, c := range credits.Credits {
		if c.ID == f.attempt.TargetCreditID && c.Status == "available" {
			targetGone = false
			break
		}
	}
	countDown1 := credits.AvailableCount == f.attempt.PreSnapshot.AvailableCount-1
	weeklyIncreased := f.attempt.PreSnapshot.WeeklyPct >= 0 && usage.WeeklyPct > f.attempt.PreSnapshot.WeeklyPct
	quotaUp := usage.WeeklyPct >= 0 && (usage.WeeklyPct >= 100 || weeklyIncreased)

	f.state = StateDONE
	// Build detailed details map with full before/after snapshot.
	details := map[string]any{
		"credit_id":         f.attempt.TargetCreditID,
		"redeem_request_id": f.attempt.RedeemRequestID,
		"pre_credits":       f.attempt.PreSnapshot.AvailableCount,
		"post_credits":      credits.AvailableCount,
		"pre_weekly_pct":    f.attempt.PreSnapshot.WeeklyPct,
		"post_weekly_pct":   usage.WeeklyPct,
		"target_gone":       targetGone,
		"target_status":     targetStatus,
		"consume_accepted":  f.attempt.Accepted,
		"consumption_known": consumptionKnown,
		"count_down_1":      countDown1,
		"quota_recovered":   quotaUp,
		"weekly_increased":  weeklyIncreased,
	}

	success := consumptionKnown && targetGone && countDown1 && quotaUp
	if !success {
		f.state = StateRECONCILING
		f.attempt.RetryStopped = true
	}

	switch {
	case success:
		f.log("info",
			fmt.Sprintf("reset verified OK: credits %d→%d, weekly %d%%→%d%%, target %s consumed",
				f.attempt.PreSnapshot.AvailableCount, credits.AvailableCount,
				f.attempt.PreSnapshot.WeeklyPct, usage.WeeklyPct,
				shortID(f.attempt.TargetCreditID)),
			"cycle complete", nil, details)
	case !consumptionKnown:
		f.log("warn", fmt.Sprintf("consume outcome unresolved: target status=%q; disappearance or expiry does not establish redemption", targetStatus),
			"reconcile original attempt", nil, details)
	case targetGone && countDown1:
		f.log("warn",
			fmt.Sprintf("reset PARTIAL: credits %d→%d ok but weekly %d%%→%d%% (delayed recovery?)",
				f.attempt.PreSnapshot.AvailableCount, credits.AvailableCount,
				f.attempt.PreSnapshot.WeeklyPct, usage.WeeklyPct),
			"reconcile original attempt", nil, details)
	default:
		f.log("error",
			fmt.Sprintf("reset MISMATCH: target_gone=%v count_down_1=%v quota_recovered=%v; retaining the original attempt",
				targetGone, countDown1, quotaUp),
			"reconcile original attempt", nil, details)
	}

	// Capture the reset result for history. NOT cleared by Reset().
	if success || recordFailure {
		f.pendingHistory = &ResetRecord{
			Timestamp:     f.now(),
			AuthID:        f.AuthID,
			CreditID:      f.attempt.TargetCreditID,
			Success:       success,
			PreWeeklyPct:  f.attempt.PreSnapshot.WeeklyPct,
			PostWeeklyPct: usage.WeeklyPct,
			PreCredits:    f.attempt.PreSnapshot.AvailableCount,
			PostCredits:   credits.AvailableCount,
		}
	}
	if success {
		return time.Time{}
	}
	return f.now().Add(f.Cfg.RefreshInterval)
}

// Reset returns a DONE FSM to IDLE for the next patrol cycle. Called by the
// worker after a cycle completes. Thread-safe. Does NOT clear pendingHistory
// — the worker collects it separately via TakePendingHistory().
func (f *AccountFSM) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.attempt.RedeemRequestID != "" {
		if f.state != StateDONE || f.checkpointLocked() != nil {
			return
		}
	}
	f.resetLocked()
}

// resetLocked is Reset's body for callers already holding mu (StepAndSettle).
func (f *AccountFSM) resetLocked() {
	f.state = StateIDLE
	f.attempt = ResetAttempt{}
	f.verifyDue = time.Time{}
	f.nextWake = time.Time{}
	f.scheduledCreditID = ""
	f.manualReset = false
}

// TakePendingHistory returns the reset result captured in stepVERIFYING and
// clears it. Returns nil if no pending history. Thread-safe.
func (f *AccountFSM) TakePendingHistory() *ResetRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec := f.pendingHistory
	f.pendingHistory = nil
	return rec
}

// shortID truncates a long credit/redeem ID for readable log messages.
// "RateLimitResetCredit_d7087f83469c819182a87d5916512c9c" -> "d7087f83…"
func shortID(id string) string {
	const prefix = "RateLimitResetCredit_"
	s := strings.TrimPrefix(id, prefix)
	if len(s) > 10 {
		return s[:8] + "…"
	}
	return s
}
