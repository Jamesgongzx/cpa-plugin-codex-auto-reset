# Retain account authentication checks and reset activity

Status: validate
Owner: ai
Category: tooling
Created: 2026-11-08
Updated: 2026-11-08
Completed: none

## Outcome

Every configured Codex account has a periodic read-only authentication observation and durable redacted history that can be joined with existing account-use and conversation-switch events.

## Delivery

- Task UUID: b64fb1aa-2bb6-405d-94b3-10c7e810618d
- Primary repository: /Volumes/External/Project/tools/cpa-plugin-codex-auto-reset
- Private work: /Volumes/External/Project/tools/cpa-plugin-codex-auto-reset/.shared/tasks/b64fb1aa-2bb6-405d-94b3-10c7e810618d/work
- Sources: AutoReset main and origin/main, plus necessary supervised activation changes in cliproxy-reset-aware-routing main and origin/main.
- Live destination: existing stock-runtime AutoReset plugin; preserve host/router behavior, credentials, refresh journal, reset enrollment/history and active conversation ownership.
- Reader: this conversation and this task. Independent technical review required; no human visual approval gate.

## Requirements

1. Existing routing journal remains authoritative for model use and switches. AutoReset owns its provider-check/reset observations, with UTC timestamps, hashed account identity joinable to routing events, endpoint/operation, safe code/status, source and credential-generation fingerprint (never tokens). Retain bounded private append-only history for 14 days or 128 MiB, whichever limit is reached first; report write errors.
2. Check all host Codex accounts every 15 minutes using only GET usage with fresh host-provided credentials, independently of reset enrollment. Keep one lifecycle-owned monitor inside the existing plugin, finite request timeouts, no overlapping monitor loops after reconfiguration or shutdown, and no login/logout/refresh/redeem side effects. Existing redemption timing and enrollment remain unchanged.
3. Instrument existing AutoReset usage, credit-list and consume outcomes, including success, error and reset verification. Keep last authentication success, first rejection in the current rejected episode, latest check/status, recovery and relevant last-known quota/reset observations durable. Distinguish HTTP401/explicit revocation from quota, HTTP403 permission, network errors and malformed responses. Credential replacement must not falsely assign old rejection to a new grant.
4. Authenticated management status exposes health independently of core eligibility and reset FSM. Preserve last-known facts through failed checks and restart, labelled by observation time. No claim to know the provider internal revocation time/cause; detection has sampling and outage gaps.
5. Test regressions before implementation where feasible. Use synthetic endpoints/credentials for revocation, recovery, quota, timeout, malformed body and redaction. Verify retention, persistence, all-account coverage, no consume during health checks, and lifecycle stop/reconfigure behavior.
6. Build and review off-service. Safely install accepted artifacts with the existing supervised activation pattern, independent recovery controller and exact hashes. Do not force a real auth failure, refresh, quota exhaustion or redemption. Verify healthy existing accounts via bounded read-only checks, new evidence and management status.

## User Questions And Answers

Q-001: User asks whether validity is checked periodically and requests enough logs for account use, AutoReset and revoked authentication. Existing patrol checks only enrolled accounts at 12-hour intervals; it does not propagate rejection to the routing evidence. Resolution: add read-only 15-minute health coverage for all accounts without changing reset enrollment.
Q-002: User asks whether authentication is good now. At 2026-11-08T21:27Z, direct usage/credit checks succeed for all five accounts. Main account has zero weekly quota and zero reset credits; next weekly reset 2026-11-12T04:06:32Z. Earlier token_revoked remains historical evidence, not current state.

## User Approval

User: “Yes we need enough log to know. Like auto reset when account used when account auth revoked. Do we check account validity periodically for auth? Auth good now right”. Direct implementation approval in this conversation, November 8. Prior no-session-break constraint continues; only reviewed supervised activation is permitted.

### Result Approval Before Integration

Not required. Technical review and live delivery evidence required.

## Current State

Implementation and independent review passed. Accepted AutoReset source is 2af81532ff22cf22b12c1ddacb23622360135ca4, version 0.1.15-health.1; exact native artifact hash be4e4e7baf217eaddbd567c3dd8ef1652cc280f54e2f91fdc6b37163e9ebfac7. The exact-plugin isolated native smoke passed. Router activation and smoke tooling from codex/auth-observability-activation was fast-forwarded into main and freshly verified on origin/main at 2b0eac6fb9817e3bf3e18484f043f045f0220761. AutoReset main publication and supervised live installation remain.

## Documentation Impact

- Updated: README.md and this task with check cadence, event coverage and evidence limitations.
- Updated: router docs/install.md for safe AutoReset artifact replacement, if needed.

## Acceptance Checks

- C1: All accounts, including reset-disabled ones, receive bounded scheduled GET checks; no consumes or credential changes are caused by health monitoring.
- C2: Success, explicit revocation/401, recovery/new credential, permission/quota and transport/parser errors retain correct independent health state, timestamps and last-known facts through restart.
- C3: Existing credit-list/usage/consume/verification traffic is observed with safe codes and correlation identity; redaction, bounded retention and persistence failures are tested.
- C4: Monitor start/stop/reconfiguration is safe; existing reset FSM tests and appropriate race tests pass; native build/host compatibility checked.
- C5: Independent review accepts exact candidate. Safe activation and rollback paths are tested; live artifacts and read-only health/evidence verified without induced failures or lost sessions. Both repositories integrated/published and remote commits checked.

## Verification

Existing evidence establishes a blind spot between a November 7 quota-driven switch and a later AutoReset 401; no provider causation is established. Current auth success does not erase that incident. Private incident-baseline.json retains the sanitized observations.

Independent reports review-health-01.md and review-health-02.md retain rejected candidates and concrete regression findings: diagnostic failure isolation, recovery metadata, malformed-response classification, old-token ordering, unavailable credentials, actual reset verification, and same-token overlap. Corrected full Go/race/vet/native builds pass. Independent review-health-04.md accepts final candidate 2af8153, including historical last_rejected and last_rejected_generation retained separately from current state. Native-smoke-v4.json verifies that exact artifact with the unchanged live host and router in isolation; no credentials or provider requests were used.

Activation reports review-activation-01.md through review-activation-03.md retain configuration/plist drift and smoke-contract corrections. Exact candidate 2b0eac6 is accepted with 55 Python checks. Native-before.json records the old plugin failing the new health contract as expected; native-smoke-v2.json verifies f46c343 native registration, version, authenticated health status, empty account map, 15-minute cadence, no provider calls and clean shutdown.

Activation idle checks cover reported existing bindings only, not a global atomic request fence. New requests can arrive after the check. Supervised recovery preserves session ownership and restores only the prior plugin on failure, never credentials or reset state.

## Dependencies And Risks

Native library update requires controlled activation of the proxy. The supervisor must recover independently while the chat transport is unavailable. No old credentials or reset state may be restored. Poll timestamps bound observation only, not exact provider revocation.
