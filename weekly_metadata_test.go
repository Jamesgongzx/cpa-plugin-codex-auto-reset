package main

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestUsageRetainsWeeklyRoutingMetadata(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, position := range []string{"primary_window", "secondary_window"} {
		t.Run(position, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]any{"rate_limit": map[string]any{position: map[string]any{"limit_window_seconds": 604800, "used_percent": 9, "reset_at": now.Add(24 * time.Hour).Unix()}}})
			snap, err := parseUsageResponse(raw, now)
			if err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(snap)
			var saved map[string]any
			if err := json.Unmarshal(encoded, &saved); err != nil {
				t.Fatal(err)
			}
			if saved["weekly_reset_at"] != now.Add(24*time.Hour).Format(time.RFC3339) || saved["weekly_window_seconds"] != float64(604800) || saved["weekly_observed_at"] != now.Format(time.RFC3339) {
				t.Fatalf("weekly metadata lost: %s", encoded)
			}
		})
	}
}

func TestWeeklyRoutingMetadataDoesNotUseOtherQuotaOrInventReset(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, kind := range []string{"code-review", "short", "missing", "past", "too-far"} {
		t.Run(kind, func(t *testing.T) {
			window := map[string]any{"limit_window_seconds": 604800, "used_percent": 9, "reset_at": now.Add(time.Hour).Unix()}
			key := "rate_limit"
			switch kind {
			case "code-review":
				key = "code_review_rate_limit"
			case "short":
				window["limit_window_seconds"] = 18000
			case "missing":
				delete(window, "reset_at")
			case "past":
				window["reset_at"] = now.Add(-time.Hour).Unix()
			case "too-far":
				window["reset_at"] = now.Add(30 * 24 * time.Hour).Unix()
			}
			raw, _ := json.Marshal(map[string]any{key: map[string]any{"primary_window": window}})
			snap, err := parseUsageResponse(raw, now)
			if err != nil {
				t.Fatal(err)
			}
			if !snap.WeeklyResetAt.IsZero() || snap.WeeklyWindowSeconds != 0 || !snap.WeeklyObservedAt.IsZero() {
				t.Fatalf("unknown weekly metadata became known: %+v", snap)
			}
		})
	}
}

type metadataClient struct {
	fakeClient
	usage Snapshot
}

func (f *metadataClient) GetUsage(CodexCredentials) (Snapshot, error) {
	f.usageCalls++
	return f.usage, nil
}
func TestExistingPatrolPersistsWeeklyMetadataWithoutExtraCalls(t *testing.T) {
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	client := &metadataClient{fakeClient: fakeClient{credits: map[string]*Credit{}}, usage: Snapshot{WeeklyPct: 9, WeeklyResetAt: now.Add(time.Hour), WeeklyWindowSeconds: 604800, WeeklyObservedAt: now}}
	fsm := NewAccountFSM("index-a", CodexCredentials{}, DefaultConfig(), client, func() time.Time { return now }, nil)
	fsm.Step()
	snap := fsm.LastSnapshot()
	if !snap.WeeklyResetAt.Equal(client.usage.WeeklyResetAt) || !snap.WeeklyObservedAt.Equal(now) || snap.WeeklyWindowSeconds != 604800 {
		t.Fatal(snap)
	}
	if client.listCalls != 1 || client.usageCalls != 1 || client.consumeCalls != 0 {
		t.Fatalf("collector behavior changed: %+v", client)
	}
	path := filepath.Join(t.TempDir(), "state.json")
	if err := saveState(path, &PluginState{Config: DefaultConfig(), Accounts: map[string]AccountRuntime{"index-a": {AuthID: "index-a", State: StateIDLE, LastSnapshot: snap}}}); err != nil {
		t.Fatal(err)
	}
	saved, _, err := loadState(path)
	if err != nil {
		t.Fatal(err)
	}
	if !saved.Accounts["index-a"].LastSnapshot.WeeklyResetAt.Equal(snap.WeeklyResetAt) {
		t.Fatal("metadata not retained")
	}
}
