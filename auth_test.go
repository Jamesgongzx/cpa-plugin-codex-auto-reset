package main

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func makeUnsignedJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	body, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(body)
	return strings.Join([]string{header, payload, ""}, ".")
}

func TestExtractCodexCredentialsTopLevel(t *testing.T) {
	idToken := makeUnsignedJWT(t, map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "acct_123",
		},
	})
	raw := json.RawMessage(`{"access_token":"access-1","id_token":"` + idToken + `"}`)
	creds, err := ExtractCodexCredentials(raw)
	if err != nil {
		t.Fatalf("ExtractCodexCredentials returned error: %v", err)
	}
	if creds.AccessToken != "access-1" || creds.ChatGPTAccountID != "acct_123" {
		t.Fatalf("creds = %#v", creds)
	}
}

func TestExtractCodexCredentialsNestedMetadata(t *testing.T) {
	idToken := makeUnsignedJWT(t, map[string]any{
		"https://api.openai.com/auth.chatgpt_account_id": "acct_nested",
	})
	raw := json.RawMessage(`{"tokens":{"access_token":"access-2"},"metadata":{"id_token":"` + idToken + `"}}`)
	creds, err := ExtractCodexCredentials(raw)
	if err != nil {
		t.Fatalf("ExtractCodexCredentials returned error: %v", err)
	}
	if creds.AccessToken != "access-2" || creds.ChatGPTAccountID != "acct_nested" {
		t.Fatalf("creds = %#v", creds)
	}
}

func TestExtractCodexCredentialsUsesStoredAccountIDWhenIDTokenMissing(t *testing.T) {
	raw := json.RawMessage(`{"access_token":"access-3","refresh_token":"refresh-3","account_id":"acct_stored","expired":"2026-06-21T10:00:00Z"}`)
	creds, err := ExtractCodexCredentials(raw)
	if err != nil {
		t.Fatalf("ExtractCodexCredentials returned error: %v", err)
	}
	if creds.AccessToken != "access-3" || creds.RefreshToken != "refresh-3" || creds.ChatGPTAccountID != "acct_stored" || creds.ExpiresAt.IsZero() {
		t.Fatalf("creds = %#v", creds)
	}
}

func TestExtractCodexCredentialsRejectsMissingAccessToken(t *testing.T) {
	_, err := ExtractCodexCredentials(json.RawMessage(`{"id_token":"x.y.z"}`))
	if err == nil {
		t.Fatalf("ExtractCodexCredentials accepted missing access token")
	}
}

func testSeatCredentials(t *testing.T, subject, workspace, tokenID string) CodexCredentials {
	t.Helper()
	token := makeUnsignedJWT(t, map[string]any{
		"sub": subject, "jti": tokenID,
		"https://api.openai.com/auth": map[string]any{"chatgpt_account_id": workspace},
	})
	raw, err := json.Marshal(map[string]string{"access_token": token, "id_token": token, "account_id": workspace})
	if err != nil {
		t.Fatal(err)
	}
	creds, err := ExtractCodexCredentials(raw)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

func TestCredentialSeatFingerprint_StableAcrossTokenRotation(t *testing.T) {
	first := testSeatCredentials(t, "user-one", "workspace", "token-one")
	rotated := testSeatCredentials(t, "user-one", "workspace", "token-two")
	otherUser := testSeatCredentials(t, "user-two", "workspace", "token-three")
	otherWorkspace := testSeatCredentials(t, "user-one", "other-workspace", "token-four")
	if first.SeatFingerprint == "" || first.SeatFingerprint != rotated.SeatFingerprint {
		t.Fatal("same subject and workspace did not retain their identity across token rotation")
	}
	if first.SeatFingerprint == otherUser.SeatFingerprint || first.SeatFingerprint == otherWorkspace.SeatFingerprint {
		t.Fatal("different user or workspace shared a seat fingerprint")
	}
}

func TestCredentialSeatFingerprint_RejectsUnknownOrConflictingClaims(t *testing.T) {
	access := testSeatCredentials(t, "user-one", "workspace", "access")
	other := testSeatCredentials(t, "user-two", "workspace", "other")
	missingSubject := makeUnsignedJWT(t, map[string]any{
		"https://api.openai.com/auth": map[string]any{"chatgpt_account_id": "workspace"},
	})
	for _, tc := range []struct{ name, access, id, workspace string }{
		{"opaque_bearer", "opaque", access.IDToken, "workspace"},
		{"missing_subject", missingSubject, "", "workspace"},
		{"changed_workspace", access.AccessToken, access.IDToken, "other-workspace"},
		{"conflicting_subject", access.AccessToken, other.IDToken, "workspace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if credentialSeatFingerprint(tc.access, tc.id, tc.workspace) != "" {
				t.Fatal("unknown or conflicting bearer identity authorized a seat")
			}
		})
	}
}
