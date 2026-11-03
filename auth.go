package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type CodexCredentials struct {
	AccessToken      string
	RefreshToken     string
	IDToken          string
	ChatGPTAccountID string
	ExpiresAt        time.Time
	SeatFingerprint  string
}

func ExtractCodexCredentials(raw json.RawMessage) (CodexCredentials, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return CodexCredentials{}, err
	}

	accessToken, ok := lookupStringDeep(doc, "access_token")
	if !ok || accessToken == "" {
		return CodexCredentials{}, errors.New("missing access_token")
	}
	refreshToken, _ := lookupStringDeep(doc, "refresh_token")
	idToken, _ := lookupStringDeep(doc, "id_token")
	expiresAt, _ := lookupTimeDeep(doc, "expired", "expire", "expires_at", "expiresAt")

	accountID, _ := lookupStringDeep(doc, "account_id")
	if accountID == "" {
		accountID, _ = lookupStringDeep(doc, "chatgpt_account_id")
	}
	if accountID == "" {
		extracted, err := extractChatGPTAccountID(idToken)
		if err != nil {
			return CodexCredentials{}, err
		}
		accountID = extracted
	}

	return CodexCredentials{
		AccessToken:      accessToken,
		RefreshToken:     refreshToken,
		IDToken:          idToken,
		ChatGPTAccountID: accountID,
		ExpiresAt:        expiresAt,
		SeatFingerprint:  credentialSeatFingerprint(accessToken, idToken, accountID),
	}, nil
}

// credentialSeatFingerprint binds the actual bearer subject to its workspace.
// It introspects host-owned tokens, without taking over validation or refresh.
// An opaque bearer, missing subject, or conflicting claim cannot authorize a
// reset. Account index, email and workspace alone do not identify a user seat.
func credentialSeatFingerprint(accessToken, idToken, accountID string) string {
	claims, err := decodeJWTClaims(accessToken)
	if err != nil || accountID == "" {
		return ""
	}
	subject, _ := stringFromMap(claims, "sub")
	workspace, err := extractChatGPTAccountID(accessToken)
	if subject == "" || err != nil || workspace != accountID {
		return ""
	}
	if idToken != "" {
		idClaims, err := decodeJWTClaims(idToken)
		if err != nil {
			return ""
		}
		if idSubject, ok := stringFromMap(idClaims, "sub"); ok && idSubject != subject {
			return ""
		}
		if idWorkspace, err := extractChatGPTAccountID(idToken); err == nil && idWorkspace != workspace {
			return ""
		}
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(workspace+"\x00"+subject)))
}

func extractChatGPTAccountID(idToken string) (string, error) {
	claims, err := decodeJWTClaims(idToken)
	if err != nil {
		return "", fmt.Errorf("missing chatgpt_account_id: %w", err)
	}
	if v, ok := stringFromMap(claims, "https://api.openai.com/auth.chatgpt_account_id"); ok && v != "" {
		return v, nil
	}
	if authClaim, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
		if v, ok := stringFromMap(authClaim, "chatgpt_account_id"); ok && v != "" {
			return v, nil
		}
	}
	if v, ok := lookupStringDeep(claims, "https://api.openai.com/auth.chatgpt_account_id"); ok && v != "" {
		return v, nil
	}
	if v, ok := lookupStringDeep(claims, "chatgpt_account_id"); ok && v != "" {
		return v, nil
	}
	return "", errors.New("missing chatgpt_account_id")
}

func decodeJWTClaims(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil, errors.New("invalid id_token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, err
	}
	return claims, nil
}

func lookupStringDeep(v any, key string) (string, bool) {
	switch typed := v.(type) {
	case map[string]any:
		if found, ok := stringFromMap(typed, key); ok {
			return found, true
		}
		for _, child := range typed {
			if found, ok := lookupStringDeep(child, key); ok {
				return found, true
			}
		}
	case []any:
		for _, child := range typed {
			if found, ok := lookupStringDeep(child, key); ok {
				return found, true
			}
		}
	}
	return "", false
}

func stringFromMap(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func lookupTimeDeep(v any, keys ...string) (time.Time, bool) {
	for _, key := range keys {
		if raw, ok := lookupStringDeep(v, key); ok {
			if parsed, ok := parseAuthTime(raw); ok {
				return parsed, true
			}
		}
	}
	return time.Time{}, false
}

func parseAuthTime(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func accessTokenExpired(credentials CodexCredentials, now time.Time) bool {
	if credentials.ExpiresAt.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	return !now.Add(2 * time.Minute).Before(credentials.ExpiresAt)
}
