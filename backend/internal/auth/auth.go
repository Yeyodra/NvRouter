// Package auth manages dashboard authentication: a single dashboard password
// (argon2id-hashed) and short-lived HMAC-signed session tokens.
//
// It is intentionally lightweight — one shared dashboard credential rather than
// per-user accounts — matching KeiRouter's local-first, single-operator model.
// On first run a default password is seeded and an onboarding flow guides the
// operator to change it. Session tokens are signed with a key persisted in
// settings so sessions survive restarts.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mydisha/keirouter/backend/internal/crypto"
	"github.com/mydisha/keirouter/backend/internal/store"
)

// DefaultPassword is seeded on first run. The onboarding flow prompts the
// operator to change it; a warning is logged while it remains in use.
const DefaultPassword = "keirouter"

// Settings keys used to persist auth state.
const (
	keyPasswordHash = "auth.password_hash"
	keySigningKey   = "auth.signing_key"
	keySessionGen   = "auth.session_generation"
	keyRevoked      = "auth.revoked_sessions"
	keyOnboarding   = "onboarding.complete"
)

// ErrInvalidPassword is returned when a login password does not match.
var ErrInvalidPassword = errors.New("auth: invalid password")

// Service provides password and session operations over the settings store.
type Service struct {
	settings   *store.SettingsRepo
	signingKey []byte
	ttl        time.Duration
	mu         sync.RWMutex
	generation int64
	revoked    map[string]int64
}

// New builds an auth Service. configKey, when non-empty, overrides the persisted
// signing key (e.g. from KEIROUTER_SECURITY__JWT_SECRET).
func New(settings *store.SettingsRepo, configKey string, ttl time.Duration) *Service {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Service{settings: settings, signingKey: []byte(configKey), ttl: ttl, revoked: make(map[string]int64)}
}

// EnsureDefaults seeds a default password and signing key on first run. It
// returns true when the default password was just created, so the caller can
// warn the operator. It is safe to call on every startup.
func (s *Service) EnsureDefaults(ctx context.Context) (seededDefault bool, err error) {
	// Signing key: prefer configured value, else load/generate a persisted one.
	if len(s.signingKey) == 0 {
		key, kerr := s.loadOrCreateSigningKey(ctx)
		if kerr != nil {
			return false, kerr
		}
		s.signingKey = key
	}
	if err := s.loadSessionState(ctx); err != nil {
		return false, err
	}

	// Password: seed the default if none exists yet.
	if _, gerr := s.settings.Get(ctx, keyPasswordHash); errors.Is(gerr, store.ErrNotFound) {
		hash, herr := crypto.HashPassword(DefaultPassword)
		if herr != nil {
			return false, herr
		}
		if serr := s.settings.Set(ctx, keyPasswordHash, hash); serr != nil {
			return false, serr
		}
		_ = s.settings.Set(ctx, keyOnboarding, "false")
		return true, nil
	} else if gerr != nil {
		return false, gerr
	}
	return false, nil
}

func (s *Service) loadOrCreateSigningKey(ctx context.Context) ([]byte, error) {
	if v, err := s.settings.Get(ctx, keySigningKey); err == nil {
		decoded, derr := base64.StdEncoding.DecodeString(v)
		if derr == nil && len(decoded) >= 32 {
			return decoded, nil
		}
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("auth: generate signing key: %w", err)
	}
	if err := s.settings.Set(ctx, keySigningKey, base64.StdEncoding.EncodeToString(key)); err != nil {
		return nil, err
	}
	return key, nil
}

// VerifyPassword reports whether the given password matches the stored hash.
func (s *Service) VerifyPassword(ctx context.Context, password string) (bool, error) {
	hash, err := s.settings.Get(ctx, keyPasswordHash)
	if err != nil {
		return false, err
	}
	return crypto.VerifyPassword(password, hash)
}

// SetPassword changes the dashboard password.
func (s *Service) SetPassword(ctx context.Context, newPassword string) error {
	if len(newPassword) < 6 {
		return errors.New("auth: password must be at least 6 characters")
	}
	hash, err := crypto.HashPassword(newPassword)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Revoke existing sessions before committing the new password. If the later
	// password write fails, the safe failure mode is a forced re-login rather
	// than leaving stolen sessions valid after a reported password-change error.
	if err := s.bumpGenerationLocked(ctx); err != nil {
		return err
	}
	return s.settings.Set(ctx, keyPasswordHash, hash)
}

// AuthenticateSession verifies a password and issues its session while holding
// the same lock used by SetPassword. This prevents an old-password request from
// verifying before a password change and receiving a new-generation token after it.
func (s *Service) AuthenticateSession(ctx context.Context, password string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hash, err := s.settings.Get(ctx, keyPasswordHash)
	if err != nil {
		return "", err
	}
	ok, err := crypto.VerifyPassword(password, hash)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrInvalidPassword
	}
	return s.issueSessionLocked()
}

// UsingDefaultPassword reports whether the current password still matches the
// seeded default, so the UI can nudge the operator to change it.
func (s *Service) UsingDefaultPassword(ctx context.Context) bool {
	ok, err := s.VerifyPassword(ctx, DefaultPassword)
	return err == nil && ok
}

// OnboardingComplete reports whether the operator finished onboarding.
func (s *Service) OnboardingComplete(ctx context.Context) bool {
	v, err := s.settings.Get(ctx, keyOnboarding)
	return err == nil && v == "true"
}

// CompleteOnboarding marks onboarding finished.
func (s *Service) CompleteOnboarding(ctx context.Context) error {
	return s.settings.Set(ctx, keyOnboarding, "true")
}

// session is the signed token payload.
type session struct {
	Sub string `json:"sub"`
	Exp int64  `json:"exp"`
	Gen int64  `json:"gen"`
	JTI string `json:"jti"`
}

// IssueSession mints a signed session token valid for the configured TTL.
func (s *Service) IssueSession() (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.issueSessionLocked()
}

func (s *Service) issueSessionLocked() (string, error) {
	jtiBytes := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, jtiBytes); err != nil {
		return "", fmt.Errorf("auth: generate session id: %w", err)
	}
	gen := s.generation
	payload := session{Sub: "dashboard", Exp: time.Now().Add(s.ttl).Unix(), Gen: gen, JTI: base64.RawURLEncoding.EncodeToString(jtiBytes)}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	body := base64.RawURLEncoding.EncodeToString(raw)
	return body + "." + s.sign(body), nil
}

// VerifySession reports whether a session token is valid and unexpired.
func (s *Service) VerifySession(token string) bool {
	p, ok := s.parseSession(token)
	if !ok {
		return false
	}
	now := time.Now().Unix()
	s.mu.RLock()
	gen := s.generation
	revokedUntil := s.revoked[p.JTI]
	s.mu.RUnlock()
	return p.Sub == "dashboard" && p.JTI != "" && now < p.Exp && p.Gen == gen && revokedUntil <= now
}

// RevokeSession invalidates one presented session token while leaving other
// browser sessions active. Revocations persist across process restarts.
func (s *Service) RevokeSession(ctx context.Context, token string) error {
	p, ok := s.parseSession(token)
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	for jti, exp := range s.revoked {
		if exp <= now {
			delete(s.revoked, jti)
		}
	}
	s.revoked[p.JTI] = p.Exp
	raw, err := json.Marshal(s.revoked)
	if err != nil {
		return err
	}
	return s.settings.Set(ctx, keyRevoked, string(raw))
}

func (s *Service) parseSession(token string) (session, bool) {
	body, sig, ok := strings.Cut(token, ".")
	if !ok || !hmac.Equal([]byte(sig), []byte(s.sign(body))) {
		return session{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return session{}, false
	}
	var p session
	if err := json.Unmarshal(raw, &p); err != nil {
		return session{}, false
	}
	return p, true
}

func (s *Service) loadSessionState(ctx context.Context) error {
	gen := int64(1)
	if value, err := s.settings.Get(ctx, keySessionGen); err == nil {
		parsed, perr := strconv.ParseInt(value, 10, 64)
		if perr != nil || parsed < 1 {
			return errors.New("auth: invalid session generation")
		}
		gen = parsed
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	} else if err := s.settings.Set(ctx, keySessionGen, "1"); err != nil {
		return err
	}
	revoked := make(map[string]int64)
	if value, err := s.settings.Get(ctx, keyRevoked); err == nil {
		if err := json.Unmarshal([]byte(value), &revoked); err != nil {
			return fmt.Errorf("auth: decode revoked sessions: %w", err)
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	s.mu.Lock()
	s.generation = gen
	s.revoked = revoked
	s.mu.Unlock()
	return nil
}

func (s *Service) bumpGenerationLocked(ctx context.Context) error {
	next := s.generation + 1
	if err := s.settings.Set(ctx, keySessionGen, strconv.FormatInt(next, 10)); err != nil {
		return err
	}
	s.generation = next
	s.revoked = make(map[string]int64)
	return s.settings.Set(ctx, keyRevoked, "{}")
}

// TTL returns the session lifetime.
func (s *Service) TTL() time.Duration { return s.ttl }

func (s *Service) sign(body string) string {
	mac := hmac.New(sha256.New, s.signingKey)
	mac.Write([]byte(body))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
