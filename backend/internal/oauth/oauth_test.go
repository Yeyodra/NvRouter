package oauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestGeneratePKCE(t *testing.T) {
	p, err := GeneratePKCE(32)
	if err != nil {
		t.Fatal(err)
	}
	if p.Verifier == "" || p.Challenge == "" || p.State == "" {
		t.Fatal("expected non-empty verifier/challenge/state")
	}
	// Challenge must be S256(verifier) base64url.
	sum := sha256.Sum256([]byte(p.Verifier))
	want := base64.RawURLEncoding.EncodeToString(sum[:])
	if p.Challenge != want {
		t.Fatalf("challenge mismatch: got %q want %q", p.Challenge, want)
	}
	// base64url must not contain padding or +/.
	if strings.ContainsAny(p.Verifier, "+/=") {
		t.Errorf("verifier is not base64url: %q", p.Verifier)
	}
}

func TestGeneratePKCEUnique(t *testing.T) {
	a, _ := GeneratePKCE(32)
	b, _ := GeneratePKCE(32)
	if a.Verifier == b.Verifier || a.State == b.State {
		t.Fatal("expected unique PKCE values across calls")
	}
}

func TestConfigFor(t *testing.T) {
	for _, id := range []string{"claude", "codex", "github", "qwen", "xai", "gemini-cli", "clinepass", "grok-cli"} {
		cfg, ok := ConfigFor(id)
		if !ok {
			t.Errorf("expected OAuth config for %q", id)
			continue
		}
		if cfg.Provider != id {
			t.Errorf("config %q has wrong Provider %q", id, cfg.Provider)
		}
	}
	if _, ok := ConfigFor("does-not-exist"); ok {
		t.Error("expected no config for unknown provider")
	}
}

// TestGrokCLIConfigShape locks Grok Build device-code OAuth (not xai PKCE).
// Note: farm refresh scripts also send scope+redirect_uri; generic Refresh omits
// them unless ExtraTokenParams/refresh body is extended later.
func TestGrokCLIConfigShape(t *testing.T) {
	cfg, ok := ConfigFor("grok-cli")
	if !ok {
		t.Fatal("expected OAuth config for grok-cli")
	}
	if cfg.Flow != FlowDeviceCode {
		t.Fatalf("grok-cli flow: got %q want %q", cfg.Flow, FlowDeviceCode)
	}
	if cfg.ClientID != "b1a00492-073a-47ea-816f-4c329264a828" {
		t.Fatalf("grok-cli ClientID: got %q", cfg.ClientID)
	}
	if cfg.DeviceCodeURL != "https://auth.x.ai/oauth2/device/code" {
		t.Fatalf("grok-cli DeviceCodeURL: got %q", cfg.DeviceCodeURL)
	}
	if cfg.TokenURL != "https://auth.x.ai/oauth2/token" {
		t.Fatalf("grok-cli TokenURL: got %q", cfg.TokenURL)
	}
	if cfg.ExtraDeviceParams["referrer"] != "grok-build" {
		t.Fatalf("grok-cli ExtraDeviceParams referrer: got %#v", cfg.ExtraDeviceParams)
	}
	if cfg.UserAgent != "grok-shell/0.2.99 (linux; x86_64)" {
		t.Fatalf("grok-cli UserAgent: got %q", cfg.UserAgent)
	}
	if cfg.RefreshLead != 10*time.Minute {
		t.Fatalf("grok-cli RefreshLead: got %v want 10m", cfg.RefreshLead)
	}
	scopeJoin := strings.Join(cfg.Scopes, " ")
	for _, want := range []string{"conversations:read", "conversations:write", "grok-cli:access", "offline_access"} {
		if !strings.Contains(scopeJoin, want) {
			t.Errorf("grok-cli scopes missing %q; got %q", want, scopeJoin)
		}
	}
}

func TestXAIConfigUntouched(t *testing.T) {
	cfg, ok := ConfigFor("xai")
	if !ok {
		t.Fatal("expected OAuth config for xai")
	}
	if cfg.Flow != FlowAuthCodePKCE {
		t.Fatalf("xai must remain FlowAuthCodePKCE, got %q", cfg.Flow)
	}
	if cfg.DeviceCodeURL != "" {
		t.Fatalf("xai must not have DeviceCodeURL, got %q", cfg.DeviceCodeURL)
	}
	if cfg.ExtraDeviceParams != nil {
		t.Fatalf("xai must not set ExtraDeviceParams, got %#v", cfg.ExtraDeviceParams)
	}
	// xai scopes intentionally omit conversation scopes (grok-cli only).
	for _, s := range cfg.Scopes {
		if s == "conversations:write" || s == "conversations:read" {
			t.Fatalf("xai scopes must not include conversation scopes, got %v", cfg.Scopes)
		}
	}
}

// testJWT builds an unsigned JWT with the given JSON payload (header+sig ignored by decodeJWTPayload).
func testJWT(payload map[string]any) string {
	raw, _ := json.Marshal(payload)
	return "eyJhbGciOiJub25lIn0." + base64.RawURLEncoding.EncodeToString(raw) + ".sig"
}

func TestGrokCLIApplyTokenMetadataEmailFromIDToken(t *testing.T) {
	cfg, ok := ConfigFor("grok-cli")
	if !ok {
		t.Fatal("expected OAuth config for grok-cli")
	}
	tokens := &Tokens{
		AccessToken: "at",
		IDToken:     testJWT(map[string]any{"email": "user@x.ai", "sub": "sub-1"}),
	}
	cfg.applyTokenMetadata(tokens)
	if tokens.Email != "user@x.ai" {
		t.Fatalf("Email: got %q want user@x.ai", tokens.Email)
	}
	if tokens.Extra["email"] != "user@x.ai" {
		t.Fatalf("Extra[email]: got %#v", tokens.Extra)
	}
}

func TestGrokCLIFetchUserInfoBestEffortFailure(t *testing.T) {
	// /user 500 must not clear tokens or fail the OAuth mapping path.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer srv.Close()

	prev := grokCLIUserURL
	grokCLIUserURL = srv.URL
	defer func() { grokCLIUserURL = prev }()

	cfg, _ := ConfigFor("grok-cli")
	tokens := &Tokens{
		AccessToken:  "at",
		RefreshToken: "rt",
		Email:        "keep@x.ai",
		Extra:        map[string]string{"email": "keep@x.ai"},
	}
	cfg.FetchUserInfo(context.Background(), tokens)
	if tokens.AccessToken != "at" || tokens.RefreshToken != "rt" {
		t.Fatalf("tokens mutated on /user failure: %+v", tokens)
	}
	if tokens.Email != "keep@x.ai" {
		t.Fatalf("Email cleared on /user failure: %q", tokens.Email)
	}
	if tokens.Extra["email"] != "keep@x.ai" {
		t.Fatalf("Extra email cleared: %#v", tokens.Extra)
	}
	if tokens.Extra["userId"] != "" {
		t.Fatalf("unexpected userId after failed /user: %#v", tokens.Extra)
	}
}

func TestGrokCLIFetchUserInfoStoresUserID(t *testing.T) {
	var gotAuth, gotUA, gotTokenAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotUA = r.Header.Get("User-Agent")
		gotTokenAuth = r.Header.Get("x-xai-token-auth")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"email":"u@x.ai","userId":"uid-99","name":"Grok User"}`))
	}))
	defer srv.Close()

	prev := grokCLIUserURL
	grokCLIUserURL = srv.URL
	defer func() { grokCLIUserURL = prev }()

	cfg, _ := ConfigFor("grok-cli")
	tokens := &Tokens{AccessToken: "secret-at"}
	cfg.FetchUserInfo(context.Background(), tokens)

	if gotAuth != "Bearer secret-at" {
		t.Errorf("Authorization: got %q", gotAuth)
	}
	if gotUA != grokCLIUserAgent {
		t.Errorf("User-Agent: got %q", gotUA)
	}
	if gotTokenAuth != grokCLITokenAuth {
		t.Errorf("x-xai-token-auth: got %q", gotTokenAuth)
	}
	if tokens.Email != "u@x.ai" {
		t.Fatalf("Email: got %q", tokens.Email)
	}
	if tokens.Extra["userId"] != "uid-99" {
		t.Fatalf("Extra userId: got %#v", tokens.Extra)
	}
	if tokens.Extra["email"] != "u@x.ai" {
		t.Fatalf("Extra email: got %#v", tokens.Extra)
	}
	if tokens.DisplayName != "Grok User" {
		t.Fatalf("DisplayName: got %q", tokens.DisplayName)
	}
}

func TestAuthURLPKCE(t *testing.T) {
	cfg, _ := ConfigFor("claude")
	url := cfg.AuthURL("http://localhost:20180/callback", "state123", "challenge456")
	for _, want := range []string{
		"https://claude.ai/oauth/authorize?",
		"client_id=9d1c250a-e61b-44d9-88ed-5944d1962f5e",
		"code_challenge=challenge456",
		"code_challenge_method=S256",
		"state=state123",
		"response_type=code",
	} {
		if !strings.Contains(url, want) {
			t.Errorf("auth URL missing %q\ngot: %s", want, url)
		}
	}
}

func TestCodexAuthURLMatchesCLIFlow(t *testing.T) {
	cfg, _ := ConfigFor("codex")
	redirectURI := cfg.ResolveRedirectURI("http://localhost:20180/oauth/callback")
	if redirectURI != "http://localhost:1455/auth/callback" {
		t.Fatalf("codex redirect mismatch: got %q", redirectURI)
	}

	authURL := cfg.AuthURL(redirectURI, "state123", "challenge456")
	for _, want := range []string{
		"https://auth.openai.com/oauth/authorize?",
		"response_type=code",
		"client_id=app_EMoamEEZ73f0CkXaXp7hrann",
		"redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback",
		"scope=openid%20profile%20email%20offline_access",
		"code_challenge=challenge456",
		"code_challenge_method=S256",
		"id_token_add_organizations=true",
		"codex_cli_simplified_flow=true",
		"originator=codex_cli_rs",
		"state=state123",
	} {
		if !strings.Contains(authURL, want) {
			t.Errorf("codex auth URL missing %q\ngot: %s", want, authURL)
		}
	}
	if strings.Contains(authURL, "scope=openid+profile") {
		t.Fatalf("codex scope must use %%20 encoding, got: %s", authURL)
	}
}

func TestXAIAuthURLMatchesCLIFlow(t *testing.T) {
	cfg, _ := ConfigFor("xai")
	redirectURI := cfg.ResolveRedirectURI("http://localhost:20180/oauth/callback")
	if redirectURI != "http://127.0.0.1:56121/callback" {
		t.Fatalf("xai redirect mismatch: got %q", redirectURI)
	}

	authURL := cfg.AuthURL(redirectURI, "state123", "challenge456")
	for _, want := range []string{
		"https://auth.x.ai/oauth2/authorize?",
		"redirect_uri=http%3A%2F%2F127.0.0.1%3A56121%2Fcallback",
		"scope=openid%20profile%20email%20offline_access%20grok-cli%3Aaccess%20api%3Aaccess",
		"state=state123",
		"plan=generic",
		"referrer=cli-proxy-api",
	} {
		if !strings.Contains(authURL, want) {
			t.Errorf("xai auth URL missing %q\ngot: %s", want, authURL)
		}
	}
	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatal(err)
	}
	if nonce := parsed.Query().Get("nonce"); len(nonce) != 32 {
		t.Fatalf("expected 16-byte hex nonce, got %q", nonce)
	}
}

func TestAuthURLDeviceCodeFlowDistinct(t *testing.T) {
	cfg, _ := ConfigFor("github")
	if cfg.Flow != FlowDeviceCode {
		t.Fatalf("github should be device-code flow, got %q", cfg.Flow)
	}
}

func TestRefreshURLFallback(t *testing.T) {
	claude, _ := ConfigFor("claude")
	if claude.refreshURL() != claude.TokenURL {
		t.Error("refresh URL should default to token URL when unset")
	}
	cline, _ := ConfigFor("cline")
	if cline.refreshURL() != "https://api.cline.bot/api/v1/auth/refresh" {
		t.Errorf("cline refresh URL should use explicit RefreshURL, got %q", cline.refreshURL())
	}
}

func TestClinepassMirrorsCline(t *testing.T) {
	cline, _ := ConfigFor("cline")
	cp, ok := ConfigFor("clinepass")
	if !ok {
		t.Fatal("expected OAuth config for clinepass")
	}
	if cp.Flow != FlowAuthCode {
		t.Errorf("clinepass flow = %v, want FlowAuthCode", cp.Flow)
	}
	if cp.AuthorizeURL != cline.AuthorizeURL || cp.TokenURL != cline.TokenURL || cp.refreshURL() != cline.refreshURL() {
		t.Error("clinepass should share cline's auth endpoints")
	}
	if !cp.SkipStandardAuthParams || cp.TokenContentType != "json" {
		t.Error("clinepass should reuse cline's non-standard authorize/token params")
	}
}

func TestGrokCliDeviceFlow(t *testing.T) {
	cfg, ok := ConfigFor("grok-cli")
	if !ok {
		t.Fatal("expected OAuth config for grok-cli")
	}
	if cfg.Flow != FlowDeviceCode {
		t.Errorf("grok-cli flow = %v, want FlowDeviceCode", cfg.Flow)
	}
	if cfg.DeviceCodeURL == "" || cfg.TokenURL == "" {
		t.Error("grok-cli must define device-code and token URLs")
	}
	if cfg.refreshURL() != cfg.TokenURL {
		t.Errorf("grok-cli refresh URL = %q, want %q", cfg.refreshURL(), cfg.TokenURL)
	}
}

func TestSessionStore(t *testing.T) {
	s := NewSessionStore()
	s.Put("k1", &Session{Provider: "claude", State: "k1", Verifier: "v"})
	got, ok := s.Get("k1")
	if !ok || got.Verifier != "v" {
		t.Fatal("expected to retrieve stored session")
	}
	s.Delete("k1")
	if _, ok := s.Get("k1"); ok {
		t.Fatal("expected session deleted")
	}
}

func TestRequestDeviceCodeExtraDeviceParams(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"device_code":"dc","user_code":"UC","verification_uri":"https://example.com","expires_in":600,"interval":5}`))
	}))
	defer srv.Close()

	cfg := ProviderConfig{
		ClientID:        "client-1",
		DeviceCodeURL:   srv.URL,
		Scopes:          []string{"api:access"},
		ExtraDeviceParams: map[string]string{"referrer": "grok-build"},
	}
	dc, err := cfg.RequestDeviceCode(context.Background(), "")
	if err != nil {
		t.Fatalf("RequestDeviceCode: %v", err)
	}
	if dc.DeviceCode != "dc" {
		t.Fatalf("device_code: got %q", dc.DeviceCode)
	}
	form, err := url.ParseQuery(gotBody)
	if err != nil {
		t.Fatal(err)
	}
	if form.Get("client_id") != "client-1" {
		t.Errorf("client_id: got %q", form.Get("client_id"))
	}
	if form.Get("scope") != "api:access" {
		t.Errorf("scope: got %q", form.Get("scope"))
	}
	if form.Get("referrer") != "grok-build" {
		t.Errorf("referrer: got %q want grok-build", form.Get("referrer"))
	}
}

// TestGrokCLIRequestDeviceCodeSendsReferrer locks the real grok-cli ProviderConfig
// (ConfigFor) through RequestDeviceCode so ExtraDeviceParams.referrer reaches the form.
func TestGrokCLIRequestDeviceCodeSendsReferrer(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"device_code":"dc","user_code":"UC","verification_uri":"https://example.com","expires_in":600,"interval":5}`))
	}))
	defer srv.Close()

	cfg, ok := ConfigFor("grok-cli")
	if !ok {
		t.Fatal("expected OAuth config for grok-cli")
	}
	cfg.DeviceCodeURL = srv.URL
	if _, err := cfg.RequestDeviceCode(context.Background(), ""); err != nil {
		t.Fatalf("RequestDeviceCode: %v", err)
	}
	form, err := url.ParseQuery(gotBody)
	if err != nil {
		t.Fatal(err)
	}
	if form.Get("referrer") != "grok-build" {
		t.Errorf("referrer: got %q want grok-build; body=%q", form.Get("referrer"), gotBody)
	}
	if form.Get("client_id") != cfg.ClientID {
		t.Errorf("client_id: got %q want %q", form.Get("client_id"), cfg.ClientID)
	}
	if form.Get("scope") != strings.Join(cfg.Scopes, " ") {
		t.Errorf("scope: got %q want %q", form.Get("scope"), strings.Join(cfg.Scopes, " "))
	}
}

func TestRequestDeviceCodeNilExtraDeviceParams(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"device_code":"dc","user_code":"UC","verification_uri":"https://example.com","expires_in":600,"interval":5}`))
	}))
	defer srv.Close()

	// github/qwen-style configs leave ExtraDeviceParams nil.
	cfg := ProviderConfig{
		ClientID:      "github-client",
		DeviceCodeURL: srv.URL,
		Scopes:        []string{"read:user"},
	}
	if _, err := cfg.RequestDeviceCode(context.Background(), ""); err != nil {
		t.Fatalf("RequestDeviceCode: %v", err)
	}
	form, err := url.ParseQuery(gotBody)
	if err != nil {
		t.Fatal(err)
	}
	if form.Get("client_id") != "github-client" {
		t.Errorf("client_id: got %q", form.Get("client_id"))
	}
	if _, ok := form["referrer"]; ok {
		t.Errorf("unexpected referrer key in form: %v", form)
	}
	if len(form) != 2 {
		t.Errorf("expected only client_id+scope, got %v", form)
	}
}

func TestRequestDeviceCodePKCEStillPresent(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"device_code":"dc","user_code":"UC","verification_uri":"https://example.com","expires_in":600,"interval":5}`))
	}))
	defer srv.Close()

	cfg := ProviderConfig{
		ClientID:      "qwen-client",
		DeviceCodeURL: srv.URL,
		Scopes:        []string{"openid"},
		// ExtraDeviceParams nil — must not break DeviceCodePKCE fields.
	}
	if _, err := cfg.RequestDeviceCode(context.Background(), "challenge-xyz"); err != nil {
		t.Fatalf("RequestDeviceCode: %v", err)
	}
	form, err := url.ParseQuery(gotBody)
	if err != nil {
		t.Fatal(err)
	}
	if form.Get("code_challenge") != "challenge-xyz" {
		t.Errorf("code_challenge: got %q", form.Get("code_challenge"))
	}
	if form.Get("code_challenge_method") != "S256" {
		t.Errorf("code_challenge_method: got %q", form.Get("code_challenge_method"))
	}
	if _, ok := form["referrer"]; ok {
		t.Errorf("unexpected referrer with nil ExtraDeviceParams: %v", form)
	}
}
