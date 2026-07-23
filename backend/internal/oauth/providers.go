package oauth

import "time"

// ProviderConfig holds the OAuth endpoints and client identity for one
// provider. It covers the
// providers KeiRouter can drive with the standard authorization-code and
// device-code grants.
type ProviderConfig struct {
	// Provider is the catalog provider id this config authenticates.
	Provider string
	Flow     FlowType

	ClientID     string
	ClientSecret string // empty for public PKCE clients

	AuthorizeURL  string // authorization_code(_pkce)
	DeviceCodeURL string // device_code
	TokenURL      string
	RefreshURL    string // defaults to TokenURL when empty

	// Scopes is space-joined into the request.
	Scopes []string
	// PKCEVerifierBytes overrides the default 32-byte verifier entropy.
	PKCEVerifierBytes int
	// UsesBasicAuth sends client_id:client_secret as HTTP Basic on token calls.
	UsesBasicAuth bool
	// ExtraAuthParams are appended to the authorize URL (provider quirks).
	ExtraAuthParams map[string]string
	// ExtraAuthParamOrder preserves provider-specific parameter order when the
	// authorize URL needs CLI-compatible percent encoding.
	ExtraAuthParamOrder []string
	// EncodeAuthSpacesAsPercent mirrors CLIs that build authorize URLs with
	// encodeURIComponent, where spaces become %20 rather than +.
	EncodeAuthSpacesAsPercent bool
	// SkipStandardAuthParams omits response_type, client_id, scope from the
	// authorize URL (provider quirks, e.g. Cline only wants client_type +
	// callback_url + redirect_uri).
	SkipStandardAuthParams bool
	// NonceBytes adds a random hex nonce parameter to the authorize URL.
	NonceBytes int
	// TokenContentType is "form" (x-www-form-urlencoded, default) or "json".
	TokenContentType string
	// ExtraTokenParams are added to the token exchange request body
	// (provider quirks, e.g. Cline requires client_type=extension).
	ExtraTokenParams map[string]string
	// ExtraDeviceParams are added to the device-code request body
	// (provider quirks, e.g. Grok CLI requires referrer=grok-build).
	ExtraDeviceParams map[string]string
	// RefreshLead is how long before TokenExpiresAt we treat a token as
	// needing refresh. Zero means use the global refreshSkew (60s).
	// Long-lived tokens (e.g. grok-cli ~6h from expires_in) should set a
	// larger lead — farm EXPIRY_BUFFER is 10*time.Minute — so keepalive and
	// EnsureFresh renew before idle gaps miss the window. Do not hardcode
	// token lifetime here; always trust expires_in from the token endpoint.
	RefreshLead time.Duration
	// DeviceCodePKCE enables PKCE for device-code flows (e.g. Qwen requires
	// code_challenge + code_challenge_method on the device-code request).
	DeviceCodePKCE bool
	// UserAgent overrides the default Go User-Agent header on HTTP requests.
	// Some providers (Qwen) sit behind a WAF that blocks Go-http-client/1.1.
	UserAgent string
	// ClientDeviceCode tells the gateway to let the browser (frontend) make
	// the device-code HTTP request instead of the Go backend.  This is
	// required when the provider's WAF uses TLS fingerprinting to block
	// non-browser clients (e.g. Qwen / Alibaba Cloud WAF).
	ClientDeviceCode bool

	// CallbackPath and FixedLoopbackPort mirror CLI OAuth loopback callbacks.
	// Providers with FixedLoopbackPort set ignore the dashboard-provided
	// redirect host and use http://LoopbackHost:FixedLoopbackPort/CallbackPath.
	CallbackPath      string
	FixedLoopbackPort int
	LoopbackHost      string

	// UserInfoURL is the endpoint called after token exchange to retrieve the
	// connected user's email and display name.  When empty, no profile fetch
	// is attempted and the account label falls back to the provider name.
	UserInfoURL string
}

// refreshURL returns the configured refresh URL, defaulting to TokenURL.
func (c ProviderConfig) refreshURL() string {
	if c.RefreshURL != "" {
		return c.RefreshURL
	}
	return c.TokenURL
}

// refreshLead returns how long before expiry a token should be refreshed.
// When RefreshLead > 0 it is used (e.g. grok-cli: 10*time.Minute); otherwise
// the global refreshSkew (60s) applies.
func (c ProviderConfig) refreshLead() time.Duration {
	if c.RefreshLead > 0 {
		return c.RefreshLead
	}
	return refreshSkew
}

// providerRefreshLead resolves the refresh lead for a provider id. Unknown
// providers fall back to the global refreshSkew.
func providerRefreshLead(provider string) time.Duration {
	if cfg, ok := ConfigFor(provider); ok {
		return cfg.refreshLead()
	}
	return refreshSkew
}

// configs maps provider id -> OAuth config. Client ids/secrets are the public
// values published by the upstream CLIs; they are not
// secrets in the confidential sense.
var configs = map[string]ProviderConfig{
	"claude": {
		Provider:     "claude",
		Flow:         FlowAuthCodePKCE,
		ClientID:     "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
		AuthorizeURL: "https://claude.ai/oauth/authorize",
		TokenURL:     "https://api.anthropic.com/v1/oauth/token",
		Scopes:       []string{"org:create_api_key", "user:profile", "user:inference"},
		// Claude's token endpoint expects a JSON body.
		TokenContentType: "json",
		ExtraAuthParams:  map[string]string{"code": "true"},
		UserInfoURL:      "https://api.anthropic.com/v1/me",
	},
	"codex": {
		Provider:                  "codex",
		Flow:                      FlowAuthCodePKCE,
		ClientID:                  "app_EMoamEEZ73f0CkXaXp7hrann",
		AuthorizeURL:              "https://auth.openai.com/oauth/authorize",
		TokenURL:                  "https://auth.openai.com/oauth/token",
		Scopes:                    []string{"openid", "profile", "email", "offline_access"},
		// prompt=login forces a fresh Auth0 session per flow. Without it, a
		// second Codex OAuth on the same browser reuses the first session and
		// Auth0 invalidates the earlier account's refresh-token family as a
		// "session takeover", breaking multi-account setups.
		ExtraAuthParams:           map[string]string{"id_token_add_organizations": "true", "codex_cli_simplified_flow": "true", "originator": "codex_cli_rs", "prompt": "login"},
		ExtraAuthParamOrder:       []string{"id_token_add_organizations", "codex_cli_simplified_flow", "originator", "prompt"},
		EncodeAuthSpacesAsPercent: true,
		CallbackPath:              "/auth/callback",
		FixedLoopbackPort:         1455,
		LoopbackHost:              "localhost",
		UserInfoURL:               "https://auth.openai.com/oauth/userinfo",
	},
	"gemini-cli": {
		Provider:        "gemini-cli",
		Flow:            FlowAuthCode,
		ClientID:        "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com",
		ClientSecret:    "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",
		AuthorizeURL:    "https://accounts.google.com/o/oauth2/v2/auth",
		TokenURL:        "https://oauth2.googleapis.com/token",
		Scopes:          []string{"https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile"},
		ExtraAuthParams: map[string]string{"access_type": "offline", "prompt": "consent"},
		UserInfoURL:     "https://www.googleapis.com/oauth2/v1/userinfo",
	},
	"antigravity": {
		Provider:        "antigravity",
		Flow:            FlowAuthCode,
		ClientID:        "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com",
		ClientSecret:    "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",
		AuthorizeURL:    "https://accounts.google.com/o/oauth2/v2/auth",
		TokenURL:        "https://oauth2.googleapis.com/token",
		Scopes:          []string{"https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/cclog", "https://www.googleapis.com/auth/experimentsandconfigs"},
		ExtraAuthParams: map[string]string{"access_type": "offline", "prompt": "consent"},
		UserInfoURL:     "https://www.googleapis.com/oauth2/v1/userinfo",
	},
	"xai": {
		Provider:                  "xai",
		Flow:                      FlowAuthCodePKCE,
		ClientID:                  "b1a00492-073a-47ea-816f-4c329264a828",
		AuthorizeURL:              "https://auth.x.ai/oauth2/authorize",
		TokenURL:                  "https://auth.x.ai/oauth2/token",
		Scopes:                    []string{"openid", "profile", "email", "offline_access", "grok-cli:access", "api:access"},
		PKCEVerifierBytes:         96,
		ExtraAuthParams:           map[string]string{"plan": "generic", "referrer": "cli-proxy-api"},
		ExtraAuthParamOrder:       []string{"plan", "referrer"},
		EncodeAuthSpacesAsPercent: true,
		NonceBytes:                16,
		CallbackPath:              "/callback",
		FixedLoopbackPort:         56121,
		LoopbackHost:              "127.0.0.1",
		UserInfoURL:               "https://auth.x.ai/oauth2/userinfo",
	},
	"github": {
		Provider:      "github",
		Flow:          FlowDeviceCode,
		ClientID:      "Iv1.b507a08c87ecfe98",
		DeviceCodeURL: "https://github.com/login/device/code",
		TokenURL:      "https://github.com/login/oauth/access_token",
		Scopes:        []string{"read:user"},
		UserInfoURL:   "https://api.github.com/user",
	},
	"qwen": {
		Provider:         "qwen",
		Flow:             FlowDeviceCode,
		ClientID:         "f0304373b74a44d2b584a3fb70ca9e56",
		DeviceCodeURL:    "https://chat.qwen.ai/api/v1/oauth2/device/code",
		TokenURL:         "https://chat.qwen.ai/api/v1/oauth2/token",
		Scopes:           []string{"openid", "profile", "email", "model.completion"},
		UserInfoURL:      "https://chat.qwen.ai/api/v1/oauth2/userinfo",
		DeviceCodePKCE:   true,
		UserAgent:        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
		ClientDeviceCode: true,
	},
	// grok-cli is Grok Build device-code OAuth (distinct from xai PKCE on api.x.ai).
	// Same ClientID as xai; device endpoint + referrer=grok-build + conversation scopes.
	"grok-cli": {
		Provider:      "grok-cli",
		Flow:          FlowDeviceCode,
		ClientID:      "b1a00492-073a-47ea-816f-4c329264a828",
		DeviceCodeURL: "https://auth.x.ai/oauth2/device/code",
		TokenURL:      "https://auth.x.ai/oauth2/token",
		Scopes: []string{
			"openid", "profile", "email", "offline_access",
			"grok-cli:access", "api:access",
			"conversations:read", "conversations:write",
		},
		ExtraDeviceParams: map[string]string{"referrer": "grok-build"},
		UserAgent:         "grok-shell/0.2.99 (linux; x86_64)",
		RefreshLead:       10 * time.Minute,
	},
	"cline": {
		Provider:               "cline",
		Flow:                   FlowAuthCode,
		AuthorizeURL:           "https://api.cline.bot/api/v1/auth/authorize",
		TokenURL:               "https://api.cline.bot/api/v1/auth/token",
		RefreshURL:             "https://api.cline.bot/api/v1/auth/refresh",
		UserInfoURL:            "https://api.cline.bot/api/v1/auth/userinfo",
		TokenContentType:       "json",
		SkipStandardAuthParams: true,
		ExtraAuthParams: map[string]string{
			"client_type": "extension",
		},
		ExtraTokenParams: map[string]string{
			"client_type": "extension",
		},
	},
	"iflow": {
		Provider:     "iflow",
		Flow:         FlowAuthCode,
		ClientID:     "10009311001",
		ClientSecret: "4Z3YjXycVsQvyGF1etiNlIBB4RsqSDtW",
		AuthorizeURL: "https://iflow.cn/oauth",
		TokenURL:     "https://iflow.cn/oauth/token",
		UserInfoURL:  "https://iflow.cn/api/oauth/getUserInfo",
		ExtraAuthParams: map[string]string{
			"loginMethod": "phone",
			"type":        "phone",
		},
	},
	"kimi-coding": {
		Provider:      "kimi-coding",
		Flow:          FlowDeviceCode,
		ClientID:      "17e5f671-d194-4dfb-9706-5516cb48c098",
		DeviceCodeURL: "https://auth.kimi.com/api/oauth/device_authorization",
		TokenURL:      "https://auth.kimi.com/api/oauth/token",
	},
	// ClinePass shares Cline's subscription auth surface (same api.cline.bot
	// endpoints and non-standard authorize params), so its flow mirrors cline.
	"clinepass": {
		Provider:               "clinepass",
		Flow:                   FlowAuthCode,
		AuthorizeURL:           "https://api.cline.bot/api/v1/auth/authorize",
		TokenURL:               "https://api.cline.bot/api/v1/auth/token",
		RefreshURL:             "https://api.cline.bot/api/v1/auth/refresh",
		UserInfoURL:            "https://api.cline.bot/api/v1/auth/userinfo",
		TokenContentType:       "json",
		SkipStandardAuthParams: true,
		ExtraAuthParams: map[string]string{
			"client_type": "extension",
		},
		ExtraTokenParams: map[string]string{
			"client_type": "extension",
		},
	},
	// Grok CLI signs in with an xAI account via device code and spends Grok
	// Build subscription credits through cli-chat-proxy.grok.com. It reuses
	// the public xAI OAuth client id and the auth.x.ai token endpoints.
	"grok-cli": {
		Provider:      "grok-cli",
		Flow:          FlowDeviceCode,
		ClientID:      "b1a00492-073a-47ea-816f-4c329264a828",
		DeviceCodeURL: "https://auth.x.ai/oauth2/device/code",
		TokenURL:      "https://auth.x.ai/oauth2/token",
		RefreshURL:    "https://auth.x.ai/oauth2/token",
		Scopes: []string{
			"openid", "profile", "email", "offline_access",
			"grok-cli:access", "api:access",
			"conversations:read", "conversations:write",
		},
		UserInfoURL: "https://auth.x.ai/oauth2/userinfo",
	},
}

// ConfigFor returns the OAuth config for a provider id.
func ConfigFor(provider string) (ProviderConfig, bool) {
	c, ok := configs[provider]
	return c, ok
}

// SupportedProviders lists provider ids with an OAuth config, for discovery.
func SupportedProviders() []string {
	out := make([]string, 0, len(configs))
	for id := range configs {
		out = append(out, id)
	}
	return out
}
