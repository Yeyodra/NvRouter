# Adding a Provider to KeiRouter

This is the source of truth for coding agents and contributors adding or extending a provider. Explore the current code before editing: this guide defines the boundaries and completion criteria, while existing providers show the latest concrete APIs.

## 1. Decide whether code needs to exist

Use the first rung that works:

1. **Existing built-in provider only needs new models or metadata** → edit `backend/internal/connectors/models.go` or `catalog.go`.
2. **Standard OpenAI Chat Completions** → add a `ProviderSpec`; `NewOpenAICompatible` is registered automatically.
3. **Standard Anthropic Messages** → add a `ProviderSpec`; `NewAnthropic` is registered automatically.
4. **Existing connector already has a small provider-specific hook** → extend that hook only when the protocol is still standard.
5. **Dedicated connector** → use only when auth, URLs, request shaping, response parsing, streaming, model discovery, or quota behavior genuinely differs.
6. **New dialect** → last resort for a proprietary wire format; add canonical request/response translation and registry wiring.

Do not create an interface with one implementation, a factory for one provider, or a dependency for behavior covered by the standard library/current helpers.

## 2. File map

| Concern | Source of truth |
|---|---|
| Provider metadata, alias, auth modes, endpoint, service kinds | `backend/internal/connectors/catalog.go` |
| Curated models and kinds | `backend/internal/connectors/models.go` |
| Connector selection and provider overrides | `backend/internal/connectors/registry.go` |
| Connector contracts and optional capabilities | `backend/internal/core/connector.go` |
| Canonical chat types | `backend/internal/core/request.go`, `message.go`, `response.go` |
| Generic OpenAI transport | `backend/internal/connectors/openai_compatible.go` |
| Generic Anthropic transport | `backend/internal/connectors/anthropic.go` |
| HTTP, SSE, status classification, proxy helpers | `backend/internal/connectors/httpclient.go` |
| Failure kind and scope | `backend/internal/core/errors.go` |
| Account selection, cooldown, fallback | `backend/internal/dispatch/dispatch.go` |
| Live model discovery and quota interfaces | `backend/internal/connectors/models.go` |
| Account metadata/vault conversion | `backend/internal/vault/`, `backend/internal/gateway/admin.go` |
| LLM provider UI and account management | `frontend/src/pages/ProviderDetail.tsx` |
| Media provider UI and modality testers | `frontend/src/pages/MediaProviderDetail.tsx` |
| Provider icon | `frontend/public/providers/<provider-id>.png` |

## 3. ProviderSpec

Add a stable lowercase ID and an unambiguous alias:

```go
{
    ID: "example", DisplayName: "Example AI", Alias: "ex",
    Dialect: core.DialectOpenAI,
    BaseURL: "https://api.example.com/v1",
    AuthKind: "api_key",
    ServiceKinds: llm(),
    Color: "#123456", Website: "https://example.com",
    APIKeyURL: "https://example.com/keys",
}
```

Rules:

- `BaseURL` is the API root expected by the connector, not a dashboard URL.
- Use `AuthModes` when more than the default `AuthKind` is supported.
- Enumerate media kinds explicitly. Empty kinds are conservatively treated as LLM-only, but new providers should be explicit.
- `SkipValidation` is for a proven WAF/server-side probe problem, not a shortcut around implementing validation.
- Add `Regions` only for genuinely different endpoint clusters.
- Add pricing only when sourced; zero means unknown/free.

## 4. Models are typed catalog data

Add curated models to `providerModels`:

```go
"example": {
    m("example-chat", "Example Chat"),
    emb("example-embed", "Example Embed", 1536),
    k("example-image", "Example Image", core.ServiceImage),
},
```

Preserve exact upstream IDs. Do not strip vendor prefixes or change case unless the upstream contract requires it.

### Catalog boundary — critical

`GET /api/providers/{id}/models` must be deterministic and offline. It returns:

```text
static catalog + persisted custom/imported models
```

It must **not**:

- call the upstream;
- iterate/decrypt account pools;
- depend on credential health;
- replace static models with an empty live response.

Large pools make serial discovery appear as intermittently missing models. Keep upstream discovery behind the explicit `adminImportModels` / `ListModels` action with a bounded timeout, then persist the result. The current dashboard exposes the Fetch control for custom providers; built-in sources are API-accessible until a built-in UI control is added.

## 5. Generic vs dedicated connector

### Generic connector

Use the generic connector when the provider accepts the canonical protocol with ordinary Bearer/API-key auth and a conventional path. Small header differences may fit an existing provider hook, but do not turn the generic connector into an unbounded switch statement.

### Dedicated connector

Create `backend/internal/connectors/<provider>.go` when one or more apply:

- workspace/project/region must be resolved before requests;
- nonstandard auth headers or token exchange;
- nonstandard endpoint paths;
- provider-specific body compatibility rules;
- proprietary response or SSE framing;
- safe pre-response retries;
- nonstandard validation/model/quota endpoints;
- provider errors require narrower routing scope.

A dedicated chat connector implements:

```go
type Connector interface {
    ID() string
    Dialect() core.Dialect
    Chat(context.Context, *core.ChatRequest, core.Credentials) (*core.ChatResponse, error)
    Stream(context.Context, *core.ChatRequest, core.Credentials, core.StreamConfig) (<-chan core.StreamChunk, error)
}
```

Prefer reusing codecs and HTTP/SSE helpers over reimplementing translation/parsing. Implement `DirectStreamable` only when raw same-dialect SSE is valid and no downstream normalization is required.

Connectors are shared across concurrent requests: caches and mutable state require synchronization and bounds. Never retain credentials beyond the minimum required behavior; never log them.

## 6. Credentials and account metadata

Credentials are decrypted by the vault into `core.Credentials`:

- `APIKey` / `AccessToken`: secret material;
- `BaseURL`: optional endpoint override;
- `Extra`: non-secret provider metadata;
- proxy/relay fields: resolved per account by dispatch.

If the provider needs account metadata:

1. accept it in the admin account request;
2. normalize it in `providerAccountMetadata`/foreign import;
3. store it as metadata so it reaches `Credentials.Extra`;
4. validate trust-boundary input;
5. add round-trip tests.

Preserve large identifier precision. Generic JSON numbers decoded through `float64` can become scientific notation. Use strings, integer types, `json.Number`, or `json.RawMessage` when lexical identity matters.

Proxy support is not automatic outside inference dispatch. For validation, model discovery, quota, and helper lookups, explicitly propagate the account's resolved proxy context when the feature promises proxy support. Current generic discovery/validation paths may use direct egress; do not claim proxy compatibility without a focused test.

## 7. Request, streaming, and retry invariants

- The pipeline gives each attempt its own request clone, so connector-local shaping is allowed. Do not retain or mutate shared request state outside the call, and clone nested data before launching concurrent work or reusing it across retries.
- Keep multi-turn messages, tools, tool choice, images, and reasoning semantics unless the upstream explicitly rejects them.
- Translate unsupported fields in one provider-specific location and leave focused tests.
- Respect context cancellation and configured timeouts.
- SSE must emit canonical text/thinking/tool/usage/finish chunks and terminate cleanly.
- Preserve fragmented tool-call arguments.
- Retry only replay-safe requests. For streaming, retry only before response headers/body delivery begins; never replay after a chunk may have reached the client.
- Async create-job media endpoints must not be blindly retried because duplicate jobs cost money.

Untrusted remote media fetches need scheme/redirect validation, timeout, MIME and size limits, and dial-time DNS/IP enforcement. Do not inherit a private-base-URL exception for arbitrary user-supplied media URLs.

## 8. Errors: classify the narrowest resource

Return `*core.ProviderError` with accurate:

- `Kind` — auth, rate limit, quota exhausted, model unavailable, bad request, upstream, timeout;
- `Scope` — request, exact model, account, provider, or network;
- provider/model/status/retry hints.

Examples:

| Failure | Scope |
|---|---|
| malformed client payload | request |
| model ID unavailable, account otherwise valid | model |
| rejected/expired credential | account |
| temporary upstream outage | provider |
| transport/DNS/proxy failure | network |

Do not disable or park an account for a model-only failure. A success on one model must not clear a sibling model's lock. Verify behavior through dispatcher storage/selection tests, not only connector error fields.

Keep 5xx/provider outages out of permanent account state. Preserve upstream `Retry-After` where applicable.

## 9. Optional capabilities

### Validation

Implement `core.Validator` when credentials can be safely and cheaply verified. Prefer a read-only account/models endpoint over a billable chat completion. Distinguish invalid auth from transient upstream failure.

### Live model discovery

Implement `LiveModelSource` only when useful and register it in `DefaultRegistry`. Fetching remains an explicit admin action. Filter/normalize unstable upstream catalogs before persistence. Built-in providers currently need an API call for this action because the dashboard Fetch control is custom-provider-only.

### Quota

Implement `QuotaSource` for provider usage endpoints and call `RegisterQuotaSource` in `DefaultRegistry`; implementing the interface alone is not discoverable. During parsing, distinguish explicit zero from a missing field before converting to `QuotaEntry`'s integer fields—missing required balance data should return an error/message, while explicit zero is valid. Return coherent limit/used/remaining/reset/plan fields and keep quota errors best-effort—quota UI failure must not break inference.

### OAuth and proprietary auth

OAuth config belongs in `backend/internal/oauth/`. Token refresh must persist rotated tokens through the existing token manager. Browser/session-cookie transports usually need a dedicated connector and explicit risk notice.

## 10. Dashboard integration

At minimum add:

```text
frontend/public/providers/<provider-id>.png
```

The provider catalog automatically exposes `/providers/<id>.png`. Verify the asset locally and in the production bundle.

Use generic account forms when possible. Add provider-specific fields only when metadata is required. LLM model cards receive the global Test action automatically; media kinds need modality-specific testers.

Provider model Test runs one exact provider/model through the native pipeline. It may fall back across accounts for that provider/model, but must not silently test a different provider.

## 11. Tests — minimum completion contract

### Catalog/registry

- provider ID and alias resolve;
- registry returns a routable connector;
- curated model IDs/kinds are correct;
- unsupported credential/model variants are excluded.

### Connector

Use `httptest.Server`; do not hit the live provider in unit tests. Cover:

- exact URL, method, auth, headers, proxy propagation;
- request body compatibility and isolation of attempt-local mutations;
- unary parsing;
- SSE text, usage, finish, fragmented tool calls;
- status/error kind + scope;
- cancellation and bounded retries;
- validation/model/quota envelopes if implemented.

### Dispatcher/gateway

- fallback to another account when allowed;
- exact model locks do not block sibling models;
- account-level failures cool only that account;
- catalog GET never calls live discovery (leave a counting/hanging-source regression test);
- global model Test uses exact provider/model and same-provider pool semantics;
- account metadata survives vault/import/export round trips.

### Frontend

Run typecheck and production build. Verify provider icon, account form, stable model list, global LLM Test success/error state, and mobile layout.

## 12. Verification ladder

Run narrow checks first, then broad checks:

```bash
# Backend
export PATH=/home/ubuntu/.local/go1.26/bin:$PATH
export GOTOOLCHAIN=local
cd backend
gofmt -w <changed-go-files>
go test ./internal/connectors ./internal/dispatch ./internal/gateway
go test ./...
go vet ./...

# Frontend, sequentially on small hosts
cd ../frontend
npm ci                 # only if node_modules is absent/stale
npm run typecheck
npm run build

# Production binary
cd ..
make build-backend
git diff --check
```

Then exercise real credentials only when explicitly available:

1. unary sentinel response;
2. SSE chunks + reconstructed text + `[DONE]`;
3. global model Test result with visible content, latency, usage;
4. inspect account/model cooldown state after failures;
5. benchmark model catalog repeatedly—identical count and millisecond latency even with a large pool.

A provider is not complete when it merely compiles. It is complete when the relevant artifacts, tests, production builds, and at least one real request (when credentials exist) agree with the contract.

## Common mistakes

- Copying provider code without its workspace/header/parameter/error semantics.
- Adding a dedicated connector for a standard OpenAI endpoint.
- Polluting a generic connector with large provider-specific branches.
- Treating all 402/429 responses as account-wide exhaustion.
- Running live model discovery while rendering the model catalog.
- Replaying a stream after delivery began.
- Decoding exact numeric IDs through `float64`.
- Ignoring proxies in helper endpoints.
- Logging credentials or raw sensitive upstream bodies.
- Adding backend support but forgetting the provider icon and frontend verification.
