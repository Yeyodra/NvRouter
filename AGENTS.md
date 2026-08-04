# KeiRouter Agent Instructions

## Provider work

Before adding or changing a provider, read [`ADDING_PROVIDER.md`](ADDING_PROVIDER.md) completely. Treat it as the source of truth for catalog, connector, model, discovery, error-scope, quota, asset, and test requirements.

Key invariants:

- Provider catalog reads are offline: static + persisted models only. Never perform upstream discovery or scan/decrypt account pools from a catalog GET.
- Live model discovery is an explicit action (`Fetch from /models`) with a bounded timeout.
- Classify failures at the narrowest correct scope: request, exact model, account, provider, or network.
- Streaming requests are never replayed after response delivery may have started.
- Credentials come from the vault and must never be logged or persisted by connectors.
- Add focused tests, then run `cd backend && go test ./...`, `cd backend && go vet ./...`, and frontend typecheck/build when applicable.

Keep changes minimal. Reuse the generic connector for standard protocols; add a dedicated connector only for real transport/auth/request/response differences.
