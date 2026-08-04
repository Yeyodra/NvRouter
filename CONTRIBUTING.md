# Contributing to KeiRouter

Thanks for your interest in contributing! This document covers everything you need to get started.

## Prerequisites

- **Go 1.26+**
- **Node.js 20+** and npm
- **Git**
- **curl** (used by `make dev` readiness checks)

## Development setup

```bash
# Clone the repo
git clone https://github.com/mydisha/keirouter.git
cd keirouter

# Install dependencies
make install

# Run backend + frontend together (Vite hot reload; restart for Go changes)
make dev
```

This starts the backend on `:20180` and the dashboard on `:5180`.

### Useful commands

| Command | What it does |
|---|---|
| `make dev` | Run backend + frontend concurrently |
| `make backend` | Run only the Go backend |
| `make frontend` | Run only the Vite dev server |
| `make build` | Build production binary + frontend assets |
| `make test` | Run the backend test suite |
| `make vet` | Run Go static analysis |
| `make bootstrap` | Create an initial API key |

## Making changes

1. **Fork** the repo and create a branch from `main`:
   ```bash
   git checkout -b feat/my-feature
   ```

2. **Make your changes.** Keep commits focused and well-described.

3. **Run checks before pushing:**
   ```bash
   make test          # Go tests
   make vet           # Go static analysis
   cd frontend && npm run typecheck && npm run build
   ```

4. **Open a Pull Request** against `main`. Fill out the PR template.

## Adding a provider

Read [`ADDING_PROVIDER.md`](ADDING_PROVIDER.md) before changing provider code. It documents the generic-vs-dedicated connector decision, catalog and live-discovery boundaries, credential metadata, error scopes, streaming/retry invariants, UI assets, and the required test/verification ladder. Coding agents should also follow the root [`AGENTS.md`](AGENTS.md).

## Code style

- **Go:** Follow standard Go conventions. Run `go vet` and `gofmt`.
- **TypeScript:** The project uses TypeScript strict mode. Run `npm run typecheck` and `npm run build` in the `frontend/` directory.
- **Commits:** Use concise, descriptive messages. Prefix with the area of change when helpful (e.g. `gateway: fix streaming chunk encoding`).

## Project structure

```
backend/
  cmd/keirouter/     entrypoint
  internal/          all Go packages (see README architecture section)
frontend/
  src/               React + TypeScript dashboard
deploy/              Dockerfile and deployment notes
compose*.yaml        Docker Compose variants
```

## Reporting bugs

Open a [GitHub Issue](https://github.com/mydisha/keirouter/issues/new?template=bug_report.md) with steps to reproduce, expected behavior, and your environment.

## Suggesting features

Open a [GitHub Issue](https://github.com/mydisha/keirouter/issues/new?template=feature_request.md) describing the problem you want solved and your proposed approach.

## Security

See [SECURITY.md](SECURITY.md) for reporting vulnerabilities.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
