# GitHub Workflows

This directory contains GitHub Actions workflow files for the Mastra project.

## Canonical-only automation

Secret-bearing, publish, schedule, and upstream-maintenance jobs remain limited
to the canonical Mastra repository. These jobs use a condition that checks the
repository identity:

```yaml
if: ${{ github.repository == 'mastra-ai/mastra' }}
```

If a job already has an `if` condition, we combine them:

```yaml
if: ${{ github.repository == 'mastra-ai/mastra' && (your existing condition) }}
```

## PapersFlow fork pull requests

The `mbenhamd/mastra` fork intentionally runs a small PR validation surface:

- PR code runs only in the unprivileged `pull_request` sandbox on ephemeral
  GitHub-hosted `ubuntu-24.04` runners.
- The validator also runs for PRs targeting `ci/**` policy branches, allowing a
  stacked feature PR to prove a new trusted-base validation target before that
  policy branch reaches `main`.
- `.github/workflows/papersflow-fork-pr.yml` always builds and type-checks Core,
  runs explicit affected-package checks for Okta Auth, Internal Core, Deployer,
  MCP, Memory, Server, AI SDK, shared Storage Test Utils, PostgreSQL, and Redis, and
  executes each supported changed Vitest file in full. Storage Test Utils
  changes must include a changed Vitest file in that workspace so shared
  conformance helpers cannot pass without execution. Okta Auth runs its package
  build and lint; Deployer runs
  explicit Memory, Agent Builder, Server, and Hono prerequisite builds before
  its package typecheck, build, lint, and changed-test coverage. This avoids the
  unrelated package failures in Deployer's broad Turbo dependency closure. MCP
  runs its package typecheck, build, lint, and changed-test coverage. Nested
  fixture manifests are not treated as workspace boundaries. Server changes run
  the package build, lint, Core-import boundary check, permission freshness
  check, both `SERVER_ROUTES` generators, and changed-test coverage. The
  generated Client SDK route types, CLI API metadata, and Core permission
  interfaces are mapped narrowly to the Server validation lane. Route
  regeneration fails unless both route artifacts are tracked and unchanged
  from the PR head. Route-surface changes also run Server route consistency and
  API-manifest tests, scoped TypeScript checks over each generated module and
  its consumers, and focused Client SDK Harness and CLI descriptor tests. The
  Server permission check owns the generated RBAC interface. Server package
  manifest changes fail closed before package-owned commands run. This allows
  route and permission PRs to commit canonical output without granting general
  validation coverage to the Client SDK, CLI, or Core workspaces.
  The validator discovers
  workspace ownership from the nearest non-fixture `package.json` and
  fails closed when a changed workspace has no owned fork-safe validation
  target. Root dependency graph changes also fail closed until broad workspace
  validation is available. Non-workspace changes must match the explicit
  CI-rollout or changeset-metadata allowlist; other root paths fail closed.
- Docs Playwright changes are covered by the fork-enabled Docs E2E workflow.
  The deterministic, in-process Core Harness real-agent E2E suite is explicitly
  allowlisted and runs through Vitest because it uses a mock language model and
  `InMemoryStore`. The exact Server favorites integration suite is also
  allowlisted because it exercises route handlers exclusively against
  `InMemoryStore`. Both exact-path exceptions are content-conditioned: edits
  that add Playwright, environment credentials, external provider or storage
  packages (including scoped OpenAI SDKs), or direct network/process primitives
  fail closed before the path exception is considered. A TypeScript parser and
  module resolver follow the changed and newly reachable local runtime
  dependency graph, including `.js` specifiers that resolve to TypeScript
  source; helper-only changes re-run the owning exact test. Every changed Server
  test is screened across that incremental runtime surface before generic Vitest
  execution. Other integration-named Server tests remain fail-closed.
  Other Playwright files, `e2e-tests/**`, nested
  integration-test packages, integration-test filename variants, explicit
  provider E2E files, and PostgreSQL pooler/performance suites fail closed until
  a dedicated fork-safe workflow provides their required setup.
  Store-provider tests other than the provisioned PostgreSQL and Redis suites
  fail closed for the same reason.
- Actual `mastracode/**` changes run the MastraCode build and E2E suite in the
  targeted validator. The canonical MastraCode workflow stays available by
  manual dispatch in the fork, but Core-only fork PRs do not run its broad
  dependency build while the fork has unrelated package-build baseline errors.
- The workflow has `contents: read`, does not receive repository secrets, and
  checks out with persisted credentials disabled. Its validation script is
  loaded from the PR's trusted base commit. The first same-repository rollout
  may bootstrap that script from its head commit; external PRs may not.
- Redis and PostgreSQL are disposable job services matching the repository test
  ports and credentials, not shared runtime infrastructure.
- `pull_request_target` metadata workflows may use GitHub-hosted runners only
  when they check out trusted default-branch automation rather than PR code.
- Starsling remains available only to same-repository branches in the canonical
  repository; external pull-request heads use GitHub-hosted runners there too.

Run the validator's focused policy fixtures locally after changing its routing
or command plan:

```bash
.github/scripts/run-papersflow-fork-pr-validation.bash --self-test
```

The fixtures use an isolated temporary Git repository and mocked package
commands. They prove Server permission and route-generation selection, map
each generated artifact back to Server even when it is the only changed file,
reject stale or deleted generated output, reject Server manifest changes before
package commands run, exercise route-contract and generated-consumer checks,
run the exact fork-safe favorites integration test, reject exact and ordinary
Server tests when their changed or newly reachable local dependencies gain
unsafe runtime requirements, distinguish real imports from comments, and keep
other integration-named tests fail-closed.

Do not register a self-hosted runner for public PR code. Keep canonical
release, secret, cloud, and scheduled workflows gated to `mastra-ai/mastra`
unless a separately reviewed fork policy explicitly owns them.

## Benefits

- Prevents unnecessary workflow runs on forks
- Reduces notifications for fork owners
- Saves GitHub Actions minutes
- Keeps untrusted PR execution off persistent infrastructure
- Gives the PapersFlow fork reproducible build, type-check, and regression
  evidence before internal package adoption
