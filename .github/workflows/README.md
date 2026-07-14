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
  the package build, lint, Core-import boundary check, and changed-test coverage.
  The validator discovers
  workspace ownership from the nearest non-fixture `package.json` and
  fails closed when a changed workspace has no owned fork-safe validation
  target. Root dependency graph changes also fail closed until broad workspace
  validation is available. Non-workspace changes must match the explicit
  CI-rollout or changeset-metadata allowlist; other root paths fail closed.
- Docs Playwright changes are covered by the fork-enabled Docs E2E workflow.
  Other Playwright files, `e2e-tests/**`, nested integration-test packages,
  integration-test filename variants, explicit provider E2E files, and
  PostgreSQL pooler/performance suites fail closed until a dedicated fork-safe
  workflow provides their required setup.
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
