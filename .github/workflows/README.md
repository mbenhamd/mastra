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
- PF-558 PR `#266` has one base-owned upstream-sync lane because its reviewed
  upstream merge necessarily updates the root manifest, Server manifest, and
  workspace policy. Admission is bound to the same-repository PR number, exact
  head/base refs, the preserved two-parent upstream merge commit, the exact
  three changed dependency-graph paths, and SHA-256 hashes for all three
  manifests plus the lockfile. Any extra patch, manifest, `.npmrc`, or pnpm-hook
  change fails before install. The lane installs with pnpm hooks and lifecycle
  scripts disabled, verifies Harness v1 and
  AgentController boundaries, and runs the owning Core, Server, Client SDK,
  React, MastraCode, Slack, Vercel, Harness, AgentController, and evented
  workflow build/test surface. The exception is intentionally not reusable by
  another PR or branch.
- `.github/workflows/papersflow-fork-pr.yml` always builds and type-checks Core,
  runs explicit affected-package checks for Okta Auth, Stagehand, Internal Core, CLI,
  Codemod, Deployer, MCP, Memory, Server, AI SDK, shared Storage Test Utils,
  PostgreSQL, and Redis, and
  executes each supported changed Vitest file in full. The stateful Core Agent
  signal suite is the sole exception: the validator maps changed lines to named
  `it()`/`test()` cases with the TypeScript AST and runs each selected case in a
  fresh Vitest process. Before execution, pinned Vitest collection must resolve
  the leaf selector to exactly one full test identity; the JSON reporter must
  then prove that same file, suite path, and leaf was the sole passing case.
  The validator exercises a unique nested identity and a duplicate-leaf
  fail-closed case after its Core prerequisite checks on every run. Duplicate
  or ambiguous leaf names fail closed. Two named PubSub failure-injection fixtures map to their
  owning regression cases; all other changes to shared setup or helpers in that
  file fail closed because they cannot be covered safely by case selection. Storage Test Utils
  changes must include a changed Vitest file in that workspace so shared
  conformance helpers cannot pass without execution. Okta Auth runs its package
  build and lint; Deployer runs
  explicit Memory, Agent Builder, Server, Hono, and Deployer builds before its
  package typecheck, lint, and changed-test coverage. CLI builds its Deployer
  chain and direct Loggers dependency, then runs package typecheck, lint, and
  changed-test coverage. Codemod runs package typecheck, build, lint, and
  changed-test coverage. The PF-1880 real-process integration tests are owned by
  those targets; other integration-test filename variants still fail closed.
  This avoids unrelated package failures in broad Turbo dependency closures.
  Server changes run
  the package build, lint, Core-import boundary check, permission freshness
  check, both `SERVER_ROUTES` generators, and changed-test coverage. Route
  contract changes additionally typecheck the focused Client SDK route
  consumers and run the CLI descriptor suite. Route-source changes also run
  the schema-consistency and API-manifest suites even when no test was edited.
  The
  generated Client SDK route types, CLI API metadata, and Core permission
  interfaces are mapped narrowly to the Server validation lane. Route
  regeneration fails unless both route artifacts are tracked and unchanged
  from the PR head. The Server permission check owns the generated RBAC
  interface. Server package manifest changes fail closed before package-owned
  commands run. This allows route and permission PRs to commit canonical output
  without granting general validation coverage to the Client SDK, CLI, or Core
  workspaces. MCP runs its package typecheck, build, lint, and changed-test
  coverage. Nested
  fixture manifests are not treated as workspace boundaries. The validator discovers
  workspace ownership from the nearest non-fixture `package.json` and
  fails closed when a changed workspace has no owned fork-safe validation
  target. A lockfile change is compared from the PR merge base and accepted only
  when every changed importer also changes its own manifest and content outside
  the importer sections remains unchanged; root manifests, workspace
  configuration, unrelated importers, non-importer dependency-graph churn, and
  unowned lockfile-only changes still fail closed. Supported package checks use
  path-bound filters that fail when no workspace matches. The exact PF-1880 CommonJS
  fixer and Vitest configuration files receive scripts TypeScript validation
  plus changed-test execution. Other non-workspace changes must match the
  explicit CI-rollout or changeset-metadata allowlist and fail closed otherwise.
- Docs Playwright changes are covered by the fork-enabled Docs E2E workflow.
  The deterministic, in-process Core Harness real-agent E2E suite is explicitly
  allowlisted and runs through Vitest because it uses a mock language model and
  `InMemoryStore`. The exact Server favorites integration suite is also
  allowlisted because it exercises route handlers exclusively against
  `InMemoryStore`. Both exact-path exceptions are content-conditioned: edits
  that add Playwright, environment credentials, external provider or storage
  packages (including scoped OpenAI SDKs), or direct network/process
  primitives fail closed before the path exception is considered. Newly added
  external imports in the exact exceptions are limited to their reviewed
  in-process Core, Vitest, Zod, and safe Node surfaces. A TypeScript parser and
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
- Stagehand changes build and lint the package, then run its browser-independent
  Vitest suite. The `profile-lifecycle` test remains fail-closed because it
  requires a configured Chrome executable; changing, deleting, or renaming that test requires a
  dedicated browser-capable validation lane. Maintainers can exercise the
  workspace classification and command selection locally with
  `.github/scripts/run-papersflow-fork-pr-validation.bash --self-test-stagehand`.
- Actual `mastracode/**` changes build the GitHub Signals test prerequisite,
  run changed-file ESLint, and execute the PF-1878 login-dialog, event-dispatch,
  and notification Vitest files in full. Each of those three production files
  must remain a regular file and change with its matching regular test; every
  other MastraCode production JS/TS source, unit test, and nested E2E test fails closed until the targeted
  validator owns its workspace build prerequisites and test configuration. The
  canonical MastraCode build and E2E workflow stays available by manual dispatch
  in the fork; the targeted validator does not report those broad suites as PR
  coverage while current `main` has unrelated MastraCode and dependency-build
  baseline errors.
- The workflow has `contents: read`, does not receive repository secrets, and
  checks out with persisted credentials disabled. Install classification and
  validation scripts are loaded from the PR's trusted base commit. The first
  same-repository rollout may bootstrap only the new policy fixtures from its
  exact CI branch; external PRs may not.
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
.github/scripts/run-papersflow-fork-pr-validation.bash --self-test-pf558-upstream-sync
```

The fixtures use an isolated temporary Git repository and mocked package
commands. They prove Server permission and route-generation selection, map
each generated artifact back to Server even when it is the only changed file,
reject stale or deleted generated output, reject Server manifest changes before
package commands run, exercise route-contract and generated-consumer checks,
run route consistency coverage and the exact fork-safe favorites integration
test, reject exact and ordinary Server tests when their changed or newly
reachable local dependencies gain unsafe runtime requirements, distinguish real
imports from comments, and keep other integration-named tests fail-closed.

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
