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
  upstream merge necessarily updates the root manifest, Server manifest,
  workspace policy, and upstream's pinned `pi-tui` patch. Admission is bound to
  the same-repository PR number, exact head/base refs, the preserved two-parent
  upstream merge commit, the exact four changed dependency-graph paths, and
  SHA-256 hashes for all three manifests, the lockfile, and that patch. Any
  additional patch, manifest, `.npmrc`, or pnpm-hook change fails before
  install. The lane installs with pnpm hooks and lifecycle scripts disabled,
  verifies Harness v1 and
  AgentController boundaries, and runs the owning Core, Server, Client SDK,
  React, MastraCode, Slack, Vercel, Harness, AgentController, and evented
  workflow build/test surface. The exception is intentionally not reusable by
  another PR or branch.
- PF-2009 PR `#277` has a second base-owned upstream-sync lane for the next
  reviewed official-main merge. Admission is bound to its exact repository,
  PR number, branch, base, both preserved two-parent merge commits, the two
  changed root dependency manifests, SHA-256 hashes for the root manifest,
  Server manifest, workspace policy, and lockfile, plus one digest covering
  every tracked package manifest, pnpm lockfile, pnpm hook, workspace file, and
  patch. The lane reuses the same explicit Harness, AgentController, workflow,
  Server, SDK, MastraCode, Slack, and Vercel validation surface without making
  the PF-558 exception reusable. Exact same-repository policy PR `#278`, its
  branch, and its `main` base may bootstrap the new validator once; every later
  PR loads it from protected base.
- PF-2045 has a topology- and tree-bound upstream-sync lane for the next
  official-main merge because its merge commit cannot be known before the
  policy lands. Admission requires the exact same-repository Linear branch and
  `main` base, and requires the checked PR head itself to have exactly two
  parents: the current protected-base tip first and reviewed upstream commit
  `4fb4d881bc107acee13890ad4d78661016c510ed` second. The trusted policy runs
  Git's merge machinery over those parents, requires exactly six rehearsed
  content conflicts, 18 base/ours/theirs stage entries, and 26 conflict
  regions, substitutes the six reviewed regular-file conflict resolutions,
  and applies two exact PF-2053 truthfulness overlays for the approval replay
  E2E and changeset. The final frozen rehearsal against base
  `711010569312141e3792be3e864ce20f010d433b` reconstructs tree
  `61e80433d05f7bf97bd51950ebab786c0a829a29`; production admission derives the
  expected tree from the current protected-base tip plus the same eight pinned
  blobs. The reviewed files are the AI SDK approval unit test, replay E2E, chat
  route, approval changeset, Core Agent stream fallback, Server agents,
  lockfile, and workspace policy.
  Wrong parents, extra parents, a non-merge head, a forged same-parent tree,
  changed conflict paths, changed resolutions or overlays, or different PR
  metadata fail before install. The lane installs the complete reconciled
  workspace with pnpm hooks and lifecycle scripts disabled, runs the shared
  Harness, AgentController, evented-workflow, Server, SDK, and MastraCode plan,
  then validates every runtime surface across the 313 incoming paths from the
  merge base to official upstream and the 312 paths in the reconciled
  first-parent merge result. The counts differ because the reviewed
  reconciliation preserves or refines fork behavior rather than accepting
  every upstream path byte-for-byte. The additional plan includes Core replay
  suites, structured-output fallback, goal, model-capability registry,
  supervisor delegation-budget enforcement, request-context-schema inference
  for dynamic skills, AI SDK A2A transformation, PF-2053
  trailing-message approval extraction and replay, the native ACP
  `createClient`/process-lifecycle contract, the full MCP client contract, the
  deterministic MCP registry list contract, Server agent authorization, docs,
  and a deterministic Memory transform/recording compatibility check. The
  nested Memory integration package's broad native suite is deliberately not
  presented as clean-runner evidence because it is outside the frozen root
  workspace and warms FastEmbed during global setup.
- `.github/workflows/papersflow-fork-pr.yml` always builds and type-checks Core,
  runs explicit affected-package checks for Okta Auth, Stagehand, Internal Core, CLI,
  Codemod, Deployer, MCP, Memory, Server, AI SDK, shared Storage Test Utils,
  PostgreSQL, Redis, Convex, LibSQL, Google Cloud PubSub, Redis Streams,
  Inngest, and the MastraCode SDK/TUI, and
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
  `InMemoryStore`. The durable-agent background-task E2E suite is likewise
  allowlisted because it self-provisions its local recorder gateway, uses
  committed replay fixtures, and uses the deterministic AI SDK mock for its
  cross-turn case. The exact Server favorites integration suite is also
  allowlisted because it exercises route handlers exclusively against
  `InMemoryStore`. All exact-path exceptions are content-conditioned: edits
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
  The PF-2044 ownership map additionally admits only the exact Convex cache,
  LibSQL thread-state, Google Cloud PubSub group, Redis Streams PubSub, and
  Inngest regression files needed by the reviewed PF-2026/PF-2007 footprints.
  A production-only change in those paths forces its paired native test to run;
  an unrecognized source or test fails closed. Convex's admitted tests are
  mocked/in-process, LibSQL uses a local database, and the admitted Inngest
  files mock transport or execute in process. Other Convex or Inngest tests
  remain rejected because they require credentials or a dedicated dev server.
  PF-2042 has one additional exact exception:
  `workflows/inngest/src/index.test.ts`, `workflows/inngest/docker-compose.yaml`,
  and `workflows/inngest/src/__tests__/adapters/_utils.ts` must change together
  and remain regular files with the exact reviewed Git blobs and SHA-256
  digests. Trusted semantic assertions additionally bind the Compose image,
  command, port mapping, host gateway, adapter constants, and index-suite
  endpoint constructor to ports 4200/4201. The validator runs
  `docker compose config --quiet`, directly lints both TypeScript files, then
  runs the full `index.test.ts` through a trusted-workflow-started
  digest-pinned `inngest/inngest:v1.34.0` dev server on the workspace's reviewed fixed,
  non-default Inngest CLI/handler endpoint pair. The workflow never starts the
  PR-controlled Compose file; the validator parses it after the trusted content
  and endpoint checks. The service is probed before Vitest and removed in an
  always-running cleanup step. It does not execute an adapter
  integration suite, admit another adapter file or live Inngest test, or admit
  `workflows/inngest/package.json`. For that exact three-file exception only,
  the validator does not claim a package-wide typecheck/build: clean fork
  `main` and the reviewed official-upstream snapshot both have the pre-existing
  `createRun`/`RunWithRawInput` mismatch tracked by PF-2051. Any broader Inngest
  source change, including PF-2007, still enters the native package checks and
  remains blocked until that baseline is repaired. PF-2050 owns the exact
  seven-file transition from the trusted-workflow/CLI split lifecycle to one
  package runtime manager: the atomic trio, the manager source and focused
  manager test, `workflows/inngest/package.json`, and
  `.github/workflows/papersflow-fork-pr.yml`. Admission proves that the manifest
  only removes the failure-unsafe `test:docker` script, the lockfile is
  unchanged, and the workflow only removes its legacy Inngest start/environment
  mutation/cleanup steps. Unrelated manifest or workflow edits and any
  reintroduced duplicate owner fail closed. The manager-owned surface runs
  native Inngest typecheck/build/lint, the focused lifecycle test, and the live
  index suite; the validator validates Compose structure but does not prestart a
  second daemon. Default Linux uses host networking with the reviewed
  `127.0.0.1` callback, while nested Docker remains an explicit
  network-and-URL configuration. Later manager-source changes force
  `src/__tests__/inngest-test-runtime.test.ts`; both the helper and its focused
  test are owned paths. Other unowned Inngest helpers and tests remain
  fail-closed.
  PF-2236 additionally owns two process-local durable-boundary contracts.
  Changes to `workflows/inngest/src/index.ts` force
  `src/create-run-contract.test.ts`, which exercises clone metadata against a
  mocked Inngest transport. Changes to `workflows/temporal/src/workflow.ts`
  force `src/workflow.test.ts`, whose fake Temporal client proves option
  admission without starting a Temporal service. Both paths run their native
  workspace typecheck, build, and lint before the focused Vitest file. Other
  Temporal sources, tests, manifests, and non-code inputs remain fail-closed.
  Temporal builds before its self-typecheck because clean checkouts do not yet
  have the package's `dist` declarations used by integration-fixture
  self-imports.
  PostgreSQL and Redis cache production paths likewise force their newly owned
  thread-state/indexed-log regressions. Other unprovisioned Store-provider tests
  fail closed for the same reason.
- Stagehand changes build and lint the package, then run its browser-independent
  Vitest suite. The `profile-lifecycle` test remains fail-closed because it
  requires a configured Chrome executable; changing, deleting, or renaming that test requires a
  dedicated browser-capable validation lane. Maintainers can exercise the
  workspace classification and command selection locally with
  `.github/scripts/run-papersflow-fork-pr-validation.bash --self-test-stagehand`.
- Actual `mastracode/**` changes build the GitHub Signals, Stagehand, and
  Agent Browser test prerequisites, run the native SDK/TUI build, typecheck,
  and lint commands, and execute the PF-1878 login-dialog, event-dispatch, and
  notification files plus the PF-2026 goal-manager and PF-2007 SDK
  signals-pubsub regression files in full. A production-only edit forces its
  matching regular test to run; every
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
- Redis cache and PostgreSQL are disposable job services matching the
  repository test ports and credentials. Redis Streams on port 6381 and the
  Google Cloud PubSub emulator on port 8085 start only when their owned
  workspaces change; both newly owned service images are pinned by immutable
  multi-architecture digest. The validator probes each required endpoint and fails
  before Vitest when the service is unavailable; none are shared runtime
  infrastructure.
- `pull_request_target` metadata workflows may use GitHub-hosted runners only
  when they check out trusted default-branch automation rather than PR code.
- Starsling remains available only to same-repository branches in the canonical
  repository; external pull-request heads use GitHub-hosted runners there too.
  Canonical-only workflows, including the initial Prebuild change detector,
  must guard every Starsling job so fork PRs skip rather than wait forever for
  a runner the fork cannot access.

Run the validator's focused policy fixtures locally after changing its routing
or command plan:

```bash
.github/scripts/run-papersflow-fork-pr-validation.bash --self-test
.github/scripts/run-papersflow-fork-pr-validation.bash --self-test-pf558-upstream-sync
.github/scripts/run-papersflow-fork-pr-validation.bash --self-test-pf2009-upstream-sync
.github/scripts/run-papersflow-fork-pr-validation.bash --self-test-pf2045-upstream-sync
.github/scripts/run-papersflow-fork-pr-validation.bash --self-test-pf2247-upstream-sync
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
They also prove production-only forced-test selection, direct test selection,
native command routing, service-unavailable failure, and rejection of unknown
sources inside a newly admitted PF-2044 workspace. They also accept the atomic
PF-2042 Inngest test/Compose/adapter-launcher trio, reject each unpaired member,
reject a valid-but-misaligned port topology, reject the Inngest manifest and
unknown live tests, and propagate Compose validation failure before executing
package commands. PF-2045 fixtures separately create the six genuine content
conflicts, prove the accepted reconstructed merge tree, the exact frozen
rehearsal-tree assertion, all six conflict resolutions, and both PF-2053
non-conflict overlays. They also prove that a benign non-conflicting
protected-base advance reconstructs and passes, and exercise wrong-upstream,
wrong-first-parent, forged-resolution, forged-overlay,
forged-same-parent-tree, protected-base conflict-input drift, extra-parent,
non-merge, and wrong-metadata failures. PF-2247 fixtures separately bind its
trusted lane to the exact reviewed merge commit, ordered parent pair, tree,
repository metadata, and protected-base intersection. They reject forged trees,
reversed and extra parents, non-merge heads, unreviewed base lineages, and
wrong branch metadata.

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
