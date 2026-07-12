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
- `.github/workflows/papersflow-fork-pr.yml` builds and type-checks the core
  package, runs baseline-clean checks for other affected fork packages, and
  executes newly added tests. A changed test file without a new or renamed
  test declaration runs in full. PostgreSQL changes run the package typecheck,
  build, and their changed tests. Explicit `*.e2e.test.*` files stay in
  package-specific or manual validation because this lane intentionally has no
  provider credentials.
- Actual `mastracode/**` changes run the MastraCode build and E2E suite in the
  targeted validator. The canonical MastraCode workflow stays available by
  manual dispatch in the fork, but Core-only fork PRs do not run its broad
  dependency build while the fork has unrelated package-build baseline errors.
- The workflow has `contents: read`, does not receive repository secrets, and
  checks out with persisted credentials disabled. Its validation script is
  loaded from the PR's trusted base commit. The first same-repository rollout
  may bootstrap that script from its head commit; external PRs may not.
- Redis and PostgreSQL are disposable job services, not shared runtime
  infrastructure.
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
