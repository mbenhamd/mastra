#!/usr/bin/env bash

set -euo pipefail

VALIDATOR_REPOSITORY_ROOT="$(git rev-parse --show-toplevel)"
TYPESCRIPT_MODULE_PATH="$VALIDATOR_REPOSITORY_ROOT/node_modules/typescript"
readonly VALIDATOR_REPOSITORY_ROOT TYPESCRIPT_MODULE_PATH

run_validator_self_tests() {
  local validator_path
  local test_root
  local fixture_repo
  local mock_bin
  local command_log
  local base_sha
  local head_sha
  local output
  local status

  validator_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
  test_root="$(mktemp -d)"
  validator_self_test_root="$test_root"
  trap 'rm -rf -- "${validator_self_test_root:?}"' EXIT
  fixture_repo="$test_root/repo"
  mock_bin="$test_root/bin"
  command_log="$test_root/pnpm.log"
  mkdir -p \
    "$fixture_repo/client-sdks/client-js/src" \
    "$fixture_repo/packages/cli/src/commands/api" \
    "$fixture_repo/packages/core/src/auth/ee/interfaces" \
    "$fixture_repo/packages/core/src/harness/v1" \
    "$fixture_repo/packages/server/src/server/handlers" \
    "$fixture_repo/packages/server/src/server/server-adapter/routes" \
    "$fixture_repo/node_modules" \
    "$mock_bin"
  # The production validator is copied to RUNNER_TEMP and resolves TypeScript
  # from the checked-out repository. Mirror that installed dependency in the
  # isolated fixture rather than resolving modules relative to BASH_SOURCE.
  ln -s "$TYPESCRIPT_MODULE_PATH" "$fixture_repo/node_modules/typescript"

  # The single-quoted lines are intentionally emitted into the mock pnpm
  # executable and expand only when that fixture command runs.
  printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' > "$mock_bin/pnpm"
  # shellcheck disable=SC2016
  printf '%s\n' \
    'printf '\''%s\n'\'' "$*" >> "${MOCK_PNPM_LOG:?}"' \
    'if [[ " $* " == *" check:permissions "* && "${MOCK_FAIL_PERMISSIONS:-0}" == 1 ]]; then exit 17; fi' \
    'if [[ " $* " == *" generate:route-types "* && "${MOCK_STALE_ROUTE_TYPES:-0}" == 1 ]]; then' \
    '  printf '\''%s\n'\'' "// regenerated" >> client-sdks/client-js/src/route-types.generated.ts' \
    'fi' \
    'if [[ " $* " == *" generate:route-types "* && ! -f client-sdks/client-js/src/route-types.generated.ts ]]; then' \
    '  mkdir -p client-sdks/client-js/src' \
    '  printf '\''%s\n'\'' "export const routeTypes = '\''regenerated'\'';" > client-sdks/client-js/src/route-types.generated.ts' \
    'fi' \
    'if [[ " $* " == *" generate:api-cli-route-metadata "* && "${MOCK_STALE_CLI_METADATA:-0}" == 1 ]]; then' \
    '  printf '\''%s\n'\'' "// regenerated" >> packages/cli/src/commands/api/route-metadata.generated.ts' \
    'fi' \
    'if [[ " $* " == *" generate:api-cli-route-metadata "* && ! -f packages/cli/src/commands/api/route-metadata.generated.ts ]]; then' \
    '  mkdir -p packages/cli/src/commands/api' \
    '  printf '\''%s\n'\'' "export const routeMetadata = '\''regenerated'\'';" > packages/cli/src/commands/api/route-metadata.generated.ts' \
    'fi' \
    'for argument in "$@"; do' \
    '  case "$argument" in' \
    '    --outputFile.json=*) printf '\''%s\n'\'' '\''{"numPassedTests":1}'\'' > "${argument#*=}" ;;' \
    '  esac' \
    'done' \
    >> "$mock_bin/pnpm"
  chmod +x "$mock_bin/pnpm"

  (
    cd "$fixture_repo"
    git init -q -b main
    git config user.email validator@example.test
    git config user.name 'Fork validator fixture'
    printf '%s\n' '{}' > package.json
    printf '%s\n' '{}' > client-sdks/client-js/package.json
    printf '%s\n' '{}' > packages/cli/package.json
    printf '%s\n' '{}' > packages/core/package.json
    printf '%s\n' 'export default {};' > packages/core/vitest.config.ts
    printf '%s\n' "export const permission = 'base';" \
      > packages/core/src/auth/ee/interfaces/permissions.generated.ts
    printf '%s\n' '{}' > packages/server/package.json
    printf '%s\n' 'export default {};' > packages/server/vitest.config.ts
    printf '%s\n' "export const route = 'base';" > packages/server/src/server/server-adapter/routes/index.ts
    printf '%s\n' "export const routeTypes = 'base';" > client-sdks/client-js/src/route-types.generated.ts
    printf '%s\n' "export const routeMetadata = 'base';" > packages/cli/src/commands/api/route-metadata.generated.ts
    printf '%s\n' 'export const favoriteFixture = true;' \
      > packages/server/src/server/handlers/favorites-helper.ts
    printf '%s\n' "import { it } from 'vitest';" "import { favoriteFixture } from './favorites-helper';" \
      "it('favorites', () => favoriteFixture);" \
      > packages/server/src/server/handlers/favorites.integration.test.ts
    printf '%s\n' "import { it } from 'vitest';" "it('harness', () => {});" \
      > packages/core/src/harness/v1/session.real-agent.e2e.test.ts
    printf '%s\n' "import { it } from 'vitest';" "it('external', () => {});" \
      > packages/server/src/server/handlers/external.integration.test.ts
    git add .
    git commit -q -m base
  )
  base_sha="$(git -C "$fixture_repo" rev-parse HEAD)"

  run_fixture() {
    local fixture_head="$1"
    local fixture_output="$2"
    shift 2
    (
      cd "$fixture_repo"
      env \
        PATH="$mock_bin:$PATH" \
        MOCK_PNPM_LOG="$command_log" \
        BASE_SHA="$base_sha" \
        HEAD_SHA="$fixture_head" \
        "$@" \
        bash "$validator_path"
    ) > "$fixture_output" 2>&1
  }

  assert_contains() {
    local expected="$1"
    local file="$2"
    if ! grep -Fq -- "$expected" "$file"; then
      echo "Expected fixture output to contain: $expected" >&2
      cat "$file" >&2
      exit 1
    fi
  }

  head_sha="$(
    cd "$fixture_repo"
    printf '%s\n' "export const route = 'head';" > packages/server/src/server/server-adapter/routes/index.ts
    printf '%s\n' "export const routeTypes = 'head';" > client-sdks/client-js/src/route-types.generated.ts
    printf '%s\n' "export const routeMetadata = 'head';" > packages/cli/src/commands/api/route-metadata.generated.ts
    git add .
    git commit -q -m 'server route change'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/server-success.log"
  run_fixture "$head_sha" "$output"
  assert_contains '--filter @mastra/server check:permissions' "$command_log"
  assert_contains '--filter @mastra/server generate:route-types' "$command_log"
  assert_contains '--filter @mastra/server generate:api-cli-route-metadata' "$command_log"
  assert_contains '--dir packages/server exec vitest run --reporter=dot src/server/server-adapter/schema-consistency.test.ts src/server/server-adapter/api-schema-manifest.test.ts' "$command_log"
  assert_contains '--dir client-sdks/client-js exec tsc-files --noEmit src/route-types.generated.ts src/types.ts src/resources/harness.ts src/resources/agent.test.ts' "$command_log"
  assert_contains '--dir packages/cli exec tsc-files --noEmit src/commands/api/route-metadata.generated.ts src/commands/api/index.ts src/commands/api/descriptors.test.ts' "$command_log"
  assert_contains '--dir client-sdks/client-js exec vitest run --reporter=dot src/resources/harness.test.ts' "$command_log"
  assert_contains '--dir packages/cli exec vitest run --reporter=dot src/commands/api/descriptors.test.ts' "$command_log"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' '{"scripts":{"build:lib":"true"}}' > packages/server/package.json
    git add .
    git commit -q -m 'change Server package scripts'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/server-manifest-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Changed Server package manifest fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/package.json' "$output"
  if [[ -s "$command_log" ]]; then
    echo 'Changed Server package manifest fixture executed PR-controlled package commands.' >&2
    cat "$command_log" >&2
    exit 1
  fi

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "export const permission = 'head';" \
      > packages/core/src/auth/ee/interfaces/permissions.generated.ts
    git add .
    git commit -q -m 'generated permissions only'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/generated-permissions-only.log"
  run_fixture "$head_sha" "$output"
  assert_contains 'packages/server' "$output"
  assert_contains '--filter @mastra/server check:permissions' "$command_log"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    git mv \
      packages/core/src/auth/ee/interfaces/permissions.generated.ts \
      packages/core/src/auth/ee/interfaces/permissions-renamed.ts
    git commit -q -m 'rename generated permissions artifact'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/generated-permissions-rename.log"
  run_fixture "$head_sha" "$output"
  assert_contains 'packages/server' "$output"
  assert_contains '--filter @mastra/server check:permissions' "$command_log"

  : > "$command_log"
  output="$test_root/permission-failure.log"
  set +e
  run_fixture "$head_sha" "$output" MOCK_FAIL_PERMISSIONS=1
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Permission freshness fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains '--filter @mastra/server check:permissions' "$command_log"

  : > "$command_log"
  output="$test_root/generated-failure.log"
  set +e
  run_fixture "$head_sha" "$output" MOCK_STALE_ROUTE_TYPES=1
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Stale generated route artifact fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'Generated Server route artifacts are stale or missing from the pull request.' "$output"

  git -C "$fixture_repo" reset -q --hard "$head_sha"
  : > "$command_log"
  output="$test_root/cli-generated-failure.log"
  set +e
  run_fixture "$head_sha" "$output" MOCK_STALE_CLI_METADATA=1
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Stale generated CLI route metadata fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'Generated Server route artifacts are stale or missing from the pull request.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "export const routeTypes = 'head';" > client-sdks/client-js/src/route-types.generated.ts
    git add .
    git commit -q -m 'generated route types only'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/generated-route-types-only.log"
  run_fixture "$head_sha" "$output"
  assert_contains 'packages/server' "$output"
  assert_contains '--filter @mastra/server generate:route-types' "$command_log"
  assert_contains '--filter @mastra/server generate:api-cli-route-metadata' "$command_log"
  assert_contains '--dir client-sdks/client-js exec tsc-files --noEmit src/route-types.generated.ts src/types.ts src/resources/harness.ts src/resources/agent.test.ts' "$command_log"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "export const routeMetadata = 'head';" > packages/cli/src/commands/api/route-metadata.generated.ts
    git add .
    git commit -q -m 'generated CLI metadata only'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/generated-cli-metadata-only.log"
  run_fixture "$head_sha" "$output"
  assert_contains 'packages/server' "$output"
  assert_contains '--filter @mastra/server generate:route-types' "$command_log"
  assert_contains '--filter @mastra/server generate:api-cli-route-metadata' "$command_log"
  assert_contains '--dir packages/cli exec tsc-files --noEmit src/commands/api/route-metadata.generated.ts src/commands/api/index.ts src/commands/api/descriptors.test.ts' "$command_log"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    git rm -q client-sdks/client-js/src/route-types.generated.ts
    git commit -q -m 'delete generated route types'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/deleted-route-types.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Deleted generated route types fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server' "$output"
  assert_contains 'Generated Server route artifacts are stale or missing from the pull request.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import '@playwright/test';" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'make favorites test require Playwright'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-playwright-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Playwright-dependent favorites fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import { run } from '@openai/agents';" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'make favorites test import scoped OpenAI agents SDK'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-openai-agents-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Scoped OpenAI agents fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'module @openai/agents' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import { openai } from '@mastra/openai';" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'make favorites test import Mastra OpenAI provider'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-mastra-openai-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Mastra OpenAI provider fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'module @mastra/openai' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "void import('./favorites-network-helper.js', { with: {} });" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    printf '%s\n' "import { createServer } from 'node:http';" \
      'export const networkFixture = createServer;' \
      > packages/server/src/server/handlers/favorites-network-helper.ts
    git add .
    git commit -q -m 'add unsafe dynamic import with options'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-dynamic-import-options-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Unsafe dynamic import-with-options fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import { createServer } from 'node:http2';" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'make favorites test require an HTTP2 server'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-http2-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'HTTP2-dependent favorites fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import { networkFixture } from './favorites-network-helper.js';" \
      >> packages/server/src/server/handlers/favorites-helper.ts
    printf '%s\n' "import { createServer } from 'http';" \
      'export const networkFixture = createServer;' \
      > packages/server/src/server/handlers/favorites-network-helper.ts
    git add .
    git commit -q -m 'add transitive unsafe favorites helper'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-transitive-helper-runtime-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Unsafe transitive favorites helper fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "// Documentation example only: import './not-a-real-module';" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'add import example comment'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-comment-import-success.log"
  run_fixture "$head_sha" "$output"
  assert_contains \
    'Running changed test file in full: packages/server/src/server/handlers/favorites.integration.test.ts' \
    "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import { createServer } from 'http';" \
      'export const serverFixture = createServer;' \
      > packages/server/src/server/handlers/server-network-helper.ts
    printf '%s\n' "import { it } from 'vitest';" \
      "import { serverFixture } from './server-network-helper.js';" \
      "it('server helper', () => serverFixture);" \
      > packages/server/src/server/handlers/server-helper.test.ts
    git add .
    git commit -q -m 'add unsafe Server unit-test helper'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/server-unit-helper-runtime-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Unsafe Server unit-test helper fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/server-helper.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import { execSync } from 'child_process';" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'make favorites test execute a bare process import'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-bare-process-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Bare process import in favorites fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import { createServer } from 'http';" \
      >> packages/server/src/server/handlers/favorites-helper.ts
    git add .
    git commit -q -m 'make imported favorites helper require a network primitive'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-helper-runtime-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Unsafe imported favorites helper fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import { GenericContainer } from 'testcontainers';" \
      "import { it } from 'vitest';" "it('container test', () => GenericContainer);" \
      > packages/server/src/server/handlers/container.test.ts
    git add .
    git commit -q -m 'add non-fork-safe Server unit test'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/server-unit-runtime-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Unsafe Server unit-test fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/container.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' 'const providerApiKey = process.env.PROVIDER_API_KEY;' \
      >> packages/core/src/harness/v1/session.real-agent.e2e.test.ts
    git add .
    git commit -q -m 'make Harness test require provider credentials'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/harness-provider-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Provider-dependent Harness fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/core/src/harness/v1/session.real-agent.e2e.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "it('harness update', () => {});" \
      >> packages/core/src/harness/v1/session.real-agent.e2e.test.ts
    git add .
    git commit -q -m 'safe Harness E2E change'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/harness-success.log"
  run_fixture "$head_sha" "$output"
  assert_contains \
    'Running changed test file in full: packages/core/src/harness/v1/session.real-agent.e2e.test.ts' \
    "$output"
  assert_contains 'src/harness/v1/session.real-agent.e2e.test.ts' "$command_log"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    git rm -q packages/cli/src/commands/api/route-metadata.generated.ts
    git commit -q -m 'delete generated CLI metadata'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/deleted-cli-metadata.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Deleted generated CLI metadata fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server' "$output"
  assert_contains 'Generated Server route artifacts are stale or missing from the pull request.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "it('favorites update', () => {});" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'favorites integration change'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-success.log"
  run_fixture "$head_sha" "$output"
  assert_contains \
    'Running changed test file in full: packages/server/src/server/handlers/favorites.integration.test.ts' \
    "$output"
  assert_contains 'src/server/handlers/favorites.integration.test.ts' "$command_log"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "it('external update', () => {});" \
      >> packages/server/src/server/handlers/external.integration.test.ts
    git add .
    git commit -q -m 'unsupported integration change'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/integration-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Non-allowlisted integration fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/external.integration.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  echo 'PapersFlow fork validator fixtures passed.'
}

if [[ "${1:-}" == "--self-test" ]]; then
  run_validator_self_tests
  exit 0
fi

: "${BASE_SHA:?BASE_SHA is required}"
: "${HEAD_SHA:?HEAD_SHA is required}"

validation_started_at=$SECONDS
validation_budget_seconds=$((50 * 60))
validation_reserve_seconds=120

remaining_validation_seconds() {
  local maximum_seconds="$1"
  local elapsed_seconds=$((SECONDS - validation_started_at))
  local available_seconds=$((validation_budget_seconds - elapsed_seconds - validation_reserve_seconds))

  if (( available_seconds <= 0 )); then
    return 1
  fi
  if (( available_seconds < maximum_seconds )); then
    printf '%s\n' "$available_seconds"
  else
    printf '%s\n' "$maximum_seconds"
  fi
}

run_with_validation_budget() {
  local maximum_seconds="$1"
  shift
  local timeout_seconds

  if ! timeout_seconds="$(remaining_validation_seconds "$maximum_seconds")"; then
    echo "Validation budget exhausted before: $*" >&2
    return 124
  fi
  timeout --kill-after=30s "${timeout_seconds}s" "$@"
}

changed_files="$(mktemp)"
changed_workspaces="$(mktemp)"
changed_tests="$(mktemp)"
delegated_docs_tests="$(mktemp)"
unowned_files="$(mktemp)"
unsupported_inputs="$(mktemp)"
unsupported_tests="$(mktemp)"
unsupported_workspaces="$(mktemp)"
workspace_candidates="$(mktemp)"
trap 'rm -f "$changed_files" "$changed_workspaces" "$changed_tests" "$delegated_docs_tests" "$unowned_files" "$unsupported_inputs" "$unsupported_tests" "$unsupported_workspaces" "$workspace_candidates"' EXIT

# Treat renames as a delete plus an add so both ownership boundaries are
# validated. Otherwise moving a generated artifact out of its canonical path
# can hide the source owner from a name-only diff.
git diff --no-renames --name-only --diff-filter=ACMRTD "${BASE_SHA}...${HEAD_SHA}" | sort > "$changed_files"

echo "Changed files:"
cat "$changed_files"

is_server_generated_artifact() {
  case "$1" in
    client-sdks/client-js/src/route-types.generated.ts | \
      packages/cli/src/commands/api/route-metadata.generated.ts | \
      packages/core/src/auth/ee/interfaces/permissions.generated.ts) return 0 ;;
    *) return 1 ;;
  esac
}

while IFS= read -r file; do
  if is_server_generated_artifact "$file"; then
    # These exact generated consumers are owned by Server route/permission
    # sources. Their output can be committed without granting general fork
    # validation coverage to the Client SDK, CLI, or Core workspaces.
    printf '%s\n' "packages/server" >> "$workspace_candidates"
    continue
  fi

  workspace_found=false
  search_dir="$(dirname "$file")"
  while [[ "$search_dir" != "." && "$search_dir" != "/" ]]; do
    manifest_path="${search_dir}/package.json"
    if [[ "$manifest_path" == */__fixtures__/* ]]; then
      search_dir="$(dirname "$search_dir")"
      continue
    fi
    if [[ -f "$manifest_path" ]] ||
      git cat-file -e "${HEAD_SHA}:${manifest_path}" 2>/dev/null ||
      git cat-file -e "${BASE_SHA}:${manifest_path}" 2>/dev/null; then
      printf '%s\n' "$search_dir" >> "$workspace_candidates"
      workspace_found=true
      break
    fi
    search_dir="$(dirname "$search_dir")"
  done
  if [[ "$workspace_found" == false ]]; then
    printf '%s\n' "$file" >> "$unowned_files"
  fi
done < "$changed_files"

sort -u "$workspace_candidates" > "$changed_workspaces"

echo "Changed workspaces:"
cat "$changed_workspaces"

while IFS= read -r workspace; do
  case "$workspace" in
    auth/okta | packages/_internal-core | packages/core | packages/deployer | packages/mcp | packages/memory | packages/server | client-sdks/ai-sdk | stores/_test-utils | stores/pg | stores/redis | mastracode | docs) ;;
    *) printf '%s\n' "$workspace" >> "$unsupported_workspaces" ;;
  esac
done < "$changed_workspaces"

while IFS= read -r file; do
  case "$file" in
    .changeset/* | \
      .github/scripts/run-papersflow-fork-pr-validation.bash | \
      .github/workflows/README.md | \
      .github/workflows/e2e-docs.yml | \
      .github/workflows/labeler.yml | \
      .github/workflows/lint-docs.yml | \
      .github/workflows/lint.yml | \
      .github/workflows/mastracode-e2e.yml | \
      .github/workflows/papersflow-fork-pr.yml) ;;
    *) printf '%s\n' "$file" >> "$unsupported_inputs" ;;
  esac
done < "$unowned_files"

grep -E '^(package\.json|pnpm-lock\.yaml|pnpm-workspace\.yaml|patches/)' "$changed_files" \
  >> "$unsupported_inputs" || true
# Server validation invokes package-owned scripts. Reject manifest edits before
# any PR-controlled Server command can weaken or replace those checks.
grep -Fx 'packages/server/package.json' "$changed_files" >> "$unsupported_inputs" || true

sort -u -o "$unsupported_inputs" "$unsupported_inputs"

if [[ -s "$unsupported_workspaces" || -s "$unsupported_inputs" ]]; then
  if [[ -s "$unsupported_workspaces" ]]; then
    echo "These changed workspaces do not have an owned fork-safe validation target:" >&2
    cat "$unsupported_workspaces" >&2
  fi
  if [[ -s "$unsupported_inputs" ]]; then
    echo "These non-workspace or root dependency-graph changes require dedicated validation:" >&2
    cat "$unsupported_inputs" >&2
  fi
  echo "Failing closed instead of reporting Core-only validation as workspace coverage." >&2
  exit 1
fi

workspace_changed() {
  grep -Fxq "$1" "$changed_workspaces"
}

mapfile -t prettier_files < <(
  while IFS= read -r file; do
    if [[ -f "$file" && "$file" =~ \.(cjs|css|js|json|jsx|md|mdx|mjs|ts|tsx|ya?ml)$ ]]; then
      printf '%s\n' "$file"
    fi
  done < "$changed_files"
)

if (( ${#prettier_files[@]} > 0 )); then
  run_with_validation_budget 300 pnpm exec prettier --check "${prettier_files[@]}"
fi

run_with_validation_budget 900 pnpm build:core
run_with_validation_budget 600 pnpm --filter @mastra/core check

if workspace_changed auth/okta; then
  run_with_validation_budget 900 pnpm --filter @mastra/auth-okta build
  run_with_validation_budget 600 pnpm --filter @mastra/auth-okta lint
fi

if workspace_changed packages/_internal-core; then
  run_with_validation_budget 600 pnpm --dir packages/_internal-core typecheck
fi

if workspace_changed packages/deployer; then
  run_with_validation_budget 900 pnpm --filter @mastra/memory build:lib
  run_with_validation_budget 900 pnpm --filter @mastra/agent-builder build
  run_with_validation_budget 900 pnpm --filter @mastra/server build:lib
  run_with_validation_budget 900 pnpm --filter @mastra/hono build
  run_with_validation_budget 600 pnpm --filter @mastra/deployer exec tsc --noEmit
  run_with_validation_budget 900 pnpm --filter @mastra/deployer build:lib
  run_with_validation_budget 600 pnpm --filter @mastra/deployer lint
fi

if workspace_changed packages/memory; then
  run_with_validation_budget 600 pnpm --filter @mastra/memory check
  run_with_validation_budget 900 pnpm --filter @mastra/memory build:lib
fi

if workspace_changed packages/mcp; then
  run_with_validation_budget 600 pnpm --filter @mastra/mcp exec tsc --noEmit
  run_with_validation_budget 900 pnpm --filter @mastra/mcp build:lib
  run_with_validation_budget 600 pnpm --filter @mastra/mcp lint
fi

if workspace_changed packages/server; then
  server_route_surface_changed=false
  if grep -Eq \
    '^(client-sdks/client-js/src/route-types\.generated\.ts|packages/cli/src/commands/api/route-metadata\.generated\.ts|packages/server/scripts/generate-(route-types|api-cli-route-metadata)\.ts|packages/server/src/server/(handlers|schemas|server-adapter/routes)/|packages/server/src/server/server-adapter/(api-schema-manifest|openapi-utils)\.ts)' \
    "$changed_files"; then
    server_route_surface_changed=true
  fi

  run_with_validation_budget 900 pnpm build:server
  run_with_validation_budget 600 pnpm --filter @mastra/server lint
  run_with_validation_budget 600 pnpm --filter @mastra/server check:core-imports
  run_with_validation_budget 300 pnpm --filter @mastra/server check:permissions
  run_with_validation_budget 300 pnpm --filter @mastra/server generate:route-types
  run_with_validation_budget 300 pnpm --filter @mastra/server generate:api-cli-route-metadata
  if ! git diff --exit-code -- \
    client-sdks/client-js/src/route-types.generated.ts \
    packages/cli/src/commands/api/route-metadata.generated.ts || \
    ! git ls-files --error-unmatch -- \
      client-sdks/client-js/src/route-types.generated.ts \
      packages/cli/src/commands/api/route-metadata.generated.ts >/dev/null; then
    echo "Generated Server route artifacts are stale or missing from the pull request." >&2
    echo "Run both @mastra/server route generators and commit their output." >&2
    exit 1
  fi
  if [[ "$server_route_surface_changed" == true ]]; then
    run_with_validation_budget 600 pnpm --dir packages/server exec vitest run --reporter=dot \
      src/server/server-adapter/schema-consistency.test.ts \
      src/server/server-adapter/api-schema-manifest.test.ts
    run_with_validation_budget 600 pnpm --dir client-sdks/client-js exec tsc-files --noEmit \
      src/route-types.generated.ts \
      src/types.ts \
      src/resources/harness.ts \
      src/resources/agent.test.ts
    run_with_validation_budget 600 pnpm --dir packages/cli exec tsc-files --noEmit \
      src/commands/api/route-metadata.generated.ts \
      src/commands/api/index.ts \
      src/commands/api/descriptors.test.ts
    run_with_validation_budget 600 pnpm --dir client-sdks/client-js exec vitest run --reporter=dot \
      src/resources/harness.test.ts
    run_with_validation_budget 600 pnpm --dir packages/cli exec vitest run --reporter=dot \
      src/commands/api/descriptors.test.ts
  fi
fi

if workspace_changed client-sdks/ai-sdk; then
  run_with_validation_budget 600 pnpm --filter @mastra/ai-sdk exec tsc --noEmit
  run_with_validation_budget 900 pnpm --filter @mastra/ai-sdk build:lib
fi

if workspace_changed stores/pg; then
  run_with_validation_budget 600 pnpm --filter @mastra/pg exec tsc --noEmit
  run_with_validation_budget 900 pnpm turbo build --filter ./stores/pg
fi

if workspace_changed stores/redis; then
  run_with_validation_budget 600 pnpm --dir stores/redis exec tsc --noEmit
  run_with_validation_budget 900 pnpm --dir stores/redis build:lib
fi

if workspace_changed mastracode; then
  run_with_validation_budget 900 pnpm run build:mastracode
  run_with_validation_budget 1200 pnpm --filter ./mastracode run e2e:test -- --reporter=dot
fi

is_explicit_fork_safe_test() {
  case "$1" in
    packages/core/src/harness/v1/session.real-agent.e2e.test.ts | \
      packages/server/src/server/handlers/favorites.integration.test.ts) return 0 ;;
    *) return 1 ;;
  esac
}

analyze_test_runtime_surface() {
  local mode="$1"
  local file="$2"

  node - \
    "$mode" \
    "$BASE_SHA" \
    "$file" \
    "$changed_files" \
    "$TYPESCRIPT_MODULE_PATH" <<'NODE'
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const [mode, baseSha, entryFile, changedFilesPath, typescriptModulePath] = process.argv.slice(2);
const ts = require(typescriptModulePath);
const repositoryRoot = process.cwd();
const changedFiles = new Set(fs.readFileSync(changedFilesPath, 'utf8').split('\n').filter(Boolean));

function repositoryPath(file) {
  const relative = path.relative(repositoryRoot, file);
  if (relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
    throw new Error(`Resolved test dependency escapes the repository: ${file}`);
  }
  return relative.split(path.sep).join('/');
}

function scriptKind(file) {
  switch (path.extname(file)) {
    case '.js':
    case '.cjs':
    case '.mjs':
      return ts.ScriptKind.JS;
    case '.jsx':
      return ts.ScriptKind.JSX;
    case '.tsx':
      return ts.ScriptKind.TSX;
    case '.json':
      return ts.ScriptKind.JSON;
    default:
      return ts.ScriptKind.TS;
  }
}

function sourceFile(file, source) {
  return ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, scriptKind(file));
}

function importDeclarationHasRuntimeValue(node) {
  const clause = node.importClause;
  if (!clause) return true;
  if (clause.isTypeOnly) return false;
  if (clause.name) return true;
  if (ts.isNamespaceImport(clause.namedBindings)) return true;
  return clause.namedBindings?.elements.some(element => !element.isTypeOnly) ?? false;
}

function exportDeclarationHasRuntimeValue(node) {
  if (node.isTypeOnly) return false;
  if (!node.exportClause || !ts.isNamedExports(node.exportClause)) return true;
  return node.exportClause.elements.some(element => !element.isTypeOnly);
}

function runtimeModuleSpecifiers(file, source) {
  const specifiers = new Set();
  const parsed = sourceFile(file, source);
  const visit = node => {
    if (
      ts.isImportDeclaration(node) &&
      importDeclarationHasRuntimeValue(node) &&
      ts.isStringLiteralLike(node.moduleSpecifier)
    ) {
      specifiers.add(node.moduleSpecifier.text);
    } else if (
      ts.isExportDeclaration(node) &&
      exportDeclarationHasRuntimeValue(node) &&
      node.moduleSpecifier &&
      ts.isStringLiteralLike(node.moduleSpecifier)
    ) {
      specifiers.add(node.moduleSpecifier.text);
    } else if (
      ts.isImportEqualsDeclaration(node) &&
      !node.isTypeOnly &&
      ts.isExternalModuleReference(node.moduleReference) &&
      node.moduleReference.expression &&
      ts.isStringLiteralLike(node.moduleReference.expression)
    ) {
      specifiers.add(node.moduleReference.expression.text);
    } else if (
      ts.isCallExpression(node) &&
      (node.expression.kind === ts.SyntaxKind.ImportKeyword ||
        (ts.isIdentifier(node.expression) && node.expression.text === 'require')) &&
      node.arguments.length >= 1 &&
      ts.isStringLiteralLike(node.arguments[0])
    ) {
      specifiers.add(node.arguments[0].text);
    }
    ts.forEachChild(node, visit);
  };
  visit(parsed);
  return specifiers;
}

function loadCompilerOptions() {
  const fallback = {
    allowJs: true,
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    resolveJsonModule: true,
  };
  const configPath = ts.findConfigFile(path.dirname(path.resolve(entryFile)), ts.sys.fileExists, 'tsconfig.json');
  if (!configPath) return fallback;
  const config = ts.readConfigFile(configPath, ts.sys.readFile);
  if (config.error) throw new Error(ts.flattenDiagnosticMessageText(config.error.messageText, '\n'));
  return {
    ...fallback,
    ...ts.parseJsonConfigFileContent(config.config, ts.sys, path.dirname(configPath)).options,
  };
}

const compilerOptions = loadCompilerOptions();

function resolveLocalImport(importer, specifier, failOnUnresolved) {
  if (!specifier.startsWith('.')) return undefined;
  const resolved = ts.resolveModuleName(specifier, importer, compilerOptions, ts.sys).resolvedModule;
  if (!resolved) {
    if (failOnUnresolved) throw new Error(`Cannot resolve local test dependency ${specifier} from ${importer}`);
    return undefined;
  }
  const resolvedFile = path.resolve(resolved.resolvedFileName);
  repositoryPath(resolvedFile);
  return resolvedFile;
}

const baseSourceCache = new Map();
function readBaseSource(file) {
  const relative = repositoryPath(file);
  if (baseSourceCache.has(relative)) return baseSourceCache.get(relative);
  let source;
  try {
    source = execFileSync('git', ['show', `${baseSha}:${relative}`], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
  } catch {
    source = undefined;
  }
  baseSourceCache.set(relative, source);
  return source;
}

function collectGraph(readSource, failOnUnresolved) {
  const entry = path.resolve(entryFile);
  const queue = [entry];
  const visited = new Set();
  while (queue.length > 0) {
    const file = queue.shift();
    const relative = repositoryPath(file);
    if (visited.has(relative)) continue;
    const source = readSource(file);
    if (source === undefined) continue;
    visited.add(relative);
    for (const specifier of runtimeModuleSpecifiers(file, source)) {
      const dependency = resolveLocalImport(file, specifier, failOnUnresolved);
      if (dependency) queue.push(dependency);
    }
  }
  return visited;
}

const headGraph = collectGraph(file => fs.readFileSync(file, 'utf8'), true);
const baseGraph = collectGraph(readBaseSource, false);
const surface = new Set(
  [...headGraph].filter(file => file === entryFile || changedFiles.has(file) || !baseGraph.has(file)),
);
surface.add(entryFile);

const bannedMastraPackages = new Set([
  'astra',
  'chroma',
  'clickhouse',
  'cloudflare',
  'cloudflare-d1',
  'convex',
  'couchbase',
  'dsql',
  'duckdb',
  'dynamodb',
  'elasticsearch',
  'lance',
  'libsql',
  'mongodb',
  'mssql',
  'mysql',
  'opensearch',
  'pg',
  'pinecone',
  'qdrant',
  'redis',
  's3vectors',
  'spanner',
  'turbopuffer',
  'upstash',
  'vectorize',
]);
const bannedBuiltins = new Set([
  'child_process',
  'cluster',
  'dgram',
  'dns',
  'http',
  'http2',
  'https',
  'net',
  'tls',
  'worker_threads',
]);

function unsupportedModuleReason(specifier) {
  if (specifier === '@playwright/test' || specifier === 'testcontainers') return specifier;
  if (specifier.startsWith('@ai-sdk/')) return specifier;
  if (specifier.startsWith('@openai/')) return specifier;
  if (specifier === '@anthropic-ai/sdk' || specifier === '@google/generative-ai') return specifier;
  if (specifier === 'ollama' || specifier === 'openai') return specifier;
  if (specifier === '@mastra/openai' || specifier.startsWith('@mastra/openai/')) return specifier;
  if (specifier.startsWith('@mastra/')) {
    const packageName = specifier.slice('@mastra/'.length).split('/')[0];
    if (bannedMastraPackages.has(packageName)) return specifier;
  }
  const bareSpecifier = specifier.startsWith('node:') ? specifier.slice('node:'.length) : specifier;
  if (bannedBuiltins.has(bareSpecifier.split('/')[0])) return specifier;
  return undefined;
}

function propertyName(node) {
  if (ts.isPropertyAccessExpression(node)) return node.name.text;
  if (ts.isElementAccessExpression(node) && node.argumentExpression && ts.isStringLiteralLike(node.argumentExpression)) {
    return node.argumentExpression.text;
  }
  return undefined;
}

function unsupportedRuntimeReasons(file, source) {
  const reasons = new Set();
  for (const specifier of runtimeModuleSpecifiers(file, source)) {
    const reason = unsupportedModuleReason(specifier);
    if (reason) reasons.add(`module ${reason}`);
  }
  const parsed = sourceFile(file, source);
  const visit = node => {
    if (
      (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) &&
      ts.isIdentifier(node.expression) &&
      node.expression.text === 'process' &&
      propertyName(node) === 'env'
    ) {
      reasons.add('process.env');
    }
    if (ts.isCallExpression(node) || ts.isNewExpression(node)) {
      const expression = node.expression;
      const name = ts.isIdentifier(expression)
        ? expression.text
        : ts.isPropertyAccessExpression(expression)
          ? expression.name.text
          : undefined;
      if (name === 'fetch' || name === 'WebSocket') reasons.add(`${name}()`);
    }
    ts.forEachChild(node, visit);
  };
  visit(parsed);
  return reasons;
}

if (mode === 'surface') {
  process.stdout.write([...surface].sort().join('\n'));
} else if (mode === 'unsupported') {
  const findings = [];
  for (const file of [...surface].sort()) {
    const absoluteFile = path.resolve(file);
    for (const reason of unsupportedRuntimeReasons(absoluteFile, fs.readFileSync(absoluteFile, 'utf8'))) {
      findings.push(`${file}: ${reason}`);
    }
  }
  process.stdout.write(findings.join('\n'));
} else {
  throw new Error(`Unknown test-runtime analysis mode: ${mode}`);
}
NODE
}

list_test_runtime_surface() {
  analyze_test_runtime_surface surface "$1"
}

test_runtime_surface_has_unsupported_runtime() {
  local file="$1"
  local findings=""

  if ! findings="$(analyze_test_runtime_surface unsupported "$file")"; then
    echo "Could not inspect the local runtime dependency surface for test: $file" >&2
    return 0
  fi
  if [[ -n "$findings" ]]; then
    echo "Unsupported fork-test runtime surface for $file:" >&2
    printf '%s\n' "$findings" >&2
    return 0
  fi
  return 1
}

mapfile -t detected_tests < <(
  while IFS= read -r file; do
    if [[ -f "$file" ]] && grep -Eq \
      '\.(test|spec)\.(ts|tsx|js|jsx|mjs|cjs)$|\.test-d\.ts$' \
      <<< "$file"; then
      printf '%s\n' "$file"
    fi
  done < "$changed_files"
)

# A changed or newly reachable local dependency of an exact-path exception
# changes the runtime contract of that test even when the test file is untouched.
for explicit_test in \
  packages/core/src/harness/v1/session.real-agent.e2e.test.ts \
  packages/server/src/server/handlers/favorites.integration.test.ts; do
  [[ -f "$explicit_test" ]] || continue
  if surface="$(list_test_runtime_surface "$explicit_test")"; then
    while IFS= read -r source_file; do
      if grep -Fxq "$source_file" "$changed_files"; then
        detected_tests+=("$explicit_test")
        break
      fi
    done <<< "$surface"
  else
    printf '%s\n' "$explicit_test" >> "$unsupported_tests"
  fi
done

if (( ${#detected_tests[@]} > 0 )); then
  mapfile -t detected_tests < <(printf '%s\n' "${detected_tests[@]}" | sort -u)
fi

if (( ${#detected_tests[@]} > 0 )); then
  for file in "${detected_tests[@]}"; do
    if [[ "$file" == docs/* ]] && grep -Eq "['\"]@playwright/test['\"]" "$file"; then
      printf '%s\n' "$file" >> "$delegated_docs_tests"
    elif is_explicit_fork_safe_test "$file" &&
      test_runtime_surface_has_unsupported_runtime "$file"; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    elif [[ "$file" == packages/server/* ]] &&
      test_runtime_surface_has_unsupported_runtime "$file"; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    elif [[ "$file" == packages/core/src/harness/v1/session.real-agent.e2e.test.ts ]]; then
      # This is a deterministic, in-process Vitest suite. It uses Mastra's mock
      # language model and InMemoryStore and requires no provider credentials or
      # external service beyond the standard Core test environment.
      printf '%s\n' "$file" >> "$changed_tests"
    elif [[ "$file" == packages/server/src/server/handlers/favorites.integration.test.ts ]]; then
      # This exact cross-layer Server suite is deterministic and fork-safe: it
      # exercises real route handlers against InMemoryStore without credentials,
      # provider calls, containers, or other external infrastructure.
      printf '%s\n' "$file" >> "$changed_tests"
    elif [[ "$file" == e2e-tests/* || "$file" == */integration-tests/* || \
      "$file" =~ \.e2e\.(test|spec)\.(ts|tsx|js|jsx|mjs|cjs)$ ]] || \
      grep -Eq "['\"]@playwright/test['\"]" "$file"; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    elif [[ "$file" =~ integration\.(test|spec)\. && \
      "$file" != stores/pg/* && "$file" != stores/redis/* ]]; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    elif [[ "$file" == stores/* && "$file" != stores/_test-utils/* && \
      "$file" != stores/pg/* && "$file" != stores/redis/* ]]; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    elif [[ "$file" == stores/pg/* && \
      ( "$file" =~ \.pooler\.test\. || "$file" =~ \.performance\.test\. || \
        "$file" == */performance-indexes/* || "$file" == */row-number-performance.test.* ) ]]; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    else
      printf '%s\n' "$file" >> "$changed_tests"
    fi
  done
fi

if [[ -s "$delegated_docs_tests" ]]; then
  echo "Delegating docs Playwright files to the fork-enabled Docs E2E workflow:"
  cat "$delegated_docs_tests"
fi

if [[ -s "$unsupported_tests" ]]; then
  echo "These changed tests require a dedicated fork-safe workflow or suite-specific infrastructure:" >&2
  cat "$unsupported_tests" >&2
  echo "Failing closed instead of reporting incomplete validation as successful." >&2
  exit 1
fi

if workspace_changed stores/_test-utils &&
  ! grep -Eq '^stores/_test-utils/.*\.(test|spec)\.(ts|tsx|js|jsx|mjs|cjs)$' "$changed_tests"; then
  echo "Storage test utility changes must include a changed Vitest file in stores/_test-utils." >&2
  echo "Failing closed instead of accepting unexecuted shared conformance helpers." >&2
  exit 1
fi

if [[ ! -s "$changed_tests" ]]; then
  echo "No changed test files detected. Build and type checks completed."
  exit 0
fi

echo "Changed test files:"
cat "$changed_tests"

test_status=0
while IFS= read -r file; do
  status=0
  test_dir=""
  search_dir="$(dirname "$file")"

  while [[ "$search_dir" != "." && "$search_dir" != "/" ]]; do
    if [[ -f "$search_dir/vitest.config.ts" || -f "$search_dir/vitest.config.mts" || \
      -f "$search_dir/vitest.config.js" || -f "$search_dir/vitest.config.mjs" || \
      -f "$search_dir/vitest.config.cjs" || -f "$search_dir/vite.config.ts" || \
      -f "$search_dir/vite.config.mts" || -f "$search_dir/vite.config.js" || \
      -f "$search_dir/vite.config.mjs" || -f "$search_dir/vite.config.cjs" ]]; then
      test_dir="$search_dir"
      break
    fi
    search_dir="$(dirname "$search_dir")"
  done

  if [[ -n "$test_dir" ]]; then
    relative_file="${file#"$test_dir"/}"
    if [[ "$file" == *.test-d.ts ]]; then
      if ! timeout_seconds="$(remaining_validation_seconds 600)"; then
        echo "Validation budget exhausted before $file; failing predictably before the workflow timeout." >&2
        test_status=124
        break
      fi
      set +e
      timeout --kill-after=30s "${timeout_seconds}s" \
        pnpm --dir "$test_dir" exec vitest run --typecheck.only --reporter=dot "$relative_file"
      status=$?
      set -e
    else
      if ! timeout_seconds="$(remaining_validation_seconds 900)"; then
        echo "Validation budget exhausted before $file; failing predictably before the workflow timeout." >&2
        test_status=124
        break
      fi
      echo "Running changed test file in full: $file"
      test_result="$(mktemp)"
      set +e
      timeout --kill-after=30s "${timeout_seconds}s" \
        pnpm --dir "$test_dir" exec vitest run \
          --reporter=dot --reporter=json --outputFile.json="$test_result" \
          "$relative_file"
      status=$?
      set -e

      if (( status == 0 )); then
        set +e
        node - "$test_result" <<'NODE'
const fs = require('node:fs');
const result = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
if (result.numPassedTests < 1) {
  console.error('The changed test file did not execute a passing Vitest test.');
  process.exit(1);
}
NODE
        status=$?
        set -e
      fi
      rm -f "$test_result"
    fi
  elif [[ "$file" == *.test-d.ts ]]; then
    if ! timeout_seconds="$(remaining_validation_seconds 600)"; then
      echo "Validation budget exhausted before $file; failing predictably before the workflow timeout." >&2
      test_status=124
      break
    fi
    set +e
    timeout --kill-after=30s "${timeout_seconds}s" pnpm exec vitest run --typecheck.only --reporter=dot "$file"
    status=$?
    set -e
  else
    if ! timeout_seconds="$(remaining_validation_seconds 900)"; then
      echo "Validation budget exhausted before $file; failing predictably before the workflow timeout." >&2
      test_status=124
      break
    fi
    echo "Running changed test file in full: $file"
    test_result="$(mktemp)"
    set +e
    timeout --kill-after=30s "${timeout_seconds}s" \
      pnpm exec vitest run \
        --reporter=dot --reporter=json --outputFile.json="$test_result" \
        "$file"
    status=$?
    set -e

    if (( status == 0 )); then
      set +e
      node - "$test_result" <<'NODE'
const fs = require('node:fs');
const result = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
if (result.numPassedTests < 1) {
  console.error('The changed test file did not execute a passing Vitest test.');
  process.exit(1);
}
NODE
      status=$?
      set -e
    fi

    rm -f "$test_result"
  fi

  if (( status != 0 )); then
    test_status=$status
  fi
done < "$changed_tests"

exit "$test_status"
