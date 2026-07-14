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
    "$fixture_repo/client-sdks/client-js/src/resources" \
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
    printf '%s\n' \
      '{"compilerOptions":{"module":"NodeNext","moduleResolution":"NodeNext","noEmit":true,"strict":true}}' \
      > client-sdks/client-js/tsconfig.json
    printf '%s\n' '{}' > packages/cli/package.json
    printf '%s\n' '{}' > packages/core/package.json
    printf '%s\n' 'export default {};' > packages/core/vitest.config.ts
    printf '%s\n' "export const permission = 'base';" \
      > packages/core/src/auth/ee/interfaces/permissions.generated.ts
    printf '%s\n' '{}' > packages/server/package.json
    printf '%s\n' 'export default {};' > packages/server/vitest.config.ts
    printf '%s\n' "export const route = 'base';" > packages/server/src/server/server-adapter/routes/index.ts
    printf '%s\n' 'export type RouteTypes = { source: "base" };' \
      'export type HarnessRoute = { source: "base" };' \
      'export type IndexOnlyRoute = { source: "base" };' \
      > client-sdks/client-js/src/route-types.generated.ts
    printf '%s\n' "import type { RouteTypes } from './route-types.generated.js';" \
      'export type ClientRouteTypes = RouteTypes;' \
      > client-sdks/client-js/src/types.ts
    printf '%s\n' "import type { HarnessRoute } from '../route-types.generated.js';" \
      'export type ClientHarnessRoute = HarnessRoute;' \
      > client-sdks/client-js/src/resources/harness.ts
    printf '%s\n' "export type { IndexOnlyRoute } from './route-types.generated.js';" \
      > client-sdks/client-js/src/index.ts
    printf '%s\n' "export const routeMetadata = 'base';" > packages/cli/src/commands/api/route-metadata.generated.ts
    printf '%s\n' 'export const favoriteFixture = true;' \
      > packages/server/src/server/handlers/favorites-helper.ts
    printf '%s\n' "import { Hono } from 'hono';" \
      'export const dormantExternalFixture = Hono;' \
      > packages/server/src/server/handlers/dormant-external-helper.ts
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

  assert_route_consumer_commands() {
    assert_contains '--dir client-sdks/client-js exec tsc-files --noEmit src/route-types.generated.ts src/types.ts src/resources/harness.ts src/resources/agent.test.ts' "$command_log"
    assert_contains '--dir packages/cli exec tsc-files --noEmit src/commands/api/route-metadata.generated.ts src/commands/api/index.ts src/commands/api/descriptors.test.ts' "$command_log"
    assert_contains '--dir client-sdks/client-js exec vitest run src/resources/harness.test.ts --reporter=dot' "$command_log"
    assert_contains '--dir packages/cli exec vitest run src/commands/api/descriptors.test.ts --reporter=dot' "$command_log"
  }

  head_sha="$(
    cd "$fixture_repo"
    printf '%s\n' "export const route = 'head';" > packages/server/src/server/server-adapter/routes/index.ts
    printf '%s\n' 'export type RouteTypes = { source: "head" };' \
      'export type HarnessRoute = { source: "head" };' \
      'export type IndexOnlyRoute = { source: "head" };' \
      > client-sdks/client-js/src/route-types.generated.ts
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
  assert_contains 'Client SDK route consumers accept generated route types.' "$output"
  assert_route_consumer_commands
  assert_contains 'src/server/server-adapter/schema-consistency.test.ts' "$command_log"
  assert_contains 'src/server/server-adapter/api-schema-manifest.test.ts' "$command_log"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' '{"scripts":{"build:lib":"true"}}' > packages/server/package.json
    git add .
    git commit -q -m 'change Server package scripts'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/server-package-manifest-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Changed Server package manifest unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/package.json' "$output"
  assert_contains 'require dedicated validation' "$output"
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
    printf '%s\n' 'export type RouteTypes = { source: "head" };' \
      'export type HarnessRoute = { source: "head" };' \
      'export type IndexOnlyRoute = { source: "head" };' \
      > client-sdks/client-js/src/route-types.generated.ts
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
  assert_contains 'Client SDK route consumers accept generated route types.' "$output"
  assert_route_consumer_commands

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' 'export type RouteTypes = { source: "head" };' \
      'export type HarnessRoute = { source: "head" };' \
      > client-sdks/client-js/src/route-types.generated.ts
    git add .
    git commit -q -m 'drop an index-only generated route export'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/generated-index-export-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Missing public generated route export unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'Client SDK index references missing generated exports: IndexOnlyRoute' "$output"

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
  assert_contains 'Client SDK route consumers accept generated route types.' "$output"
  assert_route_consumer_commands

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
    printf '%s\n' "import { z } from 'zod';" 'void z;' \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'make favorites test use an approved external module'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-approved-external-success.log"
  run_fixture "$head_sha" "$output"
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$command_log"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import { createOpenAI } from '@ai-sdk/openai';" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'make favorites test import AI SDK provider'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-ai-sdk-provider-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'AI SDK provider fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'module @ai-sdk/openai' "$output"

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
    printf '%s\n' "import { dormantExternalFixture } from './dormant-external-helper';" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'make a dormant external helper newly reachable'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-dormant-external-helper-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Newly reachable dormant external helper unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

  head_sha="$(
    cd "$fixture_repo"
    git reset -q --hard "$base_sha"
    printf '%s\n' "import { OpenAIVoice } from '@mastra/voice-openai';" \
      >> packages/server/src/server/handlers/favorites.integration.test.ts
    git add .
    git commit -q -m 'make favorites test require a voice provider'
    git rev-parse HEAD
  )"
  : > "$command_log"
  output="$test_root/favorites-voice-provider-failure.log"
  set +e
  run_fixture "$head_sha" "$output"
  status=$?
  set -e
  if (( status == 0 )); then
    echo 'Voice-provider favorites fixture unexpectedly passed.' >&2
    cat "$output" >&2
    exit 1
  fi
  assert_contains 'packages/server/src/server/handlers/favorites.integration.test.ts' "$output"
  assert_contains 'Failing closed instead of reporting incomplete validation as successful.' "$output"

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


run_stagehand_validation_self_test() {
  local script_path test_root test_repo stub_bin command_log base_sha head_sha deleted_test_sha deletion_log
  local restored_test_sha renamed_test_sha rename_log
  script_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
  test_root="$(mktemp -d)"
  test_repo="$test_root/repo"
  stub_bin="$test_root/bin"
  command_log="$test_root/commands.log"
  trap 'rm -rf "$test_root"' RETURN

  mkdir -p "$test_repo/browser/stagehand/src/__tests__" "$test_repo/packages/core/src" "$stub_bin"
  cat > "$stub_bin/pnpm" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${VALIDATION_COMMAND_LOG:?}"

if [[ "$*" == *'--dir packages/core exec vitest list '* ]]; then
  test_file="${!#}"
  absolute_file="$(pwd)/packages/core/$test_file"
  if [[ "$*" == *'duplicate leaf$'* ]]; then
    printf '[{"name":"passing sibling > duplicate leaf","file":"%s"},{"name":"second sibling > duplicate leaf","file":"%s"}]\n' \
      "$absolute_file" "$absolute_file"
  else
    printf '[{"name":"outer suite > inner suite > unique nested case","file":"%s"}]\n' "$absolute_file"
  fi
  exit 0
fi

if [[ "$*" == *'--dir packages/core exec vitest run '* ]]; then
  test_file="${!#}"
  absolute_file="$(pwd)/packages/core/$test_file"
  output_file=''
  for argument in "$@"; do
    if [[ "$argument" == --outputFile.json=* ]]; then
      output_file="${argument#--outputFile.json=}"
    fi
  done
  : "${output_file:?Vitest JSON output path is required}"
  printf '{"numPassedTests":1,"numFailedTests":0,"testResults":[{"name":"%s","assertionResults":[{"status":"passed","ancestorTitles":["outer suite","inner suite"],"title":"unique nested case"}]}]}\n' \
    "$absolute_file" > "$output_file"
fi
STUB
  chmod +x "$stub_bin/pnpm"

  git -C "$test_repo" init --quiet
  git -C "$test_repo" config user.email validator@example.invalid
  git -C "$test_repo" config user.name 'Fork validator self-test'
  printf '{"name":"@mastra/stagehand"}\n' > "$test_repo/browser/stagehand/package.json"
  printf '{"name":"@mastra/core"}\n' > "$test_repo/packages/core/package.json"
  printf 'export type BrowserConfig = {}\n' > "$test_repo/browser/stagehand/src/types.ts"
  printf 'export {}\n' > "$test_repo/browser/stagehand/src/__tests__/profile-lifecycle.test.ts"
  git -C "$test_repo" add .
  git -C "$test_repo" commit --quiet -m base
  base_sha="$(git -C "$test_repo" rev-parse HEAD)"
  printf 'export type BrowserConfig = { recording?: boolean }\n' > "$test_repo/browser/stagehand/src/types.ts"
  git -C "$test_repo" commit --quiet -am head
  head_sha="$(git -C "$test_repo" rev-parse HEAD)"

  (
    cd "$test_repo"
    PATH="$stub_bin:$PATH" VALIDATION_COMMAND_LOG="$command_log" BASE_SHA="$base_sha" HEAD_SHA="$head_sha" \
      bash "$script_path"
  )

  grep -Fx -- 'build:core' "$command_log"
  grep -Fx -- '--filter ./browser/stagehand --fail-if-no-match build' "$command_log"
  grep -Fx -- '--filter ./browser/stagehand --fail-if-no-match lint' "$command_log"
  grep -Fx -- \
    '--dir browser/stagehand exec vitest run --reporter=dot --exclude src/__tests__/profile-lifecycle.test.ts' \
    "$command_log"

  rm "$test_repo/browser/stagehand/src/__tests__/profile-lifecycle.test.ts"
  git -C "$test_repo" commit --quiet -am 'delete browser-dependent test'
  deleted_test_sha="$(git -C "$test_repo" rev-parse HEAD)"
  deletion_log="$test_root/deletion.log"
  if (
    cd "$test_repo"
    PATH="$stub_bin:$PATH" VALIDATION_COMMAND_LOG="$command_log" BASE_SHA="$head_sha" HEAD_SHA="$deleted_test_sha" \
      bash "$script_path"
  ) 2> "$deletion_log"; then
    echo 'Stagehand fork validation accepted deletion of profile-lifecycle.test.ts.' >&2
    return 1
  fi
  grep -Fx -- 'browser/stagehand/src/__tests__/profile-lifecycle.test.ts' "$deletion_log"
  grep -F -- 'Failing closed instead of' "$deletion_log"

  printf 'export {}\n' > "$test_repo/browser/stagehand/src/__tests__/profile-lifecycle.test.ts"
  git -C "$test_repo" add browser/stagehand/src/__tests__/profile-lifecycle.test.ts
  git -C "$test_repo" commit --quiet -m 'restore browser-dependent test'
  restored_test_sha="$(git -C "$test_repo" rev-parse HEAD)"
  git -C "$test_repo" mv \
    browser/stagehand/src/__tests__/profile-lifecycle.test.ts \
    browser/stagehand/src/profile-lifecycle.ts
  git -C "$test_repo" commit --quiet -m 'rename browser-dependent test'
  renamed_test_sha="$(git -C "$test_repo" rev-parse HEAD)"
  rename_log="$test_root/rename.log"
  if (
    cd "$test_repo"
    PATH="$stub_bin:$PATH" VALIDATION_COMMAND_LOG="$command_log" \
      BASE_SHA="$restored_test_sha" HEAD_SHA="$renamed_test_sha" bash "$script_path"
  ) 2> "$rename_log"; then
    echo 'Stagehand fork validation accepted renaming profile-lifecycle.test.ts.' >&2
    return 1
  fi
  grep -Fx -- 'browser/stagehand/src/__tests__/profile-lifecycle.test.ts' "$rename_log"
  grep -F -- 'Failing closed instead of' "$rename_log"
  echo 'Stagehand fork-validation self-test passed.'
}

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

run_isolated_vitest_case() {
  local test_workspace="$1"
  local vitest_file="$2"
  local test_name="$3"
  local test_pattern="$4"
  local expected_test=""
  local list_result=""
  local status=0
  local test_result=""
  local timeout_seconds=""

  list_result="$(mktemp)"
  expected_test="$(mktemp)"
  test_result="$(mktemp)"

  if ! timeout_seconds="$(remaining_validation_seconds 240)"; then
    echo "Validation budget exhausted before collecting Agent signal test: $test_name" >&2
    rm -f "$list_result" "$expected_test" "$test_result"
    return 124
  fi

  echo "Collecting changed Agent signal test: $test_name"
  if timeout --kill-after=30s "${timeout_seconds}s" \
    pnpm --dir "$test_workspace" exec vitest list \
      --json -t "$test_pattern" "$vitest_file" > "$list_result"; then
    status=0
  else
    status=$?
    echo "Vitest could not collect the selected Agent signal case: $test_name" >&2
    rm -f "$list_result" "$expected_test" "$test_result"
    return "$status"
  fi

  if ! node - "$list_result" "$expected_test" "$test_name" <<'NODE'
const fs = require('node:fs');
const path = require('node:path');

try {
  const listed = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
  const selectedTitle = process.argv[4];
  if (!Array.isArray(listed)) {
    throw new Error('Vitest list did not return an array.');
  }
  if (listed.length !== 1) {
    throw new Error(
      `Expected exactly one collected Vitest test for ${JSON.stringify(selectedTitle)}; found ${listed.length}.`,
    );
  }

  const [expected] = listed;
  if (!expected || typeof expected.name !== 'string' || typeof expected.file !== 'string') {
    throw new Error('Vitest list returned an entry without a test name and file.');
  }
  fs.writeFileSync(
    process.argv[3],
    JSON.stringify({
      file: path.resolve(expected.file),
      name: expected.name,
    }),
  );
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}
NODE
  then
    echo "The selected Agent signal case is not uniquely collectable; failing closed." >&2
    rm -f "$list_result" "$expected_test" "$test_result"
    return 1
  fi

  if ! timeout_seconds="$(remaining_validation_seconds 240)"; then
    echo "Validation budget exhausted before Agent signal test: $test_name" >&2
    rm -f "$list_result" "$expected_test" "$test_result"
    return 124
  fi

  echo "Running changed Agent signal test in isolation: $test_name"
  if timeout --kill-after=30s "${timeout_seconds}s" \
    pnpm --dir "$test_workspace" exec vitest run \
      --reporter=dot --reporter=json --outputFile.json="$test_result" \
      -t "$test_pattern" "$vitest_file"; then
    status=0
  else
    status=$?
  fi

  if (( status == 0 )); then
    if node - "$test_result" "$expected_test" "$test_name" <<'NODE'
const fs = require('node:fs');
const path = require('node:path');

try {
  const result = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
  const expected = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
  const selectedTitle = process.argv[4];
  const passed = [];

  for (const testFile of result.testResults ?? []) {
    for (const assertion of testFile.assertionResults ?? []) {
      if (assertion.status === 'passed') passed.push({ assertion, file: testFile.name });
    }
  }

  if (result.numPassedTests !== 1 || result.numFailedTests !== 0 || passed.length !== 1) {
    throw new Error(
      `Expected exactly one passing Vitest assertion and no failures; reporter recorded ` +
        `${result.numPassedTests ?? 0} passed, ${result.numFailedTests ?? 0} failed, and ` +
        `${passed.length} passing assertion entries.`,
    );
  }

  const [{ assertion, file }] = passed;
  const reporterIdentity = [...(assertion.ancestorTitles ?? []), assertion.title].join(' > ');
  if (assertion.title !== selectedTitle) {
    throw new Error(
      `Vitest passed ${JSON.stringify(assertion.title)} instead of selected case ${JSON.stringify(selectedTitle)}.`,
    );
  }
  if (reporterIdentity !== expected.name) {
    throw new Error(
      `Vitest reporter identity ${JSON.stringify(reporterIdentity)} does not match collected identity ` +
        `${JSON.stringify(expected.name)}.`,
    );
  }
  if (typeof file !== 'string' || path.resolve(file) !== expected.file) {
    throw new Error(
      `Vitest reporter file ${JSON.stringify(file)} does not match collected file ${JSON.stringify(expected.file)}.`,
    );
  }
} catch (error) {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
}
NODE
    then
      status=0
    else
      status=$?
    fi
  fi

  rm -f "$list_result" "$expected_test" "$test_result"
  return "$status"
}

run_vitest_selection_regressions() (
  local duplicate_file=""
  local fixture_dir=""
  local test_log=""
  local test_workspace="packages/core"
  local unique_file=""

  if [[ ! -f "$test_workspace/package.json" ]]; then
    echo "Vitest selection self-test requires the packages/core workspace." >&2
    return 1
  fi

  fixture_dir="$(mktemp -d "$test_workspace/src/papersflow-vitest-selection.XXXXXX")"
  test_log="$(mktemp)"
  trap 'rm -rf "$fixture_dir"; rm -f "$test_log"' EXIT

  unique_file="$fixture_dir/unique-nested.test.ts"
  cat > "$unique_file" <<'TEST'
import { describe, expect, it } from 'vitest';

describe('outer suite', () => {
  describe('inner suite', () => {
    it('unique nested case', () => {
      expect(true).toBe(true);
    });
  });
});
TEST

  run_isolated_vitest_case \
    "$test_workspace" "${unique_file#"$test_workspace"/}" \
    'unique nested case' 'unique nested case$'

  duplicate_file="$fixture_dir/duplicate-leaf.test.ts"
  cat > "$duplicate_file" <<'TEST'
import { describe, expect, it } from 'vitest';

describe('passing sibling', () => {
  it('duplicate leaf', () => {
    expect(true).toBe(true);
  });
});

describe('second sibling', () => {
  it('duplicate leaf', () => {
    expect(true).toBe(true);
  });
});
TEST

  if run_isolated_vitest_case \
    "$test_workspace" "${duplicate_file#"$test_workspace"/}" \
    'duplicate leaf' 'duplicate leaf$' > "$test_log" 2>&1; then
    echo "Duplicate-leaf Vitest selection unexpectedly passed." >&2
    return 1
  fi
  if ! grep -Fq 'Expected exactly one collected Vitest test for "duplicate leaf"; found 2.' "$test_log"; then
    echo "Duplicate-leaf Vitest selection failed for an unexpected reason:" >&2
    cat "$test_log" >&2
    return 1
  fi

  echo "Vitest selection regressions passed: unique nested identity and duplicate-leaf fail-closed behavior."
  rm -rf "$fixture_dir"
  rm -f "$test_log"
  trap - EXIT
)

case "${1:-}" in
  --self-test-stagehand)
    run_stagehand_validation_self_test
    exit
    ;;
  --self-test-vitest-selection)
    run_vitest_selection_regressions
    exit
    ;;
  '') ;;
  *)
    echo "Unknown validator argument: $1" >&2
    exit 2
    ;;
esac

: "${BASE_SHA:?BASE_SHA is required}"
: "${HEAD_SHA:?HEAD_SHA is required}"

merge_base_sha="$(git merge-base "$BASE_SHA" "$HEAD_SHA")"

changed_files="$(mktemp)"
changed_lockfile_importers="$(mktemp)"
changed_workspaces="$(mktemp)"
changed_tests="$(mktemp)"
forced_mastracode_tests="$(mktemp)"
delegated_docs_tests="$(mktemp)"
deleted_tests="$(mktemp)"
fixer_test_result="$(mktemp)"
root_vitest_config_list="$(mktemp)"
unowned_files="$(mktemp)"
unsupported_inputs="$(mktemp)"
missing_mastracode_tests="$(mktemp)"
unsupported_mastracode_tests="$(mktemp)"
unsupported_mastracode_sources="$(mktemp)"
unsupported_tests="$(mktemp)"
unsupported_workspaces="$(mktemp)"
workspace_candidates="$(mktemp)"
trap 'rm -f "$changed_files" "$changed_lockfile_importers" "$changed_workspaces" "$changed_tests" "$forced_mastracode_tests" "$delegated_docs_tests" "$deleted_tests" "$fixer_test_result" "$root_vitest_config_list" "$unowned_files" "$unsupported_inputs" "$missing_mastracode_tests" "$unsupported_mastracode_tests" "$unsupported_mastracode_sources" "$unsupported_tests" "$unsupported_workspaces" "$workspace_candidates"' EXIT

# Treat renames as a delete plus an add so both ownership boundaries are
# validated. Otherwise moving a generated artifact out of its canonical path
# can hide the source owner from a name-only diff.
git diff --no-renames --name-only --diff-filter=ACMRTD "${merge_base_sha}..${HEAD_SHA}" | sort > "$changed_files"

# Deleted Vitest files never reach the changed-test runner (it only enqueues
# files that exist at the proposed head), so removed coverage in owned
# workspaces would otherwise be accepted silently. Collect deletions here and
# fail closed on them below.
git diff --no-renames --name-only --diff-filter=D "${merge_base_sha}..${HEAD_SHA}" |
  grep -E '\.(test|spec)\.(ts|tsx|js|jsx|mjs|cjs|mts|cts)$|\.test-d\.ts$' |
  sort > "$deleted_tests" || true

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
  if [[ ! -f "$workspace/package.json" ]]; then
    printf '%s\n' "$workspace" >> "$unsupported_workspaces"
    continue
  fi
  case "$workspace" in
    auth/okta | browser/stagehand | packages/_internal-core | packages/cli | packages/codemod | packages/core | packages/deployer | packages/mcp | packages/memory | packages/server | client-sdks/ai-sdk | stores/_test-utils | stores/pg | stores/redis | mastracode | docs) ;;
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
      .github/workflows/papersflow-fork-pr.yml | \
      pnpm-lock.yaml | \
      scripts/commonjs-tsc-fixer.js | \
      scripts/commonjs-tsc-fixer.test.ts | \
      scripts/tsconfig.json | \
      scripts/vitest.config.ts | \
      vitest.config.ts) ;;
    *) printf '%s\n' "$file" >> "$unsupported_inputs" ;;
  esac
done < "$unowned_files"

grep -E '^(package\.json|pnpm-workspace\.yaml|patches/)' "$changed_files" \
  >> "$unsupported_inputs" || true
# Server validation invokes package-owned scripts. Reject manifest edits before
# any PR-controlled Server command can weaken or replace those checks.
grep -Fx 'packages/server/package.json' "$changed_files" >> "$unsupported_inputs" || true

if grep -Fxq 'pnpm-lock.yaml' "$changed_files"; then
  node - "$merge_base_sha" "$HEAD_SHA" > "$changed_lockfile_importers" <<'NODE'
const { execFileSync } = require('node:child_process');

function lockfileSections(sha) {
  const lockfile = execFileSync('git', ['show', `${sha}:pnpm-lock.yaml`], {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  const lines = lockfile.split('\n');
  const sections = new Map();
  const start = lines.indexOf('importers:');
  if (start < 0) return { importers: sections, nonImporterGraph: lockfile };

  let importer;
  let body = [];
  let end = lines.length;
  const flush = () => {
    if (importer !== undefined) sections.set(importer, body.join('\n'));
  };

  for (let index = start + 1; index < lines.length; index += 1) {
    const line = lines[index];
    if (line && !line.startsWith(' ')) {
      end = index;
      break;
    }
    const match = /^  (\S.*):$/.exec(line);
    if (match) {
      flush();
      importer = match[1].replace(/^'|'$/g, '').replace(/''/g, "'");
      body = [];
    } else if (importer !== undefined) {
      body.push(line);
    }
  }
  flush();
  return {
    importers: sections,
    nonImporterGraph: [...lines.slice(0, start), ...lines.slice(end)].join('\n'),
  };
}

const before = lockfileSections(process.argv[2]);
const after = lockfileSections(process.argv[3]);
if (before.nonImporterGraph !== after.nonImporterGraph) {
  console.log('__PAPERSFLOW_NON_IMPORTER_GRAPH__');
}
const importers = new Set([...before.importers.keys(), ...after.importers.keys()]);
for (const importer of [...importers].sort()) {
  if (before.importers.get(importer) !== after.importers.get(importer)) console.log(importer);
}
NODE

  lockfile_importers_match_manifests=true
  if [[ ! -s "$changed_lockfile_importers" ]]; then
    lockfile_importers_match_manifests=false
  fi
  while IFS= read -r importer; do
    if [[ "$importer" == "__PAPERSFLOW_NON_IMPORTER_GRAPH__" ]]; then
      lockfile_importers_match_manifests=false
      echo "Lockfile content outside importer sections changed without an owned validation target." >&2
      continue
    fi
    manifest="${importer}/package.json"
    if [[ "$importer" == "." ]]; then
      manifest="package.json"
    fi
    if ! grep -Fxq "$manifest" "$changed_files"; then
      lockfile_importers_match_manifests=false
      echo "Lockfile importer changed without its manifest: $importer" >&2
    fi
  done < "$changed_lockfile_importers"

  if [[ "$lockfile_importers_match_manifests" == false ]]; then
    printf '%s\n' 'pnpm-lock.yaml' >> "$unsupported_inputs"
  fi
fi

git_regular_file_at_head() {
  git ls-tree "$HEAD_SHA" -- "$1" | grep -Eq '^100(644|755) blob '
}

while IFS= read -r file; do
  if [[ "$file" =~ ^mastracode/.*\.(cjs|cts|js|jsx|mjs|mts|ts|tsx)$ ]]; then
    if grep -Eq '\.(test|spec)\.(cjs|cts|js|jsx|mjs|mts|ts|tsx)$|\.test-d\.ts$' <<< "$file"; then
      if ! git_regular_file_at_head "$file"; then
        printf '%s\n' "$file" >> "$missing_mastracode_tests"
      else
        case "$file" in
          mastracode/src/tui/components/login-dialog.test.ts | \
            mastracode/src/tui/event-dispatch.test.ts | \
            mastracode/src/tui/notify.test.ts | \
            mastracode/src/utils/__tests__/signals-pubsub.test.ts) ;;
          *) printf '%s\n' "$file" >> "$unsupported_mastracode_tests" ;;
        esac
      fi
      continue
    fi
    required_test=""
    case "$file" in
      mastracode/src/tui/components/login-dialog.ts)
        required_test="mastracode/src/tui/components/login-dialog.test.ts"
        ;;
      mastracode/src/tui/event-dispatch.ts)
        required_test="mastracode/src/tui/event-dispatch.test.ts"
        ;;
      mastracode/src/tui/notify.ts)
        required_test="mastracode/src/tui/notify.test.ts"
        ;;
      mastracode/src/utils/signals-pubsub.ts)
        required_test="mastracode/src/utils/__tests__/signals-pubsub.test.ts"
        ;;
      mastracode/src/index.ts)
        # Composition root: no unit suite owns it; the mastracode lane's
        # build:mastracode compiles it and the owned TUI suites exercise its
        # wiring. Restoration-only edits are accepted under the build gate.
        ;;
      *)
        printf '%s\n' "$file" >> "$unsupported_mastracode_sources"
        ;;
    esac
    if [[ -n "$required_test" ]]; then
      if ! git_regular_file_at_head "$file" || ! git_regular_file_at_head "$required_test"; then
        printf '%s\n' "$file" >> "$unsupported_mastracode_sources"
      elif ! grep -Fxq "$required_test" "$changed_files"; then
        # The paired suite exists but was not edited: force it to RUN so the
        # source change is still executed against its owned coverage instead
        # of failing closed on parse-only or restoration commits.
        printf '%s\n' "$required_test" >> "$forced_mastracode_tests"
      fi
    fi
  fi
done < "$changed_files"

sort -u -o "$unsupported_inputs" "$unsupported_inputs"
sort -u -o "$missing_mastracode_tests" "$missing_mastracode_tests"
sort -u -o "$unsupported_mastracode_tests" "$unsupported_mastracode_tests"
sort -u -o "$unsupported_mastracode_sources" "$unsupported_mastracode_sources"

if [[ -s "$unsupported_workspaces" || -s "$unsupported_inputs" || -s "$deleted_tests" || -s "$missing_mastracode_tests" || -s "$unsupported_mastracode_tests" || -s "$unsupported_mastracode_sources" ]]; then
  if [[ -s "$unsupported_workspaces" ]]; then
    echo "These changed workspaces do not have an owned fork-safe validation target:" >&2
    cat "$unsupported_workspaces" >&2
  fi
  if [[ -s "$unsupported_inputs" ]]; then
    echo "These non-workspace or root dependency-graph changes require dedicated validation:" >&2
    cat "$unsupported_inputs" >&2
  fi
  if [[ -s "$deleted_tests" ]]; then
    echo "These Vitest files are deleted at the proposed head, so this validator cannot re-run their coverage:" >&2
    cat "$deleted_tests" >&2
  fi
  if [[ -s "$unsupported_mastracode_sources" ]]; then
    echo "These MastraCode production sources are outside the owned source-and-test pairs:" >&2
    cat "$unsupported_mastracode_sources" >&2
  fi
  if [[ -s "$missing_mastracode_tests" ]]; then
    echo "These changed MastraCode tests do not exist as regular files at the proposed head and cannot run:" >&2
    cat "$missing_mastracode_tests" >&2
  fi
  if [[ -s "$unsupported_mastracode_tests" ]]; then
    echo "These MastraCode tests are outside the three owned regression files:" >&2
    cat "$unsupported_mastracode_tests" >&2
  fi
  echo "Failing closed instead of reporting Core-only validation as workspace coverage." >&2
  exit 1
fi

workspace_changed() {
  grep -Fxq "$1" "$changed_workspaces"
}

server_route_source_changed() {
  grep -Eq \
    '^(packages/server/scripts/generate-(route-types|api-cli-route-metadata)\.ts|packages/server/src/server/(handlers|schemas|server-adapter/routes)/|packages/server/src/server/server-adapter/(api-schema-manifest|openapi-utils)\.ts)' \
    "$changed_files"
}

server_route_contract_changed() {
  server_route_source_changed ||
    grep -Eq \
      '^(client-sdks/client-js/src/route-types\.generated\.ts|packages/cli/src/commands/api/route-metadata\.generated\.ts)$' \
      "$changed_files"
}

check_client_route_consumers() {
  local timeout_seconds
  if ! timeout_seconds="$(remaining_validation_seconds 600)"; then
    echo 'Validation budget exhausted before Client SDK route-consumer typecheck.' >&2
    return 124
  fi

  timeout --kill-after=30s "${timeout_seconds}s" node - "$TYPESCRIPT_MODULE_PATH" <<'NODE'
const fs = require('node:fs');
const path = require('node:path');

const ts = require(process.argv[2]);
const formatHost = {
  getCanonicalFileName: file => file,
  getCurrentDirectory: () => process.cwd(),
  getNewLine: () => '\n',
};
const configPath = 'client-sdks/client-js/tsconfig.json';
const config = ts.readConfigFile(configPath, ts.sys.readFile);
if (config.error) {
  console.error(ts.formatDiagnosticsWithColorAndContext([config.error], formatHost));
  process.exit(1);
}

const parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, path.dirname(configPath));
if (parsed.errors.length > 0) {
  console.error(ts.formatDiagnosticsWithColorAndContext(parsed.errors, formatHost));
  process.exit(1);
}

const rootNames = [
  'client-sdks/client-js/src/route-types.generated.ts',
  'client-sdks/client-js/src/types.ts',
  'client-sdks/client-js/src/resources/harness.ts',
].map(file => path.resolve(file));
const program = ts.createProgram({
  rootNames,
  options: {
    ...parsed.options,
    incremental: false,
    noEmit: true,
    tsBuildInfoFile: undefined,
  },
});
const diagnostics = ts.getPreEmitDiagnostics(program);
if (diagnostics.length > 0) {
  console.error(ts.formatDiagnosticsWithColorAndContext(diagnostics, formatHost));
  process.exit(1);
}

const generatedSource = program.getSourceFile(rootNames[0]);
const generatedModule = generatedSource && program.getTypeChecker().getSymbolAtLocation(generatedSource);
if (!generatedSource || !generatedModule) {
  console.error('Could not resolve the generated Client SDK route-type module.');
  process.exit(1);
}
const generatedExports = new Set(
  program
    .getTypeChecker()
    .getExportsOfModule(generatedModule)
    .map(symbol => symbol.getName()),
);
const indexPath = 'client-sdks/client-js/src/index.ts';
const indexSource = ts.createSourceFile(
  indexPath,
  fs.readFileSync(indexPath, 'utf8'),
  ts.ScriptTarget.Latest,
  true,
  ts.ScriptKind.TS,
);
if (indexSource.parseDiagnostics.length > 0) {
  console.error(ts.formatDiagnosticsWithColorAndContext(indexSource.parseDiagnostics, formatHost));
  process.exit(1);
}
const publicRouteExports = [];
for (const statement of indexSource.statements) {
  if (
    ts.isExportDeclaration(statement) &&
    statement.moduleSpecifier &&
    ts.isStringLiteralLike(statement.moduleSpecifier) &&
    statement.moduleSpecifier.text === './route-types.generated.js' &&
    statement.exportClause &&
    ts.isNamedExports(statement.exportClause)
  ) {
    for (const element of statement.exportClause.elements) {
      publicRouteExports.push(element.propertyName?.text ?? element.name.text);
    }
  }
}
if (publicRouteExports.length === 0) {
  console.error('Client SDK index has no explicit generated route-type re-export contract.');
  process.exit(1);
}
const missingPublicExports = publicRouteExports.filter(name => !generatedExports.has(name));
if (missingPublicExports.length > 0) {
  console.error(`Client SDK index references missing generated exports: ${missingPublicExports.join(', ')}`);
  process.exit(1);
}
console.log('Client SDK route consumers accept generated route types.');
NODE
}

server_prerequisites_built=false
ensure_server_prerequisites() {
  if [[ "$server_prerequisites_built" == true ]]; then
    return
  fi

  run_with_validation_budget 900 pnpm --filter ./packages/memory --fail-if-no-match build:lib
  run_with_validation_budget 900 pnpm --filter ./packages/agent-builder --fail-if-no-match build
  run_with_validation_budget 900 pnpm --filter ./packages/server --fail-if-no-match build:lib
  server_prerequisites_built=true
}

deployer_prerequisites_built=false
ensure_deployer_prerequisites() {
  if [[ "$deployer_prerequisites_built" == true ]]; then
    return
  fi

  ensure_server_prerequisites
  run_with_validation_budget 900 pnpm --filter ./server-adapters/hono --fail-if-no-match build
  run_with_validation_budget 900 pnpm --filter ./packages/deployer --fail-if-no-match build:lib
  deployer_prerequisites_built=true
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
run_with_validation_budget 600 pnpm --filter ./packages/core --fail-if-no-match check
if [[ -z "${MOCK_PNPM_LOG:-}" ]]; then
  # The Vitest selection self-check drives real pnpm/vitest processes. Inside
  # the validator's own fixture harness pnpm is a command-logging mock, so the
  # self-check cannot run there; production runs never set MOCK_PNPM_LOG.
  run_vitest_selection_regressions
fi

if workspace_changed auth/okta; then
  run_with_validation_budget 900 pnpm --filter ./auth/okta --fail-if-no-match build
  run_with_validation_budget 600 pnpm --filter ./auth/okta --fail-if-no-match lint
fi

if workspace_changed browser/stagehand; then
  run_with_validation_budget 900 pnpm --filter ./browser/stagehand --fail-if-no-match build
  run_with_validation_budget 600 pnpm --filter ./browser/stagehand --fail-if-no-match lint
  run_with_validation_budget 900 pnpm --dir browser/stagehand exec vitest run \
    --reporter=dot --exclude src/__tests__/profile-lifecycle.test.ts
fi

if workspace_changed packages/_internal-core; then
  run_with_validation_budget 600 pnpm --dir packages/_internal-core typecheck
fi

if workspace_changed packages/server; then
  ensure_server_prerequisites
  run_with_validation_budget 600 pnpm --filter ./packages/server --fail-if-no-match exec tsc --noEmit
  run_with_validation_budget 600 pnpm --filter ./packages/server --fail-if-no-match lint
  run_with_validation_budget 300 pnpm --filter ./packages/server --fail-if-no-match check:core-imports
fi

if workspace_changed packages/deployer; then
  ensure_deployer_prerequisites
  run_with_validation_budget 600 pnpm --filter ./packages/deployer --fail-if-no-match exec tsc --noEmit
  run_with_validation_budget 600 pnpm --filter ./packages/deployer --fail-if-no-match lint
fi

if workspace_changed packages/cli; then
  ensure_deployer_prerequisites
  run_with_validation_budget 900 pnpm --filter ./packages/loggers --fail-if-no-match build:lib
  run_with_validation_budget 600 pnpm --filter ./packages/cli --fail-if-no-match exec tsc --noEmit
  run_with_validation_budget 600 pnpm --filter ./packages/cli --fail-if-no-match lint
fi

if workspace_changed packages/codemod; then
  run_with_validation_budget 600 pnpm --filter ./packages/codemod --fail-if-no-match exec tsc --noEmit
  run_with_validation_budget 900 pnpm --filter ./packages/codemod --fail-if-no-match build
  run_with_validation_budget 600 pnpm --filter ./packages/codemod --fail-if-no-match lint
fi

if grep -Eq '^(scripts/(commonjs-tsc-fixer\.(js|test\.ts)|tsconfig\.json|vitest\.config\.ts)|vitest\.config\.ts)$' "$changed_files"; then
  # scripts/tsconfig.json includes the root vitest.config.ts, so this also
  # type-checks root Vitest config changes.
  run_with_validation_budget 600 pnpm exec tsc --project scripts/tsconfig.json --noEmit
fi

if grep -Fxq 'scripts/commonjs-tsc-fixer.js' "$changed_files"; then
  # The scripts tsconfig only type-checks the fixer's test and config, so a
  # changed JS fixer must run its Vitest regressions to be validated.
  echo "Running the CommonJS fixer Vitest regressions for the changed scripts/commonjs-tsc-fixer.js"
  run_with_validation_budget 600 pnpm exec vitest run \
    --config scripts/vitest.config.ts \
    --reporter=dot --reporter=json --outputFile.json="$fixer_test_result" \
    commonjs-tsc-fixer.test.ts
  node - "$fixer_test_result" <<'NODE'
const fs = require('node:fs');
const result = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
if (result.numPassedTests < 1 || result.numFailedTests !== 0) {
  console.error('The CommonJS fixer Vitest regressions did not record a passing run.');
  process.exit(1);
}
NODE
fi

if grep -Fxq 'vitest.config.ts' "$changed_files"; then
  # Type-checking alone does not execute the root config's project discovery.
  # Prove the changed root Vitest config still loads and resolves its
  # discovered projects by collecting the fixer suite through it.
  echo "Collecting the scripts project through the changed root vitest.config.ts"
  run_with_validation_budget 600 pnpm exec vitest list \
    --config vitest.config.ts --project scripts \
    --json="$root_vitest_config_list" commonjs-tsc-fixer.test.ts
  node - "$root_vitest_config_list" <<'NODE'
const fs = require('node:fs');
const listed = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
if (!Array.isArray(listed) || listed.length < 1) {
  console.error('The root Vitest config smoke did not collect any scripts-project tests.');
  process.exit(1);
}
NODE
fi

if workspace_changed packages/memory; then
  run_with_validation_budget 600 pnpm --filter ./packages/memory --fail-if-no-match check
  run_with_validation_budget 900 pnpm --filter ./packages/memory --fail-if-no-match build:lib
fi

if workspace_changed packages/mcp; then
  run_with_validation_budget 600 pnpm --filter ./packages/mcp --fail-if-no-match exec tsc --noEmit
  run_with_validation_budget 900 pnpm --filter ./packages/mcp --fail-if-no-match build:lib
  run_with_validation_budget 600 pnpm --filter ./packages/mcp --fail-if-no-match lint
fi

if workspace_changed packages/server; then
  run_with_validation_budget 900 pnpm build:server
  run_with_validation_budget 600 pnpm --filter @mastra/server lint
  run_with_validation_budget 600 pnpm --filter @mastra/server check:core-imports
  run_with_validation_budget 300 pnpm --filter @mastra/server check:permissions
  if grep -Fxq 'packages/server/src/server/server-adapter/routes/permissions.ts' "$changed_files" || \
    grep -Fxq 'packages/core/src/auth/ee/interfaces/permissions.generated.ts' "$changed_files"; then
    # check:permissions only proves the generated RBAC table is self-consistent
    # with getEffectivePermission. Permission-source changes must also pass the
    # semantics suite that encodes what callers rely on.
    run_with_validation_budget 600 \
      pnpm --dir packages/server exec vitest run src/server/server-adapter/routes/permissions.test.ts --reporter=dot
  fi
  # Anchor artifact freshness to the commit the workflow checked out. In CI
  # that is the pull_request merge commit, whose second parent must be the PR
  # head; local and fixture runs check out the head directly. Capturing the
  # anchor before the generators run means a PR-controlled generator that
  # stages or commits its own output still diffs against reviewed content.
  server_artifact_anchor="$(git rev-parse HEAD)"
  if [[ "$server_artifact_anchor" != "$HEAD_SHA" && \
    "$(git rev-parse --quiet --verify 'HEAD^2' 2>/dev/null || true)" != "$HEAD_SHA" ]]; then
    echo "Validation checkout matches neither the pull request head ${HEAD_SHA} nor a merge of it." >&2
    echo "Failing closed instead of validating unreviewed content." >&2
    exit 1
  fi
  run_with_validation_budget 300 pnpm --filter @mastra/server generate:route-types
  run_with_validation_budget 300 pnpm --filter @mastra/server generate:api-cli-route-metadata
  if [[ "$(git rev-parse HEAD)" != "$server_artifact_anchor" ]]; then
    echo "A generator or package script moved HEAD during Server validation; failing closed." >&2
    exit 1
  fi
  if ! git cat-file -e "${server_artifact_anchor}:client-sdks/client-js/src/route-types.generated.ts" 2>/dev/null || \
    ! git cat-file -e "${server_artifact_anchor}:packages/cli/src/commands/api/route-metadata.generated.ts" 2>/dev/null || \
    ! git diff --exit-code "$server_artifact_anchor" -- \
      client-sdks/client-js/src/route-types.generated.ts \
      packages/cli/src/commands/api/route-metadata.generated.ts; then
    echo "Generated Server route artifacts are stale or missing from the pull request." >&2
    echo "Run both @mastra/server route generators and commit their output." >&2
    exit 1
  fi
  if server_route_contract_changed; then
    # Byte-clean generated output is not sufficient if either native consumer
    # rejects it. Keep this scoped to the direct Client SDK and CLI type/test
    # consumers so unrelated package baselines cannot mask route compatibility.
    check_client_route_consumers
    run_with_validation_budget 600 pnpm --dir client-sdks/client-js exec tsc-files --noEmit \
      src/route-types.generated.ts \
      src/types.ts \
      src/resources/harness.ts \
      src/resources/agent.test.ts
    run_with_validation_budget 600 pnpm --dir packages/cli exec tsc-files --noEmit \
      src/commands/api/route-metadata.generated.ts \
      src/commands/api/index.ts \
      src/commands/api/descriptors.test.ts
    run_with_validation_budget 600 \
      pnpm --dir client-sdks/client-js exec vitest run src/resources/harness.test.ts --reporter=dot
    run_with_validation_budget 600 \
      pnpm --dir packages/cli exec vitest run src/commands/api/descriptors.test.ts --reporter=dot
  fi
fi

if workspace_changed client-sdks/ai-sdk; then
  run_with_validation_budget 600 pnpm --filter ./client-sdks/ai-sdk --fail-if-no-match exec tsc --noEmit
  run_with_validation_budget 900 pnpm --filter ./client-sdks/ai-sdk --fail-if-no-match build:lib
fi

if workspace_changed stores/pg; then
  run_with_validation_budget 600 pnpm --filter ./stores/pg --fail-if-no-match exec tsc --noEmit
  run_with_validation_budget 900 pnpm turbo build --filter ./stores/pg
fi

if workspace_changed stores/redis; then
  run_with_validation_budget 600 pnpm --dir stores/redis exec tsc --noEmit
  run_with_validation_budget 900 pnpm --dir stores/redis build:lib
fi

if workspace_changed mastracode; then
  # MastraCode's Vitest setup mocks these workspace packages, but Vite still
  # resolves their exported dist entries before applying the mocks
  # (settings.ts lazily imports @mastra/stagehand; the TUI imports
  # @mastra/github-signals).
  run_with_validation_budget 900 pnpm --filter ./signals/github --fail-if-no-match build:lib
  run_with_validation_budget 900 pnpm --filter ./browser/stagehand --fail-if-no-match build
  mapfile -t mastracode_lint_files < <(
    while IFS= read -r file; do
      if [[ -f "$file" && "$file" =~ ^mastracode/.*\.(ts|tsx|js|jsx|mjs|cjs)$ ]]; then
        printf '%s\n' "$file"
      fi
    done < "$changed_files"
  )
  if (( ${#mastracode_lint_files[@]} > 0 )); then
    run_with_validation_budget 600 pnpm exec eslint "${mastracode_lint_files[@]}"
  fi
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
const { builtinModules } = require('node:module');
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

function runtimeModuleSpecifiers(file, source, computedSpecifiers) {
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
        (ts.isIdentifier(node.expression) && node.expression.text === 'require'))
    ) {
      if (node.arguments.length >= 1 && ts.isStringLiteralLike(node.arguments[0])) {
        specifiers.add(node.arguments[0].text);
      } else if (computedSpecifiers) {
        // A computed specifier hides the loaded module from this literal-only
        // scan, so the caller must fail closed instead of trusting the graph.
        computedSpecifiers.add(node.getText(parsed).slice(0, 120));
      }
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
  'openai',
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
const nodeBuiltins = new Set(builtinModules.map(specifier => specifier.replace(/^node:/, '').split('/')[0]));
const exactTestEntries = new Set([
  'packages/core/src/harness/v1/session.real-agent.e2e.test.ts',
  'packages/server/src/server/handlers/favorites.integration.test.ts',
]);

function unsupportedModuleReason(specifier) {
  if (specifier === '@playwright/test' || specifier === 'testcontainers') return specifier;
  if (specifier.startsWith('@ai-sdk/')) return specifier;
  if (specifier.startsWith('@openai/')) return specifier;
  if (specifier === '@anthropic-ai/sdk' || specifier === '@google/generative-ai') return specifier;
  if (specifier === 'ollama' || specifier === 'openai') return specifier;
  if (specifier === '@mastra/openai' || specifier.startsWith('@mastra/openai/')) return specifier;
  if (specifier.startsWith('@mastra/voice-')) return specifier;
  if (specifier.startsWith('@mastra/')) {
    const packageName = specifier.slice('@mastra/'.length).split('/')[0];
    if (bannedMastraPackages.has(packageName)) return specifier;
  }
  const bareSpecifier = specifier.startsWith('node:') ? specifier.slice('node:'.length) : specifier;
  if (bannedBuiltins.has(bareSpecifier.split('/')[0])) return specifier;
  return undefined;
}

function approvedExactExternalSpecifier(specifier) {
  const bareSpecifier = specifier.startsWith('node:') ? specifier.slice('node:'.length) : specifier;
  return (
    specifier === 'vitest' ||
    specifier === 'zod' ||
    specifier.startsWith('zod/') ||
    specifier === '@mastra/core' ||
    specifier.startsWith('@mastra/core/') ||
    nodeBuiltins.has(bareSpecifier.split('/')[0])
  );
}

function propertyName(node) {
  if (ts.isPropertyAccessExpression(node)) return node.name.text;
  if (ts.isElementAccessExpression(node) && node.argumentExpression && ts.isStringLiteralLike(node.argumentExpression)) {
    return node.argumentExpression.text;
  }
  return undefined;
}

function isTypePosition(node) {
  const parent = node.parent;
  return Boolean(parent && (ts.isTypeReferenceNode(parent) || ts.isTypeQueryNode(parent)));
}

function isDeclaredNamePosition(node) {
  const parent = node.parent;
  if (!parent) return false;
  if (ts.isPropertyAccessExpression(parent) && parent.name === node) return true;
  if (ts.isPropertyAssignment(parent) && parent.name === node) return true;
  if (ts.isPropertySignature(parent) && parent.name === node) return true;
  if (ts.isMethodDeclaration(parent) && parent.name === node) return true;
  if (ts.isBindingElement(parent) && parent.propertyName === node) return true;
  return false;
}

// Aliasing any of these globals (const request = fetch, const { env } = process,
// const load = require) strips the name this literal scanner keys on, so every
// value-position reference outside the directly supported shapes fails closed.
function unsupportedRuntimeReasons(file, source) {
  const reasons = new Set();
  const wasReachableFromExactTest = baseGraph.has(repositoryPath(file));
  const baseSource = readBaseSource(file);
  const baseSpecifiers = baseSource ? runtimeModuleSpecifiers(file, baseSource) : new Set();
  const computedSpecifiers = new Set();
  for (const specifier of runtimeModuleSpecifiers(file, source, computedSpecifiers)) {
    const reason = unsupportedModuleReason(specifier);
    // A banned specifier that already existed in this file at the trusted
    // base commit is part of the reviewed production surface (e.g. a server
    // handler's SSRF-guarded fetch); editing unrelated lines of that file
    // must not retroactively reject it. Only NEWLY ADDED banned imports in
    // the changed surface fail closed.
    if (reason && !(baseSource !== undefined && baseSpecifiers.has(specifier))) {
      reasons.add(`module ${reason}`);
    }
    if (
      exactTestEntries.has(entryFile) &&
      !specifier.startsWith('.') &&
      (!wasReachableFromExactTest || !baseSpecifiers.has(specifier)) &&
      !approvedExactExternalSpecifier(specifier)
    ) {
      reasons.add(`unreviewed external module ${specifier}`);
    }
  }
  for (const occurrence of computedSpecifiers) {
    reasons.add(`computed module specifier ${occurrence}`);
  }
  const parsed = sourceFile(file, source);
  const flagGlobalReference = (node, name) => {
    if (isDeclaredNamePosition(node) || isTypePosition(node)) return;
    const parent = node.parent;
    if (name === 'process') {
      const isMemberReceiver =
        parent &&
        (ts.isPropertyAccessExpression(parent) || ts.isElementAccessExpression(parent)) &&
        parent.expression === node;
      if (isMemberReceiver) {
        if (propertyName(parent) === 'env') reasons.add('process.env');
        return;
      }
      reasons.add('process alias');
      return;
    }
    if (name === 'require') {
      if (parent && ts.isCallExpression(parent) && parent.expression === node) return;
      reasons.add('require alias');
      return;
    }
    if (name === 'createRequire') {
      reasons.add('createRequire()');
      return;
    }
    const isDirectCall =
      parent && (ts.isCallExpression(parent) || ts.isNewExpression(parent)) && parent.expression === node;
    reasons.add(isDirectCall ? `${name}()` : `${name} alias`);
  };
  const visit = node => {
    if (ts.isIdentifier(node) && ['process', 'require', 'createRequire', 'fetch', 'WebSocket'].includes(node.text)) {
      flagGlobalReference(node, node.text);
    } else if (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) {
      const accessedName = propertyName(node);
      if (['process', 'fetch', 'WebSocket'].includes(accessedName)) {
        // globalThis.process / globalThis['fetch'] and deeper receiver chains.
        flagGlobalReference(node, accessedName);
      }
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
    if [[ "$file" == browser/stagehand/src/__tests__/profile-lifecycle.test.ts ]] ||
      { [[ -f "$file" ]] && grep -Eq \
        '\.(test|spec)\.(ts|tsx|js|jsx|mjs|cjs|mts|cts)$|\.test-d\.ts$' \
        <<< "$file"; }; then
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
    elif [[ "$file" == browser/stagehand/src/__tests__/profile-lifecycle.test.ts ]]; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    elif [[ "$file" == e2e-tests/* || "$file" == */integration-tests/* || \
      "$file" == mastracode/e2e/* || \
      "$file" =~ \.e2e\.(test|spec)\.(ts|tsx|js|jsx|mjs|cjs|mts|cts)$ ]] || \
      grep -Eq "['\"]@playwright/test['\"]" "$file"; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    elif [[ "$file" == mastracode/* && \
      "$file" != mastracode/src/tui/components/login-dialog.test.ts && \
      "$file" != mastracode/src/tui/event-dispatch.test.ts && \
      "$file" != mastracode/src/tui/notify.test.ts && \
      "$file" != mastracode/src/utils/__tests__/signals-pubsub.test.ts ]]; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    elif [[ "$file" =~ integration\.(test|spec)\. && \
      "$file" != packages/cli/src/services/service.deps.integration.test.ts && \
      "$file" != packages/codemod/src/lib/transform.integration.test.ts && \
      "$file" != packages/deployer/src/deploy/log.integration.test.ts && \
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

if server_route_source_changed; then
  # Route generators prove artifact freshness; these canonical suites prove
  # the serving tuple remains collision-free and the public manifest remains
  # derivable from it even when the PR does not edit a test file.
  printf '%s\n' \
    'packages/server/src/server/server-adapter/schema-consistency.test.ts' \
    'packages/server/src/server/server-adapter/api-schema-manifest.test.ts' \
    >> "$changed_tests"
fi

if [[ -s "$forced_mastracode_tests" ]]; then
  echo "Forcing owned MastraCode suites to run for source-only changes:"
  sort -u "$forced_mastracode_tests"
  sort -u "$forced_mastracode_tests" >> "$changed_tests"
fi
sort -u -o "$changed_tests" "$changed_tests"

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
  ! grep -Eq '^stores/_test-utils/.*\.(test|spec)\.(ts|tsx|js|jsx|mjs|cjs|mts|cts)$' "$changed_tests"; then
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

select_changed_agent_signal_tests() {
  local file="$1"
  local output_file="$2"

  node - "$merge_base_sha" "$HEAD_SHA" "$file" > "$output_file" <<'NODE'
const { execFileSync } = require('node:child_process');
const ts = require('typescript');

const [baseSha, headSha, file] = process.argv.slice(2);
const readSource = sha => execFileSync('git', ['show', `${sha}:${file}`], { encoding: 'utf8' });
const diff = execFileSync('git', ['diff', '--unified=0', '--no-color', baseSha, headSha, '--', file], {
  encoding: 'utf8',
});

const oldRanges = [];
const newRanges = [];
for (const line of diff.split('\n')) {
  const match = /^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/.exec(line);
  if (!match) continue;
  const oldCount = match[2] === undefined ? 1 : Number(match[2]);
  const newCount = match[4] === undefined ? 1 : Number(match[4]);
  if (oldCount > 0) oldRanges.push([Number(match[1]), Number(match[1]) + oldCount - 1]);
  if (newCount > 0) newRanges.push([Number(match[3]), Number(match[3]) + newCount - 1]);
}

function rootIdentifier(expression) {
  let current = expression;
  while (ts.isPropertyAccessExpression(current) || ts.isCallExpression(current)) {
    current = current.expression;
  }
  return ts.isIdentifier(current) ? current.text : undefined;
}

function collectTests(source) {
  const sourceFile = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const tests = [];
  function visit(node) {
    if (ts.isCallExpression(node) && ['it', 'test'].includes(rootIdentifier(node.expression))) {
      const name = node.arguments[0];
      if (name && (ts.isStringLiteral(name) || ts.isNoSubstitutionTemplateLiteral(name))) {
        tests.push({
          name: name.text,
          start: sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile)).line + 1,
          end: sourceFile.getLineAndCharacterOfPosition(node.getEnd()).line + 1,
        });
      }
    }
    ts.forEachChild(node, visit);
  }
  visit(sourceFile);
  return tests;
}

const knownSharedFixtureTests = new Map([
  [
    'DeliverThenRejectRegistrationPubSub',
    ['does not reject a partially delivered registration until its enqueued stream is drained'],
  ],
  [
    'RejectFirstRunCompletedPubSub',
    ['removes a failed terminal without a drain waiter before the same run id is reused'],
  ],
]);

function collectKnownSharedFixtures(source) {
  const sourceFile = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const fixtures = [];
  function visit(node) {
    if (ts.isClassDeclaration(node) && node.name && knownSharedFixtureTests.has(node.name.text)) {
      fixtures.push({
        name: node.name.text,
        start: sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile)).line + 1,
        end: sourceFile.getLineAndCharacterOfPosition(node.getEnd()).line + 1,
      });
    }
    ts.forEachChild(node, visit);
  }
  visit(sourceFile);
  return fixtures;
}

function namesForRanges(source, tests, ranges, revision) {
  const names = new Set();
  const fixtures = collectKnownSharedFixtures(source);
  const sourceLines = source.split('\n');
  for (const [start, end] of ranges) {
    for (let line = start; line <= end; line += 1) {
      if ((sourceLines[line - 1] ?? '').trim() === '') continue;
      const containing = tests.filter(test => test.start <= line && test.end >= line);
      if (containing.length > 0) {
        containing.sort((left, right) => left.end - left.start - (right.end - right.start));
        names.add(containing[0].name);
        continue;
      }
      const fixture = fixtures.find(candidate => candidate.start <= line && candidate.end >= line);
      if (!fixture) {
        throw new Error(
          `${file} changes lines ${start}-${end} outside a named it()/test() case or owned shared fixture in ` +
            `${revision}; the fork validator cannot safely select coverage for this shared setup change.`,
        );
      }
      for (const testName of knownSharedFixtureTests.get(fixture.name) ?? []) names.add(testName);
    }
  }
  return names;
}

const baseSource = readSource(baseSha);
const headSource = readSource(headSha);
const baseTests = collectTests(baseSource);
const headTests = collectTests(headSource);
const headNameCounts = new Map();
for (const test of headTests) headNameCounts.set(test.name, (headNameCounts.get(test.name) ?? 0) + 1);
const headNames = new Set(headTests.map(test => test.name));
const selected = new Set(namesForRanges(headSource, headTests, newRanges, headSha));
for (const name of namesForRanges(baseSource, baseTests, oldRanges, baseSha)) {
  if (headNames.has(name)) selected.add(name);
}
for (const name of selected) {
  if (!headNames.has(name)) throw new Error(`Selected Agent signal test does not exist at ${headSha}: ${name}`);
  if (headNameCounts.get(name) !== 1) {
    throw new Error(
      `Selected Agent signal test name is not unique at ${headSha}: ${name}; ` +
        `found ${headNameCounts.get(name)} declarations.`,
    );
  }
}
if (selected.size === 0) {
  throw new Error(`No runnable changed test cases were selected for ${file}.`);
}

const escapeRegex = value => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
for (const name of [...selected].sort()) {
  process.stdout.write(`${name}\t${escapeRegex(name)}$\n`);
}
NODE
}

test_status=0
while IFS= read -r file; do
  status=0
  test_dir=""
  test_workspace=""
  search_dir="$(dirname "$file")"

  workspace_search_dir="$search_dir"
  while [[ "$workspace_search_dir" != "/" ]]; do
    if [[ "$workspace_search_dir" != "." && -f "$workspace_search_dir/package.json" && \
      "$workspace_search_dir" != */__fixtures__/* ]]; then
      test_workspace="$workspace_search_dir"
      break
    fi
    if [[ "$workspace_search_dir" == "." ]]; then
      break
    fi
    workspace_search_dir="$(dirname "$workspace_search_dir")"
  done

  while [[ "$search_dir" != "/" ]]; do
    if [[ -f "$search_dir/vitest.config.ts" || -f "$search_dir/vitest.config.mts" || \
      -f "$search_dir/vitest.config.js" || -f "$search_dir/vitest.config.mjs" || \
      -f "$search_dir/vitest.config.cjs" || -f "$search_dir/vite.config.ts" || \
      -f "$search_dir/vite.config.mts" || -f "$search_dir/vite.config.js" || \
      -f "$search_dir/vite.config.mjs" || -f "$search_dir/vite.config.cjs" ]]; then
      if [[ "$search_dir" == "." || -f "$search_dir/package.json" ]]; then
        test_dir="$search_dir"
        break
      fi
    fi
    if [[ "$search_dir" == "." ]]; then
      break
    fi
    search_dir="$(dirname "$search_dir")"
  done

  if [[ "$test_dir" == "." ]]; then
    test_dir=""
  fi

  if [[ "$file" == packages/core/src/agent/__tests__/agent-signals.test.ts ]]; then
    selected_tests="$(mktemp)"
    set +e
    select_changed_agent_signal_tests "$file" "$selected_tests"
    status=$?
    set -e
    if (( status != 0 )); then
      echo "Unable to select isolated Agent signal tests; failing closed." >&2
      rm -f "$selected_tests"
      test_status=$status
      continue
    fi

    while IFS=$'\t' read -r test_name test_pattern; do
      set +e
      run_isolated_vitest_case \
        "$test_workspace" "${file#"$test_workspace"/}" "$test_name" "$test_pattern"
      status=$?
      set -e
      if (( status != 0 )); then
        break
      fi
    done < "$selected_tests"
    rm -f "$selected_tests"
    if (( status != 0 )); then
      test_status=$status
    fi
    continue
  fi

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
    vitest_command=(pnpm exec vitest)
    vitest_file="$file"
    if [[ -n "$test_workspace" ]]; then
      vitest_command=(pnpm --dir "$test_workspace" exec vitest)
      vitest_file="${file#"$test_workspace"/}"
    fi
    timeout --kill-after=30s "${timeout_seconds}s" \
      "${vitest_command[@]}" run --typecheck.only --reporter=dot "$vitest_file"
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
    vitest_command=(pnpm exec vitest)
    vitest_file="$file"
    if [[ -n "$test_workspace" ]]; then
      vitest_command=(pnpm --dir "$test_workspace" exec vitest)
      vitest_file="${file#"$test_workspace"/}"
    fi
    timeout --kill-after=30s "${timeout_seconds}s" \
      "${vitest_command[@]}" run \
        --reporter=dot --reporter=json --outputFile.json="$test_result" \
        "$vitest_file"
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
