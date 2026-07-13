#!/usr/bin/env bash

set -euo pipefail

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

git diff --name-only --diff-filter=ACMRTD "${BASE_SHA}...${HEAD_SHA}" | sort > "$changed_files"

echo "Changed files:"
cat "$changed_files"

while IFS= read -r file; do
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
    auth/okta | packages/_internal-core | packages/core | packages/deployer | packages/mcp | packages/memory | client-sdks/ai-sdk | stores/pg | stores/redis | mastracode | docs) ;;
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

mapfile -t detected_tests < <(
  while IFS= read -r file; do
    if [[ -f "$file" ]] && grep -Eq \
      '\.(test|spec)\.(ts|tsx|js|jsx|mjs|cjs)$|\.test-d\.ts$' \
      <<< "$file"; then
      printf '%s\n' "$file"
    fi
  done < "$changed_files"
)

if (( ${#detected_tests[@]} > 0 )); then
  for file in "${detected_tests[@]}"; do
    if [[ "$file" == docs/* ]] && grep -Eq "['\"]@playwright/test['\"]" "$file"; then
      printf '%s\n' "$file" >> "$delegated_docs_tests"
    elif [[ "$file" == e2e-tests/* || "$file" == */integration-tests/* || \
      "$file" =~ \.e2e\.(test|spec)\.(ts|tsx|js|jsx|mjs|cjs)$ ]] || \
      grep -Eq "['\"]@playwright/test['\"]" "$file"; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    elif [[ "$file" =~ integration\.(test|spec)\. && \
      "$file" != stores/pg/* && "$file" != stores/redis/* ]]; then
      printf '%s\n' "$file" >> "$unsupported_tests"
    elif [[ "$file" == stores/* && "$file" != stores/pg/* && "$file" != stores/redis/* ]]; then
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
