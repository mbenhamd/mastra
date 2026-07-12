#!/usr/bin/env bash

set -euo pipefail

: "${BASE_SHA:?BASE_SHA is required}"
: "${HEAD_SHA:?HEAD_SHA is required}"

changed_files="$(mktemp)"
changed_tests="$(mktemp)"
skipped_e2e_tests="$(mktemp)"
skipped_playwright_tests="$(mktemp)"
trap 'rm -f "$changed_files" "$changed_tests" "$skipped_e2e_tests" "$skipped_playwright_tests"' EXIT

git diff --name-only --diff-filter=ACMRTD "${BASE_SHA}...${HEAD_SHA}" | sort > "$changed_files"

echo "Changed files:"
cat "$changed_files"

mapfile -t prettier_files < <(
  while IFS= read -r file; do
    if [[ -f "$file" && "$file" =~ \.(cjs|css|js|json|jsx|md|mdx|mjs|ts|tsx|ya?ml)$ ]]; then
      printf '%s\n' "$file"
    fi
  done < "$changed_files"
)

if (( ${#prettier_files[@]} > 0 )); then
  pnpm exec prettier --check "${prettier_files[@]}"
fi

pnpm build:core
pnpm --filter @mastra/core check

if grep -qE '^(packages/memory/|pnpm-lock\.yaml$|pnpm-workspace\.yaml$|package\.json$)' "$changed_files"; then
  pnpm --filter @mastra/memory check
  pnpm --filter @mastra/memory build:lib
fi

if grep -qE '^(client-sdks/ai-sdk/|pnpm-lock\.yaml$|pnpm-workspace\.yaml$|package\.json$)' "$changed_files"; then
  pnpm --filter @mastra/ai-sdk exec tsc --noEmit
  pnpm --filter @mastra/ai-sdk build:lib
fi

if grep -qE '^(stores/pg/|pnpm-lock\.yaml$|pnpm-workspace\.yaml$|package\.json$)' "$changed_files"; then
  pnpm --filter @mastra/pg exec tsc --noEmit
  pnpm turbo build --filter ./stores/pg
fi

if grep -qE '^mastracode/' "$changed_files"; then
  pnpm run build:mastracode
  pnpm --filter ./mastracode run e2e:test -- --reporter=dot
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
    if [[ "$file" =~ \.e2e\.(test|spec)\.(ts|tsx|js|jsx|mjs|cjs)$ ]]; then
      printf '%s\n' "$file" >> "$skipped_e2e_tests"
    elif grep -Eq "['\"]@playwright/test['\"]" "$file"; then
      printf '%s\n' "$file" >> "$skipped_playwright_tests"
    else
      printf '%s\n' "$file" >> "$changed_tests"
    fi
  done
fi

if [[ -s "$skipped_e2e_tests" ]]; then
  echo "Skipping explicit E2E files in the secretless fork lane:"
  cat "$skipped_e2e_tests"
fi

if [[ -s "$skipped_playwright_tests" ]]; then
  echo "Skipping Playwright files in the Vitest selector; dedicated Playwright workflows must validate them:"
  cat "$skipped_playwright_tests"
fi

if [[ ! -s "$changed_tests" ]]; then
  echo "No changed test files detected. Build and type checks completed."
  exit 0
fi

echo "Changed test files:"
cat "$changed_tests"

test_status=0
while IFS= read -r file; do
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
      set +e
      timeout --kill-after=30s 10m \
        pnpm --dir "$test_dir" exec vitest typecheck --reporter=dot "$relative_file"
      status=$?
      set -e
    else
      test_diff="$(mktemp)"
      test_list="$(mktemp)"
      test_patterns="$(mktemp)"
      git diff --unified=0 "${BASE_SHA}...${HEAD_SHA}" -- "$file" > "$test_diff"

      set +e
      pnpm --dir "$test_dir" exec vitest list "$relative_file" \
        --json="$test_list" --includeTaskLocation
      list_status=$?
      set -e

      if (( list_status == 0 )); then
        node - "$test_diff" "$test_list" <<'NODE' > "$test_patterns"
const fs = require('node:fs');

const [diffPath, listPath] = process.argv.slice(2);
const addedLines = new Set();
let newLine = null;

for (const line of fs.readFileSync(diffPath, 'utf8').split('\n')) {
  const hunk = line.match(/^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@/);
  if (hunk) {
    newLine = Number(hunk[1]);
    continue;
  }

  if (newLine === null) continue;
  if (line.startsWith('+')) {
    addedLines.add(newLine);
    newLine += 1;
  } else if (line.startsWith('-') || line.startsWith('\\')) {
    // Removed lines and "no newline" markers do not advance the new file.
  } else if (line.startsWith(' ')) {
    newLine += 1;
  } else {
    newLine = null;
  }
}

const tests = JSON.parse(fs.readFileSync(listPath, 'utf8'));
const names = new Set(
  tests
    .filter(test => test.location && addedLines.has(test.location.line))
    .map(test => test.name),
);

for (const name of names) {
  // Vitest's list command displays suite separators as " > ", while
  // testNamePattern matches the runtime full name with spaces.
  const runtimeName = name.replaceAll(' > ', ' ');
  const escaped = runtimeName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  console.log(`^${escaped}$`);
}
NODE
      fi

      if [[ -s "$test_patterns" ]]; then
        status=0
        while IFS= read -r pattern; do
          echo "Running added or renamed test: $pattern"
          test_result="$(mktemp)"
          set +e
          timeout --kill-after=30s 10m \
            pnpm --dir "$test_dir" exec vitest run \
              --reporter=dot --reporter=json --outputFile.json="$test_result" \
              "$relative_file" -t "$pattern"
          pattern_status=$?
          set -e

          if (( pattern_status == 0 )); then
            set +e
            node - "$test_result" <<'NODE'
const fs = require('node:fs');
const result = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
if (result.numPassedTests < 1) {
  console.error('The selected test pattern did not execute a passing test.');
  process.exit(1);
}
NODE
            pattern_status=$?
            set -e
          fi

          rm -f "$test_result"
          if (( pattern_status != 0 )); then
            status=$pattern_status
          fi
        done < "$test_patterns"
      else
        echo "No added or renamed test declaration detected; running $file in full."
        test_result="$(mktemp)"
        set +e
        timeout --kill-after=30s 15m \
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

      rm -f "$test_diff" "$test_list" "$test_patterns"
    fi
  elif [[ "$file" == *.test-d.ts ]]; then
    set +e
    timeout --kill-after=30s 10m pnpm exec vitest typecheck --reporter=dot "$file"
    status=$?
    set -e
  else
    test_result="$(mktemp)"
    set +e
    timeout --kill-after=30s 15m \
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
