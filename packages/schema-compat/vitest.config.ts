import { defineConfig } from 'vitest/config';

// This test makes the lazy validator import fail, so it needs a process where no other file has loaded it.
const BLOB_WORKER_TEST = 'src/standard-schema/adapters/json-schema-blob-worker.test.ts';
// This test needs a process that disallows code generation from strings before anything is imported.
const NO_CODEGEN_TEST = 'src/standard-schema/adapters/json-schema-no-codegen.test.ts';
const FRESH_PROCESS_TESTS = [BLOB_WORKER_TEST, NO_CODEGEN_TEST];

export default defineConfig({
  test: {
    environment: 'node',
    projects: [
      {
        // Both need a process that disallows code generation from strings before anything is
        // imported, and Node caches the external @cfworker/json-schema module per process, so
        // each runs in a fresh fork.
        test: {
          name: 'unit:packages/schema-compat:no-codegen',
          environment: 'node',
          include: FRESH_PROCESS_TESTS,
          pool: 'forks',
          isolate: true,
          execArgv: ['--disallow-code-generation-from-strings'],
        },
      },
      {
        test: {
          name: 'unit:packages/schema-compat:v4',
          environment: 'node',
          include: ['src/**/*.test.ts'],
          exclude: ['src/**/*-v3.test.ts', ...FRESH_PROCESS_TESTS],
          isolate: false,
        },
      },
      {
        test: {
          name: 'unit:packages/schema-compat:v3',
          environment: 'node',
          include: ['src/**/*.test.ts'],
          exclude: [
            'src/**/*-v4.test.ts',
            // Exclude provider-compats tests from v3 since they have snapshot tests
            // that produce different output between v3 (zod-to-json-schema) and v4 (native toJSONSchema)
            'src/provider-compats/*.test.ts',
            ...FRESH_PROCESS_TESTS,
          ],
          isolate: false,
        },
        resolve: {
          alias: {
            // Alias 'zod' to 'zod-v4' so all imports resolve to the same v4 package
            zod: 'zod-v3',
          },
        },
      },
    ],
  },
});
