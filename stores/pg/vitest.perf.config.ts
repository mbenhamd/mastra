import { defineConfig } from 'vitest/config';

const includePgPerfIntegration = process.env.MASTRA_RUN_PG_PERF_INTEGRATION === 'true';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['src/**/*.performance.test.ts', 'src/**/performance-indexes/*.test.ts'],
    exclude: includePgPerfIntegration ? [] : ['src/**/performance-indexes/*.integration.test.ts'],
  },
});
