import { defineConfig } from 'vitest/config';

export default defineConfig({
  root: import.meta.dirname,
  test: {
    name: 'scripts',
    environment: 'node',
    include: ['**/*.test.ts'],
  },
});
