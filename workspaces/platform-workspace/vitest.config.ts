import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    typecheck: {
      include: ['src/template-compatibility.test.ts'],
      tsconfig: './tsconfig.type-tests.json',
    },
  },
});
