/**
 * The first load of the shared QuickJS module spends the run's budget. These
 * tests stall that load and check that the run still ends at its deadline or
 * abort. They live in their own file because they replace `getQuickJS`.
 */

import type { CodeModeToolResult } from '@mastra/core/tools';
import { describe, expect, it, vi } from 'vitest';
import { QuickJsCodeModeTransport } from './transport';

vi.mock('quickjs-emscripten', async importOriginal => ({
  ...(await importOriginal<typeof import('quickjs-emscripten')>()),
  getQuickJS: () => new Promise(() => {}),
}));

function run(options: { timeout: number; abortSignal?: AbortSignal }): Promise<CodeModeToolResult> {
  return new QuickJsCodeModeTransport().run({
    program: `return 1;`,
    toolIds: [],
    dispatch: async () => undefined,
    timeout: options.timeout,
    abortSignal: options.abortSignal,
  });
}

describe('QuickJsCodeModeTransport first module load', () => {
  it('times out when the first module load stalls', async () => {
    const result = await run({ timeout: 50 });
    expect(result).toEqual({
      success: false,
      logs: [],
      error: { message: 'Code Mode execution timed out after 50ms', name: 'TimeoutError' },
    });
  });

  it('returns an AbortError result when the run is aborted during a stalled first module load', async () => {
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 20);
    const result = await run({ timeout: 5_000, abortSignal: controller.signal });
    expect(result).toEqual({
      success: false,
      logs: [],
      error: { message: 'Code Mode execution aborted', name: 'AbortError' },
    });
  });
});
