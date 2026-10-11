import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';

import { createTool } from '../../tools';
import { responseRecoveryToolChoice } from './response-recovery-tool-choice';

const clientTool = createTool({
  id: 'client_tool',
  description: 'A client-executed tool',
  inputSchema: z.object({}),
  execute: async () => ({}),
});
const providerTool = { type: 'provider-defined', id: 'openai.web_search', name: 'web_search', args: {} };

describe('responseRecoveryToolChoice', () => {
  it('keeps an unforced choice so the request carries the same tools and choice', () => {
    expect(responseRecoveryToolChoice('auto', { client_tool: clientTool }, undefined)).toBe('auto');
    expect(responseRecoveryToolChoice(undefined, { client_tool: clientTool }, undefined)).toBeUndefined();
    expect(responseRecoveryToolChoice('none', { client_tool: clientTool }, undefined)).toBe('none');
  });

  it('relaxes a forced choice so the model can reply', () => {
    expect(responseRecoveryToolChoice('required', { client_tool: clientTool }, undefined)).toBe('auto');
    expect(
      responseRecoveryToolChoice({ type: 'tool', toolName: 'client_tool' }, { client_tool: clientTool }, undefined),
    ).toBe('auto');
  });

  it('falls back to no tools when an exposed tool executes on the provider side', () => {
    const tools = { client_tool: clientTool, web_search: providerTool };
    expect(responseRecoveryToolChoice('auto', tools, undefined)).toBe('none');
    // A provider tool outside the active selection is not exposed.
    expect(responseRecoveryToolChoice('auto', tools, ['client_tool'])).toBe('auto');
  });
});
