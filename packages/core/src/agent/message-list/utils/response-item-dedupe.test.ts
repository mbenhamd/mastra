import { describe, expect, it } from 'vitest';

import { dedupeResponseProviderItemParts } from './response-item-metadata';

const reasoning = (itemId: string, text = 'thinking') => ({
  type: 'reasoning',
  text,
  providerOptions: { openai: { itemId } },
});

describe('dedupeResponseProviderItemParts (PF-2279)', () => {
  it('drops a later part repeating an earlier provider item id, first wins', () => {
    const messages = [
      { role: 'assistant', content: [reasoning('rs_dup'), { type: 'text', text: 'a' }] },
      { role: 'assistant', content: [reasoning('rs_dup', 'replayed'), { type: 'text', text: 'b' }] },
    ];

    const result = dedupeResponseProviderItemParts(messages);

    expect(result[0]?.content).toHaveLength(2);
    expect(result[1]?.content).toEqual([{ type: 'text', text: 'b' }]);
  });

  it('keeps distinct ids, id-less parts, and non-assistant messages untouched', () => {
    const user = { role: 'user', content: [{ type: 'text', text: 'q' }] };
    const messages = [
      user,
      { role: 'assistant', content: [reasoning('rs_a'), { type: 'text', text: 'x' }] },
      { role: 'assistant', content: [reasoning('rs_b'), { type: 'text', text: 'y' }] },
    ];

    const result = dedupeResponseProviderItemParts(messages);

    expect(result[0]).toBe(user);
    expect(result[1]?.content).toHaveLength(2);
    expect(result[2]?.content).toHaveLength(2);
  });

  it('dedupes across azure and openai namespaces carrying the same id', () => {
    const messages = [
      {
        role: 'assistant',
        content: [{ type: 'reasoning', text: 't', providerOptions: { azure: { itemId: 'rs_x' } } }],
      },
      {
        role: 'assistant',
        content: [{ type: 'reasoning', text: 't2', providerMetadata: { azure: { itemId: 'rs_x' } } }],
      },
    ];

    const result = dedupeResponseProviderItemParts(messages);

    expect(result[0]?.content).toHaveLength(1);
    expect(result[1]?.content).toHaveLength(0);
  });

  it('leaves string-content and part-free messages alone', () => {
    const messages = [
      { role: 'assistant', content: 'plain' },
      { role: 'system', content: 'sys' },
    ];
    expect(dedupeResponseProviderItemParts(messages)).toEqual(messages);
  });
});
