import { describe, expect, it } from 'vitest';

import {
  consumeBuilderValidatedInput,
  consumeBuilderValidatedSuspend,
  markBuilderValidatedInput,
  markBuilderValidatedSuspend,
} from './builder-validation-context';
import * as tools from './index';

describe('builder validation context', () => {
  it('does not expose validation bypass helpers from the public tools entrypoint', () => {
    expect(tools).not.toHaveProperty('markBuilderValidatedInput');
    expect(tools).not.toHaveProperty('consumeBuilderValidatedInput');
    expect(tools).not.toHaveProperty('markBuilderValidatedSuspend');
    expect(tools).not.toHaveProperty('consumeBuilderValidatedSuspend');
  });

  it('consumes builder validation state once', () => {
    const context = {};

    expect(consumeBuilderValidatedInput(context)).toBe(false);

    markBuilderValidatedInput(context);

    expect(consumeBuilderValidatedInput(context)).toBe(true);
    expect(consumeBuilderValidatedInput(context)).toBe(false);
  });

  it('consumes builder suspension validation state once', () => {
    const context = {};

    expect(consumeBuilderValidatedSuspend(context)).toBe(false);
    markBuilderValidatedSuspend(context);
    expect(consumeBuilderValidatedSuspend(context)).toBe(true);
    expect(consumeBuilderValidatedSuspend(context)).toBe(false);
  });
});
