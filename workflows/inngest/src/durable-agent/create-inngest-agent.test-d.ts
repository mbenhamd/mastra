import { describe, expectTypeOf, it } from 'vitest';

import type { InngestAgentResumeOptions, InngestAgentStreamOptions } from './create-inngest-agent';

describe('Inngest durable-agent option contracts', () => {
  it('scopes the monotonic permission requirement to resume options', () => {
    const resumeOptions = {
      requireToolPermissionPolicy: true,
    } satisfies InngestAgentResumeOptions;
    expectTypeOf(resumeOptions.requireToolPermissionPolicy).toEqualTypeOf<true>();

    const streamOptions: InngestAgentStreamOptions = {};
    // @ts-expect-error - this is a resume-only monotonic requirement
    streamOptions.requireToolPermissionPolicy = true;
  });
});
