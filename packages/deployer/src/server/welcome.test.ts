import { describe, expect, it } from 'vitest';

import { welcomeHtml } from './welcome';

describe('welcomeHtml', () => {
  it.each([
    { input: undefined, expectedPrefix: '/api' },
    { input: '/custom/v1/', expectedPrefix: '/custom/v1' },
  ])('normalizes $input to $expectedPrefix', ({ input, expectedPrefix }) => {
    const html = welcomeHtml(input);

    expect(html).toContain(`<span class="endpoint-path">${expectedPrefix}/agents</span>`);
  });

  it('normalizes leading and trailing API prefix slashes', () => {
    const html = welcomeHtml('////custom/v1////');

    expect(html).toContain('data-url="custom/v1/agents"');
    expect(html).toContain('<span class="endpoint-path">/custom/v1/agents</span>');
    expect(html).not.toContain('/custom/v1//agents');
  });

  it('normalizes adversarially long slash runs', () => {
    const slashRun = '/'.repeat(100_000);
    const html = welcomeHtml(`${slashRun}custom/v1${slashRun}`);

    expect(html).toContain('data-url="custom/v1/agents"');
    expect(html).toContain('<span class="endpoint-path">/custom/v1/agents</span>');
  });

  it('preserves an adversarially long internal slash run', () => {
    const slashRun = '/'.repeat(20_000);
    const prefix = `custom/${slashRun}v1`;
    const html = welcomeHtml(prefix);

    expect(html).toContain(`data-url="${prefix}/agents"`);
    expect(html).toContain(`<span class="endpoint-path">/${prefix}/agents</span>`);
  });
});
