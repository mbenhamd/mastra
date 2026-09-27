/**
 * PF-4446: pure pattern tests colocated with the fetch compiler.
 *
 * Deliberately dependency-free (vitest + `./fetch-pattern` only): the
 * fork's fail-closed test gate scans the full transitive closure of new
 * tests, and only this pure module passes it. The behavioral battery
 * (unit + shared adapter-suite parity) lives with the downstream
 * consumer. These pins cover what P3's `Bun.serve({ routes })` host
 * depends on: exact matching, param extraction, and Bun-key shape.
 */
import { describe, expect, it } from 'vitest';

import { canonicalizeBunRouteKey, compileFetchRoutePattern, isStaticRoutePattern } from './fetch-pattern';

describe('compileFetchRoutePattern', () => {
  it('matches static paths exactly', () => {
    const pattern = compileFetchRoutePattern('/agents');
    expect(pattern.match('/agents')).toEqual({});
    expect(pattern.match('/agents/')).toBeNull();
    expect(pattern.match('/Agents')).toBeNull();
    expect(pattern.match('/agents/1')).toBeNull();
  });

  it('extracts single and multiple params with decoding', () => {
    const single = compileFetchRoutePattern('/agents/:agentId');
    expect(single.match('/agents/abc')).toEqual({ agentId: 'abc' });
    expect(single.match('/agents/a%20b')).toEqual({ agentId: 'a b' });
    expect(single.match('/agents/a/b')).toBeNull();

    const multi = compileFetchRoutePattern('/stored/agents/:agentId/versions/:versionId/activate');
    expect(multi.match('/stored/agents/a1/versions/v2/activate')).toEqual({ agentId: 'a1', versionId: 'v2' });
  });

  it('escapes regex syntax in static segments', () => {
    const dotted = compileFetchRoutePattern('/.well-known/:agentId/agent-card.json');
    expect(dotted.match('/.well-known/a1/agent-card.json')).toEqual({ agentId: 'a1' });
    expect(dotted.match('/.well-known/a1/agent-cardXjson')).toBeNull();

    const plus = compileFetchRoutePattern('/c++/info');
    expect(plus.match('/c++/info')).toEqual({});
    expect(plus.match('/c/info')).toBeNull();
  });
});

describe('canonicalizeBunRouteKey', () => {
  it('collapses shape-identical patterns regardless of param names', () => {
    expect(canonicalizeBunRouteKey('/agents/:agentId/generate')).toBe('/agents/:p0/generate');
    expect(canonicalizeBunRouteKey('/agents/:id/generate')).toBe('/agents/:p0/generate');
    expect(canonicalizeBunRouteKey('/stored/agents/:agentId/versions/:versionId/activate')).toBe(
      '/stored/agents/:p0/versions/:p1/activate',
    );
  });

  it('leaves static paths untouched', () => {
    expect(canonicalizeBunRouteKey('/agents')).toBe('/agents');
    expect(canonicalizeBunRouteKey('/')).toBe('/');
  });

  it('emits valid Bun :param segments only', () => {
    for (const path of ['/agents/:agentId', '/workflows/:workflowId', '/memory/threads/:threadId']) {
      for (const segment of canonicalizeBunRouteKey(path).split('/')) {
        expect(segment === '' || !segment.startsWith(':') || /^:[A-Za-z0-9_]+$/.test(segment)).toBe(true);
      }
    }
  });
});

describe('isStaticRoutePattern', () => {
  it('distinguishes static from param patterns', () => {
    expect(isStaticRoutePattern('/agents')).toBe(true);
    expect(isStaticRoutePattern('/')).toBe(true);
    expect(isStaticRoutePattern('/agents/:agentId')).toBe(false);
  });
});
