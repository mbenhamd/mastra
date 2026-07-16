import { describe, expect, it } from 'vitest';

import { MastraError } from '../../../error';
import type { RunRegistryEntry, SerializableToolHookPolicy } from '../types';
import { serializeDurableOptions } from './serialize-state';
import { assertDurableToolHookPolicyAvailable } from './tool-hook-policy';

function registryEntry(
  id: string,
  hooks: { beforeToolCall?: (...args: any[]) => any; afterToolCall?: (...args: any[]) => any },
): RunRegistryEntry {
  return {
    toolHookPolicy: { id, hooks },
  } as unknown as RunRegistryEntry;
}

function expectUnavailable(run: () => void): MastraError {
  try {
    run();
  } catch (error) {
    expect(error).toBeInstanceOf(MastraError);
    expect((error as MastraError).id).toBe('DURABLE_AGENT_TOOL_HOOK_POLICY_UNAVAILABLE');
    return error as MastraError;
  }

  throw new Error('Expected the durable tool-hook policy assertion to fail');
}

describe('assertDurableToolHookPolicyAvailable', () => {
  it('accepts the exact process-local policy recorded by the serialized marker', () => {
    const beforeToolCall = () => undefined;
    const afterToolCall = () => undefined;
    const serialized: SerializableToolHookPolicy = {
      kind: 'run-registry',
      id: 'policy-1',
      beforeToolCall: true,
      afterToolCall: true,
    };

    expect(() =>
      assertDurableToolHookPolicyAvailable({
        serialized,
        registryEntry: registryEntry(serialized.id, { beforeToolCall, afterToolCall }),
      }),
    ).not.toThrow();
  });

  it('rejects a serialized per-execution policy when the registry policy is missing', () => {
    expectUnavailable(() =>
      assertDurableToolHookPolicyAvailable({
        serialized: { kind: 'run-registry', id: 'policy-1', beforeToolCall: true, afterToolCall: false },
        registryEntry: {} as RunRegistryEntry,
      }),
    );
  });

  it('rejects a registry per-execution policy when the serialized marker is missing', () => {
    expectUnavailable(() =>
      assertDurableToolHookPolicyAvailable({
        registryEntry: registryEntry('policy-1', { beforeToolCall: () => undefined }),
      }),
    );
  });

  it('rejects a matching policy carried only by a placeholder registry entry', () => {
    const entry = registryEntry('policy-1', { beforeToolCall: () => undefined });
    entry.isPlaceholder = true;

    expectUnavailable(() =>
      assertDurableToolHookPolicyAvailable({
        serialized: { kind: 'run-registry', id: 'policy-1', beforeToolCall: true, afterToolCall: false },
        registryEntry: entry,
      }),
    );
  });

  it('rejects a stale registry policy identity', () => {
    expectUnavailable(() =>
      assertDurableToolHookPolicyAvailable({
        serialized: { kind: 'run-registry', id: 'current-policy', beforeToolCall: true, afterToolCall: false },
        registryEntry: registryEntry('stale-policy', { beforeToolCall: () => undefined }),
      }),
    );
  });

  it('rejects an unrecognized serialized marker discriminator', () => {
    expectUnavailable(() =>
      assertDurableToolHookPolicyAvailable({
        serialized: {
          kind: 'unknown-policy',
          id: 'policy-1',
          beforeToolCall: true,
          afterToolCall: false,
        } as unknown as SerializableToolHookPolicy,
        registryEntry: registryEntry('policy-1', { beforeToolCall: () => undefined }),
      }),
    );
  });

  it.each([
    {
      name: 'missing beforeToolCall callback',
      serialized: { kind: 'run-registry', id: 'policy-1', beforeToolCall: true, afterToolCall: false },
      hooks: {},
    },
    {
      name: 'unexpected beforeToolCall callback',
      serialized: { kind: 'run-registry', id: 'policy-1', beforeToolCall: false, afterToolCall: false },
      hooks: { beforeToolCall: () => undefined },
    },
    {
      name: 'missing afterToolCall callback',
      serialized: { kind: 'run-registry', id: 'policy-1', beforeToolCall: false, afterToolCall: true },
      hooks: {},
    },
    {
      name: 'unexpected afterToolCall callback',
      serialized: { kind: 'run-registry', id: 'policy-1', beforeToolCall: false, afterToolCall: false },
      hooks: { afterToolCall: () => undefined },
    },
  ] satisfies Array<{
    name: string;
    serialized: SerializableToolHookPolicy;
    hooks: { beforeToolCall?: (...args: any[]) => any; afterToolCall?: (...args: any[]) => any };
  }>)('rejects $name relative to the serialized flags', ({ serialized, hooks }) => {
    expectUnavailable(() =>
      assertDurableToolHookPolicyAvailable({
        serialized,
        registryEntry: registryEntry(serialized.id, hooks),
      }),
    );
  });

  it('allows agent-configured hooks when no per-execution marker or registry policy exists', () => {
    expect(() => assertDurableToolHookPolicyAvailable({ registryEntry: {} as RunRegistryEntry })).not.toThrow();
  });

  it('redacts serialized and registry identities, hook source, and unrelated secrets from failures', () => {
    function hookWithSecretSource() {
      return 'hook-source-secret';
    }

    const entry = registryEntry('registry-policy-secret-id', { beforeToolCall: hookWithSecretSource });
    (entry as unknown as { secret: string }).secret = 'registry-entry-secret';
    const error = expectUnavailable(() =>
      assertDurableToolHookPolicyAvailable({
        serialized: {
          kind: 'run-registry',
          id: 'serialized-policy-secret-id',
          beforeToolCall: true,
          afterToolCall: false,
        },
        registryEntry: entry,
      }),
    );
    const rendered = `${error.message}\n${error.stack ?? ''}\n${JSON.stringify(error)}`;

    expect(rendered).not.toContain('serialized-policy-secret-id');
    expect(rendered).not.toContain('registry-policy-secret-id');
    expect(rendered).not.toContain('hookWithSecretSource');
    expect(rendered).not.toContain('hook-source-secret');
    expect(rendered).not.toContain('registry-entry-secret');
    expect(error.details).toEqual({});
  });
});

describe('serializeDurableOptions tool-hook policy marker', () => {
  it('preserves only the JSON-safe marker contract used to recover process-local hooks', () => {
    const toolHookPolicy: SerializableToolHookPolicy = {
      kind: 'run-registry',
      id: 'policy-\"json-safe\"',
      beforeToolCall: true,
      afterToolCall: false,
    };
    const serialized = serializeDurableOptions({ toolHookPolicy });
    const json = JSON.stringify(serialized);

    expect(serialized.toolHookPolicy).toEqual(toolHookPolicy);
    expect(Object.keys(serialized.toolHookPolicy ?? {}).sort()).toEqual([
      'afterToolCall',
      'beforeToolCall',
      'id',
      'kind',
    ]);
    expect(JSON.parse(json).toolHookPolicy).toEqual(toolHookPolicy);
    expect(json).not.toContain('function');
  });

  it('omits the marker for agent-configured hooks with no per-execution override', () => {
    const serialized = serializeDurableOptions({});

    expect(serialized.toolHookPolicy).toBeUndefined();
    expect(JSON.stringify(serialized)).not.toContain('toolHookPolicy');
  });
});
