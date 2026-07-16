import { encodeThreadStateScope, ThreadStateStorage } from './base';
import type { ThreadStateKey } from './base';

function clone<T>(value: T): T {
  return value === undefined ? value : (structuredClone(value) as T);
}

/**
 * In-memory implementation of {@link ThreadStateStorage}.
 *
 * Holds each resource/thread scope in a `Map<scope, Map<type, value>>`.
 * Stored values are cloned on read and write so callers cannot mutate the
 * backing value.
 *
 * This is the default thread-state store wired by the composite store: task
 * tracking works out of the box without a configured backend. It is **not**
 * durable across process restarts — configure a durable backend (e.g.
 * `@mastra/libsql`) for state that must survive a restart.
 */
export class InMemoryThreadStateStorage extends ThreadStateStorage {
  private readonly stateByScope = new Map<string, Map<string, unknown>>();

  async init(): Promise<void> {
    // No-op for in-memory store.
  }

  async getState<T = unknown>(args: ThreadStateKey): Promise<T | undefined> {
    const value = this.stateByScope.get(encodeThreadStateScope(args))?.get(args.type);
    return value === undefined ? undefined : clone(value as T);
  }

  async setState<T = unknown>(args: ThreadStateKey & { value: T }): Promise<void> {
    const scope = encodeThreadStateScope(args);
    let byType = this.stateByScope.get(scope);
    if (!byType) {
      byType = new Map<string, unknown>();
      this.stateByScope.set(scope, byType);
    }
    byType.set(args.type, clone(args.value));
  }

  async deleteState(args: ThreadStateKey): Promise<void> {
    const scope = encodeThreadStateScope(args);
    const byType = this.stateByScope.get(scope);
    if (!byType) return;
    byType.delete(args.type);
    if (byType.size === 0) this.stateByScope.delete(scope);
  }

  async dangerouslyClearAll(): Promise<void> {
    this.stateByScope.clear();
  }
}
