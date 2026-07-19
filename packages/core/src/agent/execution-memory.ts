import { createRunScopeKey } from '../mastra/run-scope';
import type { MastraMemory } from '../memory/memory';

/**
 * Marks memory as resolved for one Agent execution.
 *
 * The wrapper intentionally distinguishes "resolved to undefined" from "not
 * resolved yet" so an Agent with a dynamic memory factory cannot accidentally
 * invoke that factory again on a downstream capability boundary.
 */
export interface ResolvedAgentMemory {
  readonly value: MastraMemory | undefined;
}

/**
 * Handle for a run-scoped memory handoff owned by its creator.
 *
 * `executionMemoryId` is the only value passed through Agent entrypoints. The
 * resolved memory instance remains in Mastra's process-local runScope.
 */
export interface ResolvedAgentMemoryHandoff {
  readonly executionMemoryId: string;
  get(): ResolvedAgentMemory | undefined;
  release(): void;
}

/**
 * Process-local registry for execution memory handoffs. The registry lives on
 * Mastra runScope; only its random string key is passed between Agent methods.
 */
export const RESOLVED_AGENT_MEMORY_REGISTRY_KEY =
  createRunScopeKey<Map<string, ResolvedAgentMemory>>('agent:resolvedMemoryRegistry');
