import { MastraBase } from '../../../base';
import type { PruneOptions, PruneResult, RetentionTablesDescriptor, TableRetentionPolicy } from '../../retention';

/**
 * A single task in an agent's structured task list.
 *
 * Mirrors the task shape used by the built-in task tools. Kept as a plain,
 * self-contained type so the storage domain does not depend on the tools
 * package.
 */
export interface TaskRecord {
  id: string;
  content: string;
  status: 'pending' | 'in_progress' | 'completed';
  activeForm: string;
}

/**
 * A durable goal objective for an agent thread.
 *
 * Stored in the thread-state domain under `type: 'goal'`. The objective drives
 * the in-loop goal scorer (the agent keeps working until the goal is judged
 * complete or the run budget is exhausted). Goal settings are optional: when
 * absent here they fall back to the Agent's `goal` config at read time, so an
 * objective only persists the settings a caller explicitly provided.
 * `activeDurationMs` is persisted accounting data rather than a goal setting;
 * when absent, consumers treat it as zero. `judgeModelId` is required at runtime
 * for the goal to do anything — when neither this record nor the Agent's `goal.judge`
 * resolves a judge model, the goal step is a no-op.
 */
export interface GoalObjectiveRecord {
  /** Stable objective id, used for per-goal judge memory and UI correlation. */
  id?: string;
  /** The prose objective the agent is working toward. */
  objective: string;
  status: 'active' | 'paused' | 'done';
  /** Number of goal evaluations consumed so far. */
  runsUsed: number;
  /** Accumulated active-pursuit time in milliseconds. Missing values represent zero. */
  activeDurationMs?: number;
  /** Max evaluations before the goal stops. Falls back to agent `goal.maxRuns` (default 50). */
  maxRuns?: number;
  /** Judge model id. Falls back to agent `goal.judge`; if neither resolves the goal is a no-op. */
  judgeModelId?: string;
  /** Extra judge guidance. Falls back to agent `goal.prompt` (default = built-in goal judge prompt). */
  prompt?: string;
  /**
   * Why the objective is parked (`status === 'paused'`). Set for judge failure
   * or budget exhaustion. Unset for `active`/`done`.
   */
  pausedReason?: string;
  startedAt: number;
  updatedAt: number;
}

/** Coordinates one independent durable state slot. */
export interface ThreadStateKey {
  /** Memory resource that owns the thread. */
  resourceId: string;
  /** Memory thread within the resource. */
  threadId: string;
  /** Application-defined state namespace, for example `task` or `goal`. */
  type: string;
}

/** Result of an atomic thread-state mutation. */
export type ThreadStateMutation<T, TResult> =
  | { operation: 'set'; value: T; result: TResult }
  | { operation: 'delete'; result: TResult }
  | { operation: 'keep'; result: TResult };

/**
 * Encode the resource and thread as one collision-free physical key.
 *
 * Existing storage tables historically called this column `threadId` and use
 * it in their primary key. Length-prefixing lets adapters add resource
 * isolation without an ambiguous delimiter or a destructive table migration.
 */
export function encodeThreadStateScope({ resourceId, threadId }: Pick<ThreadStateKey, 'resourceId' | 'threadId'>) {
  return `v1:${resourceId.length}:${resourceId}${threadId}`;
}

/**
 * Abstract base class for the thread-state storage domain.
 *
 * The thread-state domain holds arbitrary, durable, per-thread state keyed by a
 * `type` namespace. Each `(resourceId, threadId, type)` tuple owns one value.
 * Today the only types are `'task'` (the structured task list managed by the
 * built-in task tools) and `'goal'` (the durable {@link GoalObjectiveRecord}
 * that drives the in-loop goal scorer). The domain is intentionally generic so
 * other agent-scoped state can be tracked the same way without a new domain.
 *
 * The built-in task tools read/write the `'task'` slot synchronously within a
 * run (so a `task_update` sees the tasks a prior `task_write` produced), and the
 * task state processor reads it to project the list onto the agent state-signal
 * lane.
 */
export abstract class ThreadStateStorage extends MastraBase {
  private readonly mutationTails = new Map<string, Promise<void>>();

  /**
   * Declares which of this domain's tables are eligible for age-based retention.
   * Adapters that support retention override this; the default is empty.
   */
  static readonly retentionTables: RetentionTablesDescriptor = {};

  constructor() {
    super({
      component: 'STORAGE',
      name: 'THREAD_STATE',
    });
  }

  /**
   * Delete rows older than each policy's `maxAge`, batched, bounded, and
   * cancellable. Default implementation is a no-op (retention not supported).
   */
  async prune(_policies: Record<string, TableRetentionPolicy>, _options?: PruneOptions): Promise<PruneResult[]> {
    return [];
  }

  /**
   * Initialize the thread-state store (create tables, indexes, etc).
   */
  abstract init(): Promise<void>;

  /**
   * Get the state value for a `(resourceId, threadId, type)` tuple. Returns
   * `undefined` when no value has been set.
   */
  abstract getState<T = unknown>(args: ThreadStateKey): Promise<T | undefined>;

  /**
   * Set the state value for a `(resourceId, threadId, type)` tuple. Full-replacement
   * semantics: the stored value becomes exactly `value`.
   */
  abstract setState<T = unknown>(args: ThreadStateKey & { value: T }): Promise<void>;

  /**
   * Delete the state value for a `(resourceId, threadId, type)` tuple.
   */
  abstract deleteState(args: ThreadStateKey): Promise<void>;

  /**
   * Atomically read, transform, and optionally replace one state slot.
   *
   * The base implementation serializes mutations per slot within this storage
   * instance. Durable multi-process adapters override this method with their
   * database's transaction/locking primitive. The mutator is intentionally
   * synchronous so an adapter never holds a database transaction across
   * arbitrary asynchronous work. Adapters using optimistic concurrency may
   * invoke it more than once, so it must be deterministic and free of external
   * side effects.
   */
  async mutateState<T = unknown, TResult = void>(
    args: ThreadStateKey & {
      mutate: (current: T | undefined) => ThreadStateMutation<T, TResult>;
    },
  ): Promise<TResult> {
    const slot = `${encodeThreadStateScope(args)}\0${args.type.length}:${args.type}`;
    const previous = this.mutationTails.get(slot) ?? Promise.resolve();
    let release!: () => void;
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    const tail = previous.catch(() => {}).then(() => gate);
    this.mutationTails.set(slot, tail);

    await previous.catch(() => {});
    try {
      const current = await this.getState<T>(args);
      const mutation = args.mutate(current);
      if (mutation.operation === 'set') {
        await this.setState({ ...args, value: mutation.value });
      } else if (mutation.operation === 'delete') {
        await this.deleteState(args);
      }
      return mutation.result;
    } finally {
      release();
      if (this.mutationTails.get(slot) === tail) {
        this.mutationTails.delete(slot);
      }
    }
  }

  /**
   * Delete all thread state. Used for testing.
   */
  abstract dangerouslyClearAll(): Promise<void>;
}
