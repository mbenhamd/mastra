/**
 * Session workspace access (HARNESS_V1_SPEC.md §2.7 / §4.2 / §6.1).
 *
 * Owns the session's cached workspace handle, the single-flight resolution
 * promise and the lost-across-restart marker, and resolves the workspace
 * through the harness workspace registry. It reaches the session only through
 * {@link WorkspaceSessionPort}, which `Session` builds in `_workspacePort()`.
 */

import type { SessionRecord } from '../../storage/domains/harness';
import type { Workspace } from '../../workspace';
import { HarnessValidationError, HarnessWorkspaceLostError } from './errors';
import type { Harness } from './harness';

/**
 * Minimal slice of `Session` the workspace needs. `record` and
 * `subagentInheritWorkspace` are read through getters because `Session`
 * reassigns them after construction.
 */
export interface WorkspaceSessionPort {
  readonly id: string;
  readonly resourceId: string;
  readonly parentSessionId: string | undefined;
  readonly harness: Harness;
  readonly record: SessionRecord;
  readonly subagentInheritWorkspace: boolean | undefined;
  assertLive(method: string): void;
  flushUpdate(update: (prev: SessionRecord) => SessionRecord): Promise<void>;
}

export class SessionWorkspace {
  constructor(private readonly _port: WorkspaceSessionPort) {}

  /** Cached workspace handle. Resolves lazily on first `getWorkspace()` call. */
  private _workspace?: Workspace;
  /** Dedup promise so concurrent `getWorkspace()` calls share one provision attempt. */
  private _workspaceResolving?: Promise<Workspace>;
  /**
   * True when a non-resumable per-session workspace was found in storage on
   * rehydrate. Set by `_publish` via {@link _markWorkspaceLost}. The first
   * `getWorkspace()` call throws {@link HarnessWorkspaceLostError}; callers
   * can drop the marker and reprovision by calling `clearWorkspaceLost()`.
   */
  private _workspaceLost = false;

  // -------------------------------------------------------------------------
  // Workspace — §2.7 / §4.2.
  // -------------------------------------------------------------------------

  /**
   * Resolve this session's workspace. Returns `undefined` when the harness
   * has no workspace configured. Caches the result for the lifetime of the
   * Session (the workspace is released on `close()` per the ownership model).
   *
   * Throws {@link HarnessWorkspaceLostError} when the session's
   * `per-session` workspace was provisioned by a non-resumable provider
   * and a process restart has dropped the underlying state. Callers can
   * decide whether to surface the error or call `clearWorkspaceLost()` and
   * try again with a fresh workspace.
   */
  async getWorkspace(): Promise<Workspace | undefined> {
    this._port.assertLive('getWorkspace()');
    return this._getWorkspaceUnchecked();
  }

  // ---------------------------------------------------------------------------
  // §4.2 / §6.1 workspace access primitives. These mirror the tool-facing
  // `HarnessRequestContext` accessors. The split lets a turn avoid cloud-sandbox
  // cold starts: `hasWorkspace()`/`isWorkspaceReady()`/`peekWorkspace()` are
  // sync, NON-materializing reads; `resolveWorkspace()` is the explicit async
  // materialize/resume path. NOTE: the legacy `getWorkspace()` above is async +
  // materializing (kept for its established callers/tests); `resolveWorkspace()`
  // is its spec-named alias. (Migrating `getWorkspace()` itself to a sync read is
  // a separate, larger §4.2 reconciliation — ~25 call sites — left as follow-up.)
  // ---------------------------------------------------------------------------

  /** §6.1: is a workspace CONFIGURED for this harness (regardless of whether it
   * has been materialized)? Sync, non-materializing. */
  hasWorkspace(): boolean {
    return this._port.harness._workspaceKind !== undefined;
  }

  /** §6.1: is a usable workspace handle already materialized (warm/cached)? Sync,
   * non-materializing — `false` does not mean "no workspace", only "not yet
   * resolved" (or lost). A workspace marked lost is NOT ready even if a stale
   * handle lingers. */
  isWorkspaceReady(): boolean {
    return this._workspace !== undefined && !this._workspaceLost;
  }

  /** §6.1: the already-materialized workspace handle, or `undefined` if it has not
   * been resolved yet (or was lost). Sync and NON-materializing — never triggers a
   * cold start and never throws; `resolveWorkspace()` surfaces a lost workspace. */
  peekWorkspace(): Workspace | undefined {
    return this._workspaceLost ? undefined : this._workspace;
  }

  /** §6.1: explicitly materialize/resume the workspace (a cloud sandbox cold start
   * may happen here). Throws `HarnessWorkspaceLostError` if the workspace was lost
   * across restart, or a validation error if no workspace is configured. */
  async resolveWorkspace(): Promise<Workspace> {
    const workspace = await this.getWorkspace();
    if (workspace === undefined) {
      throw new HarnessValidationError(
        'resolveWorkspace()',
        'no workspace is configured for this harness (set `workspace.kind` in HarnessConfig)',
      );
    }
    return workspace;
  }

  async _getWorkspaceUnchecked(): Promise<Workspace | undefined> {
    if (this._workspace) return this._workspace;
    if (this._workspaceResolving) return this._workspaceResolving;

    const kind = this._port.harness._workspaceKind;
    if (!kind) return undefined;

    if (this._workspaceLost) {
      throw new HarnessWorkspaceLostError(this._port.id, {
        reason: 'restart',
        providerId: this._port.harness._workspaceRegistry.providerId,
      });
    }

    const resolve = async (): Promise<Workspace> => {
      if (kind === 'shared') {
        return this._port.harness._workspaceRegistry.acquireShared();
      }
      if (kind === 'per-resource') {
        return this._port.harness._workspaceRegistry.acquirePerResource({ resourceId: this._port.resourceId });
      }
      // kind === 'per-session'
      // Subagent sessions with `workspace: 'inherit'` reuse the parent's entry.
      // The spawn tool flips `_subagentFreshWorkspace` for the `fresh` case so
      // we don't accidentally inherit when a fresh workspace is requested.
      if (this._port.parentSessionId && this._port.subagentInheritWorkspace) {
        // §6.2 strict-lazy: a lazy parent turn may not have materialized its
        // workspace yet, but inheriting requires the parent's entry to exist.
        // Resolve the (live) parent's workspace first (idempotent).
        await this._port.harness._internalEnsureParentWorkspaceForInherit(this._port.parentSessionId);
        return this._port.harness._workspaceRegistry.inheritPerSession({
          parentSessionId: this._port.parentSessionId,
          childSessionId: this._port.id,
          resourceId: this._port.resourceId,
        });
      }
      const storedProviderId = this._port.record.workspace?.providerId;
      const storedState = this._port.record.workspace?.state;
      return this._port.harness._workspaceRegistry.acquirePerSession({
        resourceId: this._port.resourceId,
        sessionId: this._port.id,
        ...(this._port.parentSessionId ? { parentSessionId: this._port.parentSessionId } : {}),
        ...(storedProviderId ? { storedProviderId } : {}),
        ...(storedState !== undefined ? { storedState } : {}),
        onStateUpdate: async state => {
          await this._persistWorkspaceState(state);
        },
      });
    };

    this._workspaceResolving = resolve();
    try {
      this._workspace = await this._workspaceResolving;
      return this._workspace;
    } finally {
      this._workspaceResolving = undefined;
    }
  }

  /**
   * @internal — used by the harness during hydration when the stored record
   * carries workspace state but the configured provider is non-resumable.
   */
  _markWorkspaceLost(): void {
    this._workspaceLost = true;
  }

  /** @internal — writes the latest opaque workspace state into the session record. */
  private async _persistWorkspaceState(state: unknown): Promise<void> {
    const providerId = this._port.harness._workspaceRegistry.providerId;
    if (!providerId) return;
    await this._port.flushUpdate(record => ({
      ...record,
      workspace: { providerId, state },
    }));
  }

  /**
   * The cached handle exactly as stored, without the lost-marker check that
   * `peekWorkspace()` applies. Read by the skills/MCP/actions catalog port.
   */
  get cachedWorkspace(): Workspace | undefined {
    return this._workspace;
  }
}
