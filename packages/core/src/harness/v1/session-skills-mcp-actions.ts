/**
 * Session skills, MCP and action catalog (HARNESS_V1_SPEC.md §4.6 / §4.2c, PF-562, PF-576).
 *
 * Owns the per-session skill discovery cache, the action catalog caches and the
 * public `skills` / `mcp` / `actions` facade objects that `Session` exposes.
 * It reaches the session only through {@link SkillsMcpActionsSessionPort}, which
 * `Session` builds in `_skillsMcpActionsPort()`.
 */

import type { RequestContext } from '../../request-context';
import type { SessionRecord } from '../../storage/domains/harness';
import type { Workspace } from '../../workspace';
import { canonicalJson } from './canonical-json';
import {
  HarnessBusyError,
  HarnessSkillArgsValidationError,
  HarnessSkillNotFoundError,
  HarnessValidationError,
  HarnessWorkspaceLostError,
} from './errors';
import type { Harness } from './harness';
import { validateCallerRequestContext } from './request-context-input';
import {
  ACTION_CATALOG_DEFAULT_LIMIT,
  ACTION_CATALOG_MAX_LIMIT,
  ACTION_CATALOG_MCP_FAILURE_CACHE_MS,
  ACTION_CATALOG_MCP_MAX_TIMEOUT_RETRIES,
  ACTION_CATALOG_MCP_SUCCESS_CACHE_MS,
  ACTION_CATALOG_SOURCE_KINDS,
  ACTION_CATALOG_SOURCE_ORDER,
  ActionCatalogMcpListTimeoutError,
  cloneActionCatalogEntry,
  cloneActionMetadataLike,
  cloneMcpCatalogRecord,
  cloneMcpCatalogRecordArray,
  cloneMcpCatalogValue,
  cloneMcpSchemaLike,
  compareActionCatalogIds,
  encodeActionCatalogIdPart,
  RESERVED_MCP_SERVER_KEYS,
  startActionCatalogMcpListWithTimeout,
  SUPPORTED_SKILL_ARG_SCHEMA_KEYS,
} from './session-catalog';
import type {
  ActionCatalogMcpCacheEntry,
  ActionCatalogMcpTimedOutWork,
  ActionCatalogSkillDescriptor,
} from './session-catalog';
import type {
  AgentResult,
  HarnessActionCatalogEntry,
  HarnessActionCatalogListOptions,
  HarnessActionCatalogSourceKind,
  HarnessActionCatalogUnavailableReason,
  HarnessMcpServerDescriptor,
  HarnessMcpToolDescriptor,
  HarnessSkill,
  MessageOptionsDefault,
  UseSkillOptions,
} from './types';

/**
 * Minimal slice of `Session` the catalog needs. `record` and `workspace` are
 * read through getters because `Session` reassigns them.
 */
export interface SkillsMcpActionsSessionPort {
  readonly id: string;
  readonly harness: Harness;
  readonly record: SessionRecord;
  /** The already-materialized workspace handle; reading it never provisions one. */
  readonly workspace: Workspace | undefined;
  assertLive(method: string): void;
  isBusy(): boolean;
  busyReason(): 'in_flight' | 'pending_approval' | 'pending_suspension' | 'pending_question' | 'pending_plan';
  buildRequestContext(turn: {
    modeId: string;
    modelId: string;
    abortSignal: AbortSignal;
    resolveWorkspace?: boolean;
  }): Promise<RequestContext>;
  getWorkspace(): Promise<Workspace | undefined>;
  message(opts: MessageOptionsDefault): Promise<AgentResult>;
}

export class SessionSkillsMcpActions {
  constructor(private readonly _port: SkillsMcpActionsSessionPort) {}

  // -------------------------------------------------------------------------
  // Skill discovery cache (§4.6).
  //
  // Workspace skill discovery is async and lazy. We cache the merged code +
  // workspace catalog for the lifetime of this in-memory Session instance.
  // Concurrent `skills.list` / `skills.get` calls during a generation build
  // share the same in-flight promise (single-flight), avoiding duplicate
  // discovery work. `skills.refresh()` clears the cache so the next read
  // re-runs discovery through the workspace skill source.
  // -------------------------------------------------------------------------
  private _skillsCache?: HarnessSkill[];
  private _skillsResolving?: Promise<HarnessSkill[]>;
  private _actionsSkillEntriesCache?: HarnessActionCatalogEntry[];
  private _actionsSkillEntriesResolving?: Promise<HarnessActionCatalogEntry[]>;
  private _actionsMcpEntriesCacheByServer = new Map<string, ActionCatalogMcpCacheEntry>();
  private _actionsMcpEntriesResolvingByServer = new Map<string, Promise<HarnessActionCatalogEntry[]>>();
  private _actionsMcpTimedOutWorkByServer = new Map<string, ActionCatalogMcpTimedOutWork>();
  private _actionsMcpCatalogGeneration = 0;

  // -------------------------------------------------------------------------
  // Skills — §4.6.
  //
  // Code-registered skills are merged ahead of workspace-discovered skills.
  // Workspace-discovered entries are projected from the configured
  // `WorkspaceSkills` source into `HarnessSkill` descriptors. Discovery runs
  // asynchronously on the first `list` / `get` / `use` call per in-memory
  // Session instance and the merged result is cached for the session's
  // lifetime. Concurrent calls during a generation share a single-flight
  // promise. `refresh()` drops the cache so the next call re-runs discovery.
  //
  // `use(ref, opts?)` resolves a code-registered or workspace skill by name
  // or relative path, validates declared args, appends a JSON code block
  // carrying the validated args to the skill body, and delegates to the
  // signal-driven message path. The returned `AgentResult` is the underlying
  // turn's result.
  // -------------------------------------------------------------------------

  /**
   * Skill discovery, inspection, and programmatic execution — see §4.6
   * and §4.2c.
   *
   * Code-registered skills are merged ahead of workspace-discovered skills.
   * Workspace-discovered skills are projected into `HarnessSkill`
   * descriptors. Discovery runs asynchronously on the first `list` /
   * `get` / `use` call per in-memory Session instance and is cached for
   * the session's lifetime. Concurrent callers share a single-flight
   * promise. `refresh()` drops the cache so the next call re-runs
   * discovery.
   */
  readonly skills = Object.freeze({
    /**
     * List skills available to this session.
     *
     * Returns code-registered skills plus workspace-discovered skills.
     * If the session has no workspace configured, only code-registered
     * skills are returned.
     */
    list: (): Promise<HarnessSkill[]> => this._skillsList(),
    /**
     * Look up a skill by name. Returns `undefined` when the name does not
     * resolve in the code or workspace catalogues.
     */
    get: (name: string): Promise<HarnessSkill | undefined> => this._skillsGet(name),
    /**
     * Drop the cached workspace-discovery result. The next `list` / `get`
     * / `use` call re-runs discovery through the configured workspace
     * skill source. Local-only — absent from `RemoteSession` (§13.5).
     */
    refresh: (): Promise<void> => this._skillsRefresh(),
    /**
     * Resolve a code-registered skill by name, or a workspace skill by name
     * or relative path, optionally validate provided arguments against the
     * skill's declared args schema, append a JSON code block of the validated
     * args to the skill instructions, and dispatch the result
     * through the signal-driven message path as a single turn. Resolves
     * to the underlying turn's `AgentResult`.
     *
     * Throws {@link HarnessSkillNotFoundError} when `ref` does not match
     * any skill, and {@link HarnessSkillArgsValidationError} when declared
     * args are invalid.
     */
    use: (ref: string, opts?: UseSkillOptions): Promise<AgentResult> => this._skillsUse(ref, opts),
  });

  // -------------------------------------------------------------------------
  // MCP catalog — PF-562 / PF-552 desktop integration inventory.
  //
  // This is an inventory snapshot over MCP servers registered on the Harness
  // Mastra instance. Tool descriptors use MCPServerBase.getToolListInfo() so
  // lazy MCP client proxies can populate remote tool catalogs on demand.
  // Execution permission remains enforced by the MCP tool runtime; this
  // catalog only lets desktop hosts render integrations.
  // -------------------------------------------------------------------------

  readonly mcp = Object.freeze({
    /** List MCP servers registered on the Harness Mastra instance. */
    listServers: (): HarnessMcpServerDescriptor[] => this._mcpListServers(),
    /** Look up one MCP server by Mastra registration key. */
    getServer: (key: string): HarnessMcpServerDescriptor | undefined => this._mcpGetServer(key),
    /** List MCP tool descriptors for one registered server key. */
    listTools: (key: string): Promise<HarnessMcpToolDescriptor[] | undefined> => {
      this._port.assertLive('mcp.listTools()');
      this._assertMcpServerKey('mcp.listTools()', key);
      return this._mcpListTools(key);
    },
  });

  // -------------------------------------------------------------------------
  // Action catalog — PF-576 / PF-552 desktop palette inventory.
  //
  // This is a read-only aggregate over skill action metadata and MCP tool
  // descriptors. It intentionally exposes no execution or lifecycle controls;
  // callers use the source surfaces referenced by each entry.
  // -------------------------------------------------------------------------

  readonly actions = Object.freeze({
    /** List/search local desktop action catalog entries. */
    list: (options?: HarnessActionCatalogListOptions): Promise<HarnessActionCatalogEntry[]> =>
      this._actionsList(options),
    /** Convenience wrapper for `list({ query, ...options })`. */
    search: (
      query: string,
      options?: Omit<HarnessActionCatalogListOptions, 'query'>,
    ): Promise<HarnessActionCatalogEntry[]> => this._actionsSearch(query, options),
    /** Refresh already-materialized workspace skills and clear cached action catalog discovery. */
    refresh: (): Promise<void> => this._actionsRefresh(),
  });

  private _mcpListServers(): HarnessMcpServerDescriptor[] {
    this._port.assertLive('mcp.listServers()');
    return this._port.harness._listMcpServers().map(([key, server]) => this._projectMcpServer(key, server));
  }

  private _mcpGetServer(key: string): HarnessMcpServerDescriptor | undefined {
    this._port.assertLive('mcp.getServer()');
    this._assertMcpServerKey('mcp.getServer()', key);
    const server = this._port.harness._getMcpServer(key);
    return server ? this._projectMcpServer(key, server) : undefined;
  }

  private async _mcpListTools(
    key: string,
    abortSignal: AbortSignal = new AbortController().signal,
    opts: { resolveWorkspace?: boolean } = {},
  ): Promise<HarnessMcpToolDescriptor[] | undefined> {
    const server = this._port.harness._getMcpServer(key);
    if (!server) return undefined;
    const requestContext = await this._port.buildRequestContext({
      modeId: this._port.record.modeId,
      modelId: this._port.record.modelId,
      abortSignal,
      resolveWorkspace: opts.resolveWorkspace,
    });
    const toolList = await server.getToolListInfo(requestContext);
    const convertedTools = server.tools();
    return toolList.tools.map(toolInfo => {
      const infoWithId = toolInfo as typeof toolInfo & { id?: unknown };
      const toolName = typeof infoWithId.id === 'string' && infoWithId.id.length > 0 ? infoWithId.id : toolInfo.name;
      const convertedTool = convertedTools[toolName];
      const inputSchema = cloneMcpSchemaLike(toolInfo.inputSchema ?? convertedTool?.parameters);
      const outputSchema = cloneMcpSchemaLike(toolInfo.outputSchema ?? convertedTool?.outputSchema);
      const meta = cloneMcpCatalogRecord(toolInfo._meta ?? convertedTool?.mcp?._meta);
      const toolType = toolInfo.toolType ?? convertedTool?.mcp?.toolType;
      return {
        serverKey: key,
        name: toolName,
        ...(toolInfo.description ? { description: toolInfo.description } : {}),
        ...(inputSchema !== undefined ? { inputSchema } : {}),
        ...(outputSchema !== undefined ? { outputSchema } : {}),
        ...(toolType ? { toolType } : {}),
        ...(meta ? { meta } : {}),
        ...(convertedTool?.strict !== undefined ? { strict: convertedTool.strict } : {}),
      };
    });
  }

  private _assertMcpServerKey(method: string, key: unknown): asserts key is string {
    if (typeof key !== 'string' || key.length === 0) {
      throw new HarnessValidationError(method, 'key must be a non-empty string');
    }
    if (RESERVED_MCP_SERVER_KEYS.has(key)) {
      throw new HarnessValidationError(method, 'key must not be a reserved object property name');
    }
  }

  private _projectMcpServer(
    key: string,
    server: NonNullable<ReturnType<Harness['_getMcpServer']>>,
  ): HarnessMcpServerDescriptor {
    const repository = cloneMcpCatalogRecord(server.repository);
    const packages = cloneMcpCatalogRecordArray(server.packages);
    const remotes = cloneMcpCatalogRecordArray(server.remotes);
    return {
      key,
      id: server.id,
      name: server.name,
      version: server.version,
      ...(server.description ? { description: server.description } : {}),
      ...(server.instructions ? { instructions: server.instructions } : {}),
      releaseDate: server.releaseDate,
      isLatest: server.isLatest,
      ...(repository ? { repository } : {}),
      ...(server.packageCanonical ? { packageCanonical: server.packageCanonical } : {}),
      ...(packages ? { packages } : {}),
      ...(remotes ? { remotes } : {}),
    };
  }

  private async _actionsSearch(
    query: string,
    options?: Omit<HarnessActionCatalogListOptions, 'query'>,
  ): Promise<HarnessActionCatalogEntry[]> {
    this._port.assertLive('actions.search()');
    if (typeof query !== 'string') {
      throw new HarnessValidationError('actions.search()', 'query must be a string');
    }
    if (options !== undefined && (!options || typeof options !== 'object' || Array.isArray(options))) {
      throw new HarnessValidationError('actions.search()', 'options must be an object');
    }
    return this._actionsList({ ...options, query });
  }

  private async _actionsList(options?: HarnessActionCatalogListOptions): Promise<HarnessActionCatalogEntry[]> {
    this._port.assertLive('actions.list()');
    const normalized = this._normalizeActionCatalogOptions(options);
    if (normalized.limit === 0) return [];
    const entries = await this._buildActionCatalogEntries(normalized.source);
    const filtered = entries.filter(entry => this._matchesActionCatalogOptions(entry, normalized));
    return filtered
      .sort((a, b) => {
        const sourceOrder = ACTION_CATALOG_SOURCE_ORDER[a.source.kind] - ACTION_CATALOG_SOURCE_ORDER[b.source.kind];
        return sourceOrder === 0 ? compareActionCatalogIds(a.id, b.id) : sourceOrder;
      })
      .slice(normalized.offset, normalized.offset + normalized.limit)
      .map(entry => cloneActionCatalogEntry(entry));
  }

  private async _actionsRefresh(): Promise<void> {
    this._port.assertLive('actions.refresh()');
    this._clearSkillAndActionCatalogCaches();
    try {
      await this._port.workspace?.skills?.refresh();
    } finally {
      this._clearSkillAndActionCatalogCaches();
    }
  }

  private _clearSkillAndActionCatalogCaches(): void {
    this._skillsCache = undefined;
    this._skillsResolving = undefined;
    this._actionsSkillEntriesCache = undefined;
    this._actionsSkillEntriesResolving = undefined;
    this._actionsMcpEntriesCacheByServer.clear();
    this._actionsMcpEntriesResolvingByServer.clear();
    this._actionsMcpTimedOutWorkByServer.clear();
    this._actionsMcpCatalogGeneration++;
  }

  private _normalizeActionCatalogOptions(options?: HarnessActionCatalogListOptions): {
    query: string;
    source?: HarnessActionCatalogSourceKind;
    limit: number;
    offset: number;
  } {
    if (options !== undefined && (!options || typeof options !== 'object' || Array.isArray(options))) {
      throw new HarnessValidationError('actions.list()', 'options must be an object');
    }
    const query = options?.query ?? '';
    if (typeof query !== 'string') {
      throw new HarnessValidationError('actions.list()', 'query must be a string');
    }
    const source = options?.source;
    if (source !== undefined && !ACTION_CATALOG_SOURCE_KINDS.includes(source)) {
      throw new HarnessValidationError(
        'actions.list()',
        `source must be one of ${ACTION_CATALOG_SOURCE_KINDS.join(' | ')}`,
      );
    }
    const limit = options?.limit ?? ACTION_CATALOG_DEFAULT_LIMIT;
    if (!Number.isInteger(limit) || limit < 0 || limit > ACTION_CATALOG_MAX_LIMIT) {
      throw new HarnessValidationError(
        'actions.list()',
        `limit must be an integer between 0 and ${ACTION_CATALOG_MAX_LIMIT}`,
      );
    }
    const offset = options?.offset ?? 0;
    if (!Number.isInteger(offset) || offset < 0) {
      throw new HarnessValidationError('actions.list()', 'offset must be a non-negative integer');
    }
    return { query: query.trim().toLowerCase(), source, limit, offset };
  }

  private _matchesActionCatalogOptions(
    entry: HarnessActionCatalogEntry,
    options: { query: string; source?: HarnessActionCatalogSourceKind; limit: number; offset: number },
  ): boolean {
    if (options.source !== undefined && entry.source.kind !== options.source) return false;
    if (options.query.length === 0) return true;
    const haystack = this._actionCatalogSearchText(entry).toLowerCase();
    return haystack.includes(options.query);
  }

  private _actionCatalogSearchText(entry: HarnessActionCatalogEntry): string {
    const parts = [entry.id, entry.label, entry.description, entry.category, entry.status, entry.statusReason];
    if (entry.source.kind === 'skill') {
      parts.push(entry.source.skillName, entry.source.filePath);
    } else if (entry.source.kind === 'mcp-server') {
      parts.push(entry.source.serverKey, entry.mcp?.serverName);
    } else {
      parts.push(entry.source.serverKey, entry.source.toolName, entry.mcp?.serverName);
    }
    if (entry.shortcuts) {
      for (const shortcut of entry.shortcuts) {
        parts.push(shortcut.id, shortcut.label, ...(shortcut.keys ?? []));
      }
    }
    return parts.filter((part): part is string => typeof part === 'string' && part.length > 0).join('\n');
  }

  private async _buildActionCatalogEntries(
    source?: HarnessActionCatalogSourceKind,
  ): Promise<HarnessActionCatalogEntry[]> {
    const skillEntries =
      source === 'mcp-tool' || source === 'mcp-server' ? [] : await this._resolveSkillActionCatalogEntries();
    const mcpEntries: HarnessActionCatalogEntry[] = [];
    if (source !== 'skill') {
      mcpEntries.push(...(await this._resolveMcpActionCatalogEntries()));
    }
    return [...skillEntries, ...mcpEntries];
  }

  private async _resolveSkillActionCatalogEntries(): Promise<HarnessActionCatalogEntry[]> {
    if (this._actionsSkillEntriesCache) return this._actionsSkillEntriesCache;
    if (this._actionsSkillEntriesResolving) return this._actionsSkillEntriesResolving;

    const build = async (): Promise<HarnessActionCatalogEntry[]> => {
      const codeSkills = this._port.harness._listCodeSkills();
      const entries: ActionCatalogSkillDescriptor[] = [...codeSkills];
      let workspace: Workspace | undefined;
      try {
        workspace = await this._port.getWorkspace();
      } catch (error) {
        if (!(error instanceof HarnessWorkspaceLostError)) {
          throw error;
        }
      }
      const workspaceSkills = workspace?.skills;
      if (workspaceSkills) {
        const workspaceEntries = await workspaceSkills.list();
        const codeActionNames = new Set(
          codeSkills.filter(skill => this._projectSkillActionCatalogEntry(skill).length > 0).map(skill => skill.name),
        );
        for (const meta of workspaceEntries) {
          if (codeActionNames.has(meta.name) && !meta.path) continue;
          entries.push({
            name: meta.name,
            description: meta.description,
            ...(meta.path ? { filePath: meta.path } : {}),
            ...(meta.metadata ? { metadata: meta.metadata } : {}),
          });
        }
      }
      return entries.flatMap(skill => this._projectSkillActionCatalogEntry(skill));
    };

    const pending = build();
    this._actionsSkillEntriesResolving = pending;
    try {
      const result = await pending;
      if (this._actionsSkillEntriesResolving === pending) {
        this._actionsSkillEntriesCache = result;
      }
      return result;
    } finally {
      if (this._actionsSkillEntriesResolving === pending) {
        this._actionsSkillEntriesResolving = undefined;
      }
    }
  }

  private _projectSkillActionCatalogEntry(skill: ActionCatalogSkillDescriptor): HarnessActionCatalogEntry[] {
    const action = skill.action ?? cloneActionMetadataLike(skill.metadata?.action);
    if (!action) return [];
    const idSource = skill.filePath ?? skill.name;
    return [
      {
        id: `skill:${encodeActionCatalogIdPart(idSource)}`,
        source: {
          kind: 'skill',
          ref: skill.filePath ?? skill.name,
          skillName: skill.name,
          ...(skill.filePath ? { filePath: skill.filePath } : {}),
        },
        status: 'available',
        label: action.displayName ?? skill.name,
        description: skill.description,
        ...(skill.category ? { category: skill.category } : {}),
        ...(action.icon ? { icon: action.icon } : {}),
        ...(action.shortcuts
          ? { shortcuts: cloneMcpCatalogValue(action.shortcuts) as HarnessActionCatalogEntry['shortcuts'] }
          : {}),
        ...(action.inputSchema ? { inputSchema: cloneMcpCatalogValue(action.inputSchema) } : {}),
        ...(action.outputSchema ? { outputSchema: cloneMcpCatalogValue(action.outputSchema) } : {}),
        ...(action.artifactTypes ? { artifactTypes: [...action.artifactTypes] } : {}),
        ...(action.permissions
          ? { permissions: cloneMcpCatalogValue(action.permissions) as HarnessActionCatalogEntry['permissions'] }
          : {}),
      },
    ];
  }

  private _projectMcpToolActionCatalogEntry(
    server: HarnessMcpServerDescriptor,
    tool: HarnessMcpToolDescriptor,
  ): HarnessActionCatalogEntry {
    return {
      id: `mcp-tool:${encodeActionCatalogIdPart(server.key)}:${encodeActionCatalogIdPart(tool.name)}`,
      source: { kind: 'mcp-tool', serverKey: server.key, toolName: tool.name },
      status: 'available',
      label: tool.name,
      ...(tool.description ? { description: tool.description } : {}),
      ...(tool.inputSchema !== undefined ? { inputSchema: cloneMcpCatalogValue(tool.inputSchema) } : {}),
      ...(tool.outputSchema !== undefined ? { outputSchema: cloneMcpCatalogValue(tool.outputSchema) } : {}),
      permissions: { mcpScopes: [server.key] },
      mcp: {
        serverName: server.name,
        serverVersion: server.version,
        ...(tool.toolType ? { toolType: tool.toolType } : {}),
        ...(tool.strict !== undefined ? { strict: tool.strict } : {}),
        ...(tool.meta ? { meta: cloneMcpCatalogValue(tool.meta) as Record<string, unknown> } : {}),
      },
    };
  }

  private _projectUnavailableMcpActionCatalogEntry(
    server: HarnessMcpServerDescriptor,
    reason: HarnessActionCatalogUnavailableReason,
  ): HarnessActionCatalogEntry {
    const statusMessage =
      reason === 'mcp_tool_catalog_timeout'
        ? 'MCP tool catalog timed out'
        : reason === 'mcp_tool_catalog_retry_suppressed'
          ? 'MCP tool catalog retry delayed'
          : 'MCP tool catalog unavailable';
    return {
      id: `mcp-server:${encodeActionCatalogIdPart(server.key)}`,
      source: { kind: 'mcp-server', serverKey: server.key },
      status: 'unavailable',
      statusReason: reason,
      statusMessage,
      label: server.name,
      ...(server.description ? { description: server.description } : {}),
      permissions: { mcpScopes: [server.key] },
      mcp: {
        serverName: server.name,
        serverVersion: server.version,
      },
    };
  }

  private _actionMcpCatalogCacheKey(server: HarnessMcpServerDescriptor, workspaceId: string): string {
    // canonicalJson is NUL-/sentinel-safe: every byte (incl. the historical '\0'
    // separator) is JSON-escaped, so distinct (serverKey, modeId, modelId,
    // workspaceId) tuples — any of which can carry arbitrary strings — never
    // collide the way a single-char `join` could.
    return canonicalJson([server.key, this._port.record.modeId, this._port.record.modelId ?? null, workspaceId]);
  }

  private _currentMcpActionCatalogWorkspaceId(): string {
    // MCP catalog reads pass `resolveWorkspace: false`; key only by a workspace
    // that was already materialized so catalog reads never provision one.
    return this._port.workspace?.id ?? '';
  }

  private _getCachedMcpActionCatalogEntries(cacheKey: string): HarnessActionCatalogEntry[] | undefined {
    const cached = this._actionsMcpEntriesCacheByServer.get(cacheKey);
    if (!cached) return undefined;
    if (cached.expiresAt === undefined || cached.expiresAt > Date.now()) {
      return cached.entries;
    }
    this._actionsMcpEntriesCacheByServer.delete(cacheKey);
    return undefined;
  }

  private async _resolveMcpActionCatalogEntries(): Promise<HarnessActionCatalogEntry[]> {
    const results = await Promise.all(
      this._mcpListServers().map(server => this._resolveMcpActionCatalogEntriesForServer(server)),
    );
    return results.flat();
  }

  private _startMcpActionCatalogEntriesForServer(
    server: HarnessMcpServerDescriptor,
    cacheKey: string,
    retryCount = 0,
  ): Promise<HarnessActionCatalogEntry[]> {
    const catalogGeneration = this._actionsMcpCatalogGeneration;
    let pending: Promise<HarnessActionCatalogEntry[]>;
    const started = startActionCatalogMcpListWithTimeout(
      async abortSignal => {
        const tools = await this._mcpListTools(server.key, abortSignal, { resolveWorkspace: false });
        return (tools ?? []).map(tool => this._projectMcpToolActionCatalogEntry(server, tool));
      },
      work => {
        if (this._actionsMcpEntriesResolvingByServer.get(cacheKey) === pending) {
          this._actionsMcpTimedOutWorkByServer.set(cacheKey, {
            work,
            retryAfter: Date.now() + ACTION_CATALOG_MCP_FAILURE_CACHE_MS,
            retryCount,
          });
        }
      },
    );
    pending = started.pending;
    const { work, didTimeout } = started;
    pending.catch(() => {
      // `pending` is observed below for cache/single-flight cleanup; attach
      // this noop handler immediately so timeout rejection is never unhandled.
    });
    this._actionsMcpEntriesResolvingByServer.set(cacheKey, pending);

    pending
      .then(entries => {
        if (this._actionsMcpEntriesResolvingByServer.get(cacheKey) === pending) {
          this._actionsMcpEntriesCacheByServer.set(cacheKey, {
            entries,
            expiresAt: Date.now() + ACTION_CATALOG_MCP_SUCCESS_CACHE_MS,
            successful: true,
          });
          this._actionsMcpEntriesResolvingByServer.delete(cacheKey);
        }
      })
      .catch(error => {
        if (this._actionsMcpEntriesResolvingByServer.get(cacheKey) !== pending) return;
        const timeoutWorkStillTracked = this._actionsMcpTimedOutWorkByServer.get(cacheKey)?.work === work;
        const didCatalogTimeout =
          error instanceof ActionCatalogMcpListTimeoutError || (didTimeout() && timeoutWorkStillTracked);
        const existingCache = this._actionsMcpEntriesCacheByServer.get(cacheKey);
        const hasSuccessfulTimedOutResult = existingCache?.successful === true;
        if ((!didCatalogTimeout || timeoutWorkStillTracked) && !hasSuccessfulTimedOutResult) {
          const reason: HarnessActionCatalogUnavailableReason = didCatalogTimeout
            ? 'mcp_tool_catalog_timeout'
            : 'mcp_tool_catalog_failed';
          this._actionsMcpEntriesCacheByServer.set(cacheKey, {
            entries: [this._projectUnavailableMcpActionCatalogEntry(server, reason)],
            expiresAt: Date.now() + ACTION_CATALOG_MCP_FAILURE_CACHE_MS,
          });
        }
        this._actionsMcpEntriesResolvingByServer.delete(cacheKey);
      });

    work
      .then(entries => {
        if (didTimeout() && this._actionsMcpCatalogGeneration === catalogGeneration) {
          this._actionsMcpEntriesCacheByServer.set(cacheKey, {
            entries,
            expiresAt: Date.now() + ACTION_CATALOG_MCP_SUCCESS_CACHE_MS,
            successful: true,
          });
        }
      })
      .catch(() => {
        if (didTimeout() && this._actionsMcpTimedOutWorkByServer.get(cacheKey)?.work === work) {
          this._actionsMcpEntriesCacheByServer.set(cacheKey, {
            entries: [this._projectUnavailableMcpActionCatalogEntry(server, 'mcp_tool_catalog_timeout')],
            expiresAt: Date.now() + ACTION_CATALOG_MCP_FAILURE_CACHE_MS,
          });
        }
      })
      .finally(() => {
        if (didTimeout() && this._actionsMcpTimedOutWorkByServer.get(cacheKey)?.work === work) {
          this._actionsMcpTimedOutWorkByServer.delete(cacheKey);
        }
      });

    return pending;
  }

  private async _resolveMcpActionCatalogEntriesForServer(
    server: HarnessMcpServerDescriptor,
  ): Promise<HarnessActionCatalogEntry[]> {
    const cacheKey = this._actionMcpCatalogCacheKey(server, this._currentMcpActionCatalogWorkspaceId());
    const cached = this._getCachedMcpActionCatalogEntries(cacheKey);
    if (cached) return cached;

    const timedOutWork = this._actionsMcpTimedOutWorkByServer.get(cacheKey);
    if (
      timedOutWork &&
      (timedOutWork.retryAfter > Date.now() || timedOutWork.retryCount >= ACTION_CATALOG_MCP_MAX_TIMEOUT_RETRIES)
    ) {
      const entries = [this._projectUnavailableMcpActionCatalogEntry(server, 'mcp_tool_catalog_retry_suppressed')];
      this._actionsMcpEntriesCacheByServer.set(cacheKey, {
        entries,
        expiresAt:
          timedOutWork.retryAfter > Date.now()
            ? timedOutWork.retryAfter
            : Date.now() + ACTION_CATALOG_MCP_FAILURE_CACHE_MS,
      });
      return entries;
    }
    if (timedOutWork) {
      this._actionsMcpTimedOutWorkByServer.delete(cacheKey);
    }

    const pending =
      this._actionsMcpEntriesResolvingByServer.get(cacheKey) ??
      this._startMcpActionCatalogEntriesForServer(server, cacheKey, (timedOutWork?.retryCount ?? -1) + 1);
    try {
      return await pending;
    } catch (error) {
      const reason: HarnessActionCatalogUnavailableReason =
        error instanceof ActionCatalogMcpListTimeoutError ? 'mcp_tool_catalog_timeout' : 'mcp_tool_catalog_failed';
      return [this._projectUnavailableMcpActionCatalogEntry(server, reason)];
    }
  }

  private async _skillsList(): Promise<HarnessSkill[]> {
    this._port.assertLive('skills.list()');
    return this._resolveSkills();
  }

  private async _skillsGet(name: string): Promise<HarnessSkill | undefined> {
    this._port.assertLive('skills.get()');
    if (typeof name !== 'string' || name.length === 0) {
      throw new HarnessValidationError('skills.get()', 'name must be a non-empty string');
    }
    const codeSkill = this._port.harness._getCodeSkill(name);
    if (codeSkill) return codeSkill;
    const skills = await this._resolveSkills();
    return skills.find(s => s.name === name);
  }

  private async _skillsRefresh(): Promise<void> {
    this._port.assertLive('skills.refresh()');
    this._clearSkillAndActionCatalogCaches();
    try {
      await this._port.workspace?.skills?.refresh();
    } finally {
      this._clearSkillAndActionCatalogCaches();
    }
  }

  /**
   * Resolve a code-registered skill by name, or a workspace skill by name or
   * relative path, validate any declared args schema, inject the validated
   * args as a JSON code block into the skill instructions, and dispatch the
   * result through `message()` as a single turn. Returns the underlying
   * `AgentResult`.
   *
   * Reference resolution mirrors Flue's workspace `session.skill(ref, ...)`
   * behavior for explicit workspace paths, then checks the static code
   * registry by name, then falls back to workspace skill names. A code skill
   * owns its name, but does not hide an otherwise shadowed workspace skill's
   * explicit path reference.
   */
  private _isExplicitWorkspaceSkillRef(ref: string): boolean {
    return (
      ref.includes('/') || ref.startsWith('./') || ref.startsWith('../') || /\.(?:md|mdx|txt|yaml|yml)$/i.test(ref)
    );
  }

  async _skillsUse(ref: string, opts?: UseSkillOptions): Promise<AgentResult> {
    this._port.assertLive('skills.use()');
    if (typeof ref !== 'string' || ref.length === 0) {
      throw new HarnessValidationError('skills.use()', 'ref must be a non-empty string');
    }
    // §3: useSkill is a fail-fast operation — it must refuse on a busy session
    // rather than interleaving/queuing like a default message.
    if (this._port.isBusy()) {
      throw new HarnessBusyError(this._port.id, this._port.busyReason());
    }

    // §4.4c: validate caller request context before skill resolution so a bad
    // app bag fails fast (attributed to skills.use()); the normalized value is
    // forwarded to the delegated message() turn (which re-validates idempotently).
    const callerRequestContext = validateCallerRequestContext(opts?.requestContext, 'skills.use()');

    const tryWorkspaceSkill = async () => {
      const workspace = await this._port.getWorkspace();
      const skills = workspace?.skills;
      return skills ? skills.get(ref) : undefined;
    };

    if (this._isExplicitWorkspaceSkillRef(ref)) {
      const skill = await tryWorkspaceSkill();
      if (skill) {
        const args = opts?.args;
        this._validateSkillArgs(skill.name, skill.metadata, args);
        const expandedContent = this._buildSkillPrompt(skill.instructions, args);
        return this._port.message({
          content: expandedContent,
          ...(opts?.modelOverride ? { model: opts.modelOverride } : {}),
          ...(callerRequestContext ? { requestContext: callerRequestContext } : {}),
        });
      }
    }

    const codeSkill = this._port.harness._getCodeSkill(ref);
    if (codeSkill) {
      this._validateSkillArgs(codeSkill.name, codeSkill.metadata, opts?.args);
      const expandedContent = this._buildSkillPrompt(codeSkill.instructions, opts?.args);
      return this._port.message({
        content: expandedContent,
        ...(opts?.modelOverride ? { model: opts.modelOverride } : {}),
        ...(callerRequestContext ? { requestContext: callerRequestContext } : {}),
      });
    }

    // Force workspace materialization. Unlike `list` / `get`, `use` must
    // produce a definitive answer (start a turn or refuse with a typed
    // not-found error).
    const workspace = await this._port.getWorkspace();
    const skills = workspace?.skills;
    if (!skills) {
      throw new HarnessSkillNotFoundError(ref, ['code-registered']);
    }

    // `WorkspaceSkills.get` accepts either the frontmatter `name` or a
    // relative path under the configured skill source.
    const skill = await skills.get(ref);
    if (!skill) {
      throw new HarnessSkillNotFoundError(ref, ['code-registered', 'workspace']);
    }
    const args = opts?.args;
    this._validateSkillArgs(skill.name, skill.metadata, args);

    // Build the expanded prompt: skill instructions + (optional) JSON
    // code block carrying validated args. Skill authors reference the
    // args naturally in Markdown.
    const expandedContent = this._buildSkillPrompt(skill.instructions, args);

    return this._port.message({
      content: expandedContent,
      ...(opts?.modelOverride ? { model: opts.modelOverride } : {}),
      ...(callerRequestContext ? { requestContext: callerRequestContext } : {}),
    });
  }

  /**
   * Validate `metadata.args` as a small JSON-schema-ish object. The harness
   * supports the common prompt-arg fields used by workspace frontmatter:
   * `required`, `properties`, `type`, `enum`, `items`, and
   * `additionalProperties`. Unsupported or malformed schema shapes fail
   * before a skill turn starts.
   */
  private _validateSkillArgs(
    skillName: string,
    metadata: Record<string, unknown> | undefined,
    args: Record<string, unknown> | undefined,
  ): void {
    if (args !== undefined && (!args || typeof args !== 'object' || Array.isArray(args))) {
      throw new HarnessSkillArgsValidationError(skillName, ['args must be an object']);
    }

    const issues: string[] = [];
    if (args !== undefined) {
      this._validateJsonSerializableSkillArg('$', args, new WeakSet(), issues);
    }
    if (!metadata || typeof metadata !== 'object') {
      if (issues.length > 0) throw new HarnessSkillArgsValidationError(skillName, issues);
      return;
    }
    const argsField = (metadata as Record<string, unknown>).args;
    if (argsField === undefined) {
      if (issues.length > 0) throw new HarnessSkillArgsValidationError(skillName, issues);
      return;
    }
    if (!this._isPlainRecord(argsField)) {
      throw new HarnessSkillArgsValidationError(skillName, ['unsupported args schema: expected object']);
    }

    const issueCountBeforeSchemaShape = issues.length;
    this._validateSkillArgSchemaShape('$', argsField, issues, new WeakSet());
    if (issues.length > issueCountBeforeSchemaShape) {
      throw new HarnessSkillArgsValidationError(skillName, issues);
    }

    this._validateSkillArgSchemaValue('$', args ?? {}, argsField, issues);
    if (issues.length > 0) {
      throw new HarnessSkillArgsValidationError(skillName, issues);
    }
  }

  private _validateSkillArgSchemaShape(
    path: string,
    schema: Record<string, unknown>,
    issues: string[],
    seen: WeakSet<object>,
  ): void {
    if (seen.has(schema)) {
      issues.push(`${path} must not contain circular args schema references`);
      return;
    }
    seen.add(schema);

    for (const key of Object.keys(schema)) {
      if (!SUPPORTED_SKILL_ARG_SCHEMA_KEYS.has(key)) {
        issues.push(`${path}.${key} is not a supported args schema field`);
      }
    }

    const required = schema.required;
    if (
      required !== undefined &&
      (!Array.isArray(required) || required.some(k => typeof k !== 'string' || k.length === 0))
    ) {
      issues.push(`${path}.required must be an array of non-empty strings`);
    }

    const enumValues = schema.enum;
    if (enumValues !== undefined) {
      if (!Array.isArray(enumValues)) {
        issues.push(`${path}.enum must be an array`);
      } else {
        enumValues.forEach((candidate, index) => {
          this._validateJsonSerializableSkillArg(`${path}.enum[${index}]`, candidate, new WeakSet(), issues);
        });
      }
    }

    const declaredType = schema.type;
    if (declaredType !== undefined) {
      this._validateSkillArgDeclaredType(path, declaredType, issues);
    }

    const properties = schema.properties;
    if (properties !== undefined) {
      if (!this._isPlainRecord(properties)) {
        issues.push(`${path}.properties must be an object`);
      } else {
        for (const [key, childSchema] of Object.entries(properties)) {
          if (!this._isPlainRecord(childSchema)) {
            issues.push(`${path}.properties.${key} must be an object`);
            continue;
          }
          this._validateSkillArgSchemaShape(path === '$' ? key : `${path}.${key}`, childSchema, issues, seen);
        }
      }
    }

    const additionalProperties = schema.additionalProperties;
    if (additionalProperties !== undefined && additionalProperties !== true && additionalProperties !== false) {
      issues.push(`${path}.additionalProperties must be boolean`);
    }

    const items = schema.items;
    if (items !== undefined) {
      if (!this._isPlainRecord(items)) {
        issues.push(`${path}.items must be an object`);
      } else {
        this._validateSkillArgSchemaShape(`${path}[]`, items, issues, seen);
      }
    }

    seen.delete(schema);
  }

  private _validateSkillArgSchemaValue(
    path: string,
    value: unknown,
    schema: Record<string, unknown>,
    issues: string[],
  ): void {
    const required = schema.required;
    if (Array.isArray(required) && this._isPlainRecord(value)) {
      for (const key of required) {
        if (!Object.prototype.hasOwnProperty.call(value, key) || value[key] === undefined) {
          issues.push(`missing required arg: "${path === '$' ? key : `${path}.${key}`}"`);
        }
      }
    }

    const enumValues = schema.enum;
    if (Array.isArray(enumValues) && !enumValues.some(candidate => this._skillArgValuesEqual(candidate, value))) {
      issues.push(`${path} must be one of ${JSON.stringify(enumValues)}`);
    }

    const declaredType = schema.type;
    if (declaredType !== undefined && !this._matchesSkillArgType(value, declaredType, path, issues)) return;

    const properties = schema.properties;
    const additionalProperties = schema.additionalProperties;
    if (this._isPlainRecord(properties) && this._isPlainRecord(value)) {
      for (const [key, childSchema] of Object.entries(properties)) {
        if (!Object.prototype.hasOwnProperty.call(value, key)) continue;
        if (!this._isPlainRecord(childSchema)) continue;
        this._validateSkillArgSchemaValue(path === '$' ? key : `${path}.${key}`, value[key], childSchema, issues);
      }
    }
    if (additionalProperties === false && this._isPlainRecord(value)) {
      const allowedKeys = this._isPlainRecord(properties) ? new Set(Object.keys(properties)) : new Set<string>();
      for (const key of Object.keys(value)) {
        if (!allowedKeys.has(key)) issues.push(`unsupported arg: "${path === '$' ? key : `${path}.${key}`}"`);
      }
    }

    const items = schema.items;
    if (this._isPlainRecord(items) && Array.isArray(value)) {
      value.forEach((item, index) => {
        this._validateSkillArgSchemaValue(`${path}[${index}]`, item, items, issues);
      });
    }
  }

  private _validateSkillArgDeclaredType(path: string, declaredType: unknown, issues: string[]): void {
    const allowedTypes = Array.isArray(declaredType) ? declaredType : [declaredType];
    const supported = new Set(['string', 'number', 'integer', 'boolean', 'object', 'array', 'null']);
    if (allowedTypes.some(type => typeof type !== 'string' || !supported.has(type))) {
      issues.push(`${path}.type must be a supported JSON schema type`);
    }
  }

  private _matchesSkillArgType(value: unknown, declaredType: unknown, path: string, issues: string[]): boolean {
    const allowedTypes = Array.isArray(declaredType) ? declaredType : [declaredType];
    const supported = new Set(['string', 'number', 'integer', 'boolean', 'object', 'array', 'null']);
    if (allowedTypes.some(type => typeof type !== 'string' || !supported.has(type))) {
      issues.push(`${path}.type must be a supported JSON schema type`);
      return false;
    }

    const actualMatches = allowedTypes.some(type => {
      switch (type) {
        case 'string':
          return typeof value === 'string';
        case 'number':
          return typeof value === 'number' && Number.isFinite(value);
        case 'integer':
          return Number.isInteger(value);
        case 'boolean':
          return typeof value === 'boolean';
        case 'object':
          return this._isPlainRecord(value);
        case 'array':
          return Array.isArray(value);
        case 'null':
          return value === null;
        default:
          return false;
      }
    });
    if (!actualMatches) {
      issues.push(`${path} must be ${allowedTypes.join(' | ')}`);
    }
    return actualMatches;
  }

  private _isPlainRecord(value: unknown): value is Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
  }

  private _skillArgValuesEqual(left: unknown, right: unknown): boolean {
    if (Object.is(left, right)) return true;
    if (Array.isArray(left) && Array.isArray(right)) {
      return (
        left.length === right.length && left.every((value, index) => this._skillArgValuesEqual(value, right[index]))
      );
    }
    if (this._isPlainRecord(left) && this._isPlainRecord(right)) {
      const leftKeys = Object.keys(left);
      const rightKeys = Object.keys(right);
      if (leftKeys.length !== rightKeys.length) return false;
      return leftKeys.every(
        key => Object.prototype.hasOwnProperty.call(right, key) && this._skillArgValuesEqual(left[key], right[key]),
      );
    }
    return false;
  }

  private _validateJsonSerializableSkillArg(
    path: string,
    value: unknown,
    seen: WeakSet<object>,
    issues: string[],
  ): void {
    if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
    if (typeof value === 'number') {
      if (!Number.isFinite(value)) issues.push(`${path} must be JSON-serializable`);
      return;
    }
    if (value === undefined || typeof value === 'bigint' || typeof value === 'function' || typeof value === 'symbol') {
      issues.push(`${path} must be JSON-serializable`);
      return;
    }
    if (typeof value !== 'object') return;
    if (
      Object.prototype.hasOwnProperty.call(value, 'toJSON') &&
      typeof (value as { toJSON?: unknown }).toJSON === 'function'
    ) {
      issues.push(`${path}.toJSON is not supported in skill args`);
      return;
    }
    if (seen.has(value)) {
      issues.push(`${path} must not contain circular references`);
      return;
    }
    seen.add(value);
    if (Array.isArray(value)) {
      value.forEach((item, index) => this._validateJsonSerializableSkillArg(`${path}[${index}]`, item, seen, issues));
    } else if (this._isPlainRecord(value)) {
      for (const [key, child] of Object.entries(value)) {
        this._validateJsonSerializableSkillArg(path === '$' ? key : `${path}.${key}`, child, seen, issues);
      }
    } else {
      issues.push(`${path} must be JSON-serializable`);
    }
    seen.delete(value);
  }

  /**
   * Compose the skill prompt body. When args are supplied, append a JSON
   * code block carrying them. No delimiters beyond the Markdown fence —
   * skill authors reference args inline in their instructions.
   */
  private _buildSkillPrompt(instructions: string, args: Record<string, unknown> | undefined): string {
    if (!args || Object.keys(args).length === 0) return instructions;
    const json = JSON.stringify(args, null, 2);
    return `${instructions}\n\n\`\`\`json\n${json}\n\`\`\``;
  }

  /**
   * Internal: resolve the skill catalog for this session, sharing a
   * single-flight promise across concurrent callers.
   */
  private async _resolveSkills(): Promise<HarnessSkill[]> {
    if (this._skillsCache) return this._skillsCache;
    if (this._skillsResolving) return this._skillsResolving;

    const build = async (): Promise<HarnessSkill[]> => {
      const codeSkills = this._port.harness._listCodeSkills();
      const workspace = await this._port.getWorkspace();
      const workspaceSkills = workspace?.skills;
      if (!workspaceSkills) {
        // No workspace, or workspace has no skill source configured.
        return codeSkills;
      }
      const entries = await workspaceSkills.list();
      const codeNames = new Set(codeSkills.map(skill => skill.name));
      const projected = await Promise.all(
        entries
          .filter(meta => !codeNames.has(meta.name))
          .map(async meta => {
            const skill = await workspaceSkills.get(meta.path ?? meta.name);
            return {
              name: meta.name,
              description: meta.description,
              instructions: skill?.instructions ?? '',
              ...(meta.path ? { filePath: meta.path } : {}),
              // Pass through arbitrary skill frontmatter metadata so callers can
              // discover skill-level flags (e.g. `metadata.goal === true` for
              // goal-mode skills). Workspace's `SkillMetadata.metadata` is
              // typed `Record<string, unknown>` and is already JSON-serialisable.
              ...(meta.metadata ? { metadata: meta.metadata } : {}),
            };
          }),
      );
      return [...codeSkills, ...projected];
    };

    const pending = build();
    this._skillsResolving = pending;
    try {
      const result = await pending;
      // Only populate the cache when our own promise is still the
      // session-tracked one. If `skills.refresh()` ran while we were
      // resolving, `_skillsResolving` was cleared (or replaced by a
      // newer build) and we must not stomp it.
      if (this._skillsResolving === pending) {
        this._skillsCache = result;
      }
      return result;
    } finally {
      if (this._skillsResolving === pending) {
        this._skillsResolving = undefined;
      }
    }
  }
}
