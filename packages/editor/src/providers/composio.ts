import { AsyncLocalStorage } from 'node:async_hooks';

import { Composio } from '@composio/core';
import type {
  ConnectedAccountListResponse,
  Tool as ComposioTool,
  ToolListParams as ComposioToolListParams,
  ToolKitItem,
} from '@composio/core';
import { MastraProvider } from '@composio/mastra';
import type { MastraToolCollection } from '@composio/mastra';
import { MASTRA_RESOURCE_ID_KEY } from '@mastra/core/request-context';
import type { RequestContext } from '@mastra/core/request-context';
import type {
  AuthFlowStatus,
  AuthorizeOpts,
  ConnectionField,
  ExistingConnection,
  ListConnectionsOpts,
  ListConnectionsResult,
  ListToolsOpts,
  ListToolsResult,
  ResolveToolsOpts,
  ToolProviderCapabilities,
  ToolProviderHealth,
  ToolProviderInfo,
  ToolProviderToolkit,
  BaseToolProviderOptions,
} from '@mastra/core/tool-provider';
import { BaseToolProvider } from '@mastra/core/tool-provider';
import type { ToolAction } from '@mastra/core/tools';

export interface ComposioToolProviderConfig extends BaseToolProviderOptions {
  /** Composio API key. */
  apiKey: string;
  /**
   * Server-side resolver mapping request context to the Composio `userId` the
   * call should execute as. Runs for `kind: 'invoker'` and `caller-supplied`
   * resolution, so the host application (for example, its FGA layer) can
   * derive and authorize the effective user before execution.
   *
   * Only server-populated fields within request context are trusted. When the
   * resolver is absent (or returns `undefined`), invoker connections require
   * the authenticated user (`MASTRA_USER_KEY`). Legacy `caller-supplied`
   * connections retain their existing resource-id fallback.
   *
   * The exact `connectedAccountId` always comes from the stored connection
   * pin — the resolver cannot override it.
   */
  userIdResolver?: ComposioUserIdResolver;
  /**
   * Opt in to Composio SHARED connected accounts. When set, connections
   * authorized with `scope: 'shared'` are created as Composio `SHARED`
   * accounts with the given ACL. Without it, shared-scope connections stay
   * Composio `PRIVATE` accounts under the shared bucket.
   *
   * Composio marks SHARED accounts as experimental.
   */
  sharedConnections?: ComposioSharedConnectionsConfig;
  /**
   * Approval policy for resolved tools. `true` requires approval for every
   * tool; a function decides per tool from the tags Composio advertises for it
   * (`readOnlyHint`, `createHint`, `updateHint`, `destructiveHint`, ...).
   * A tool whose tags are unknown (connection-management session tools) is
   * passed `tags: []`, so a "not read-only" policy requires approval for it.
   *
   * SECURITY: tags are Composio's hints, decided once at resolution time. Treat
   * them as trusted only as far as you trust Composio's catalog.
   */
  requireToolApproval?: boolean | ComposioRequireToolApproval;
  /**
   * Forwarded to both Composio clients. `false` stops the SDK's anonymous
   * usage analytics. Absent keeps the SDK default.
   */
  allowTracking?: boolean;
}

/** What {@link ComposioToolProviderConfig.requireToolApproval} decides on. */
export interface ComposioToolApprovalInput {
  /** The Composio tool slug. */
  slug: string;
  /** The toolkit slug, when Composio reports one. */
  toolkit?: string;
  /** The tool's Composio tags; empty when unknown. */
  tags: readonly string[];
}

export type ComposioRequireToolApproval = (tool: ComposioToolApprovalInput) => boolean;

/** The Composio identity one tool call executes as. */
export interface ComposioCallIdentity {
  /** The Composio `userId` bucket. */
  userId: string;
  /** The exact connected account; absent lets Composio pick within the bucket. */
  connectedAccountId?: string;
}

/** Inputs handed to a {@link ComposioCallIdentityResolver}. */
export interface ComposioCallIdentityInput {
  /** The live request context of this tool call. */
  requestContext?: RequestContext;
  /** The Composio tool slug being called. */
  toolSlug: string;
  /** The toolkit slug, when Composio reports one. */
  toolkit?: string;
}

/**
 * Server-side resolver of the identity one catalog tool call executes as.
 * Returning `undefined` refuses the call before anything reaches Composio.
 * Must never trust client-supplied context values.
 */
export type ComposioCallIdentityResolver = (
  input: ComposioCallIdentityInput,
) => Promise<ComposioCallIdentity | undefined> | ComposioCallIdentity | undefined;

/** Options for {@link ComposioToolProvider.resolveCatalogTools}. */
/** Options for {@link ComposioToolProvider.listConnections}. */
export interface ComposioListConnectionsOpts extends ListConnectionsOpts {
  /**
   * List only accounts the requested buckets own. SHARED accounts owned by
   * other users are left out even when their ACL admits a requested bucket.
   * Use it when "connected" must mean the user's own account.
   */
  ownedOnly?: boolean;
}

/** One connected account listed by {@link ComposioToolProvider.listConnections}. */
export interface ComposioExistingConnection extends ExistingConnection {
  /**
   * The account belongs to a requested bucket. `false` for another user's
   * SHARED account whose ACL admits a requested bucket.
   */
  owned: boolean;
  /** Composio's sharing model for the account, when Composio reports one. */
  accountType?: 'PRIVATE' | 'SHARED';
}

/** Result of {@link ComposioToolProvider.listConnections}. */
export interface ComposioListConnectionsResult extends ListConnectionsResult {
  items: ComposioExistingConnection[];
}

export interface ResolveComposioCatalogToolsOpts {
  /** Composio tool slugs to materialise. */
  toolSlugs: string[];
  /** Per-tool description overrides keyed by slug. */
  toolMeta?: Record<string, { description?: string; toolkit?: string }>;
  /** Decides each call's identity from that call's request context. */
  resolveIdentity: ComposioCallIdentityResolver;
}

/**
 * The bucket catalog tools are fetched under. Only schemas are read with it;
 * every execution replaces it with the resolved call identity, and a call
 * without one is refused, so no call ever executes as this id.
 */
const CATALOG_SCHEMA_USER_ID = '__mastra_catalog_schema__';

/** ACL applied to Composio SHARED accounts. Mirrors Composio's `aclConfigForShared`. */
export interface ComposioSharedAccountAcl {
  /** Allow every Composio `userId` (except `notAllowedUserIds`). */
  allowAllUsers?: boolean;
  /** Composio `userId`s allowed to use the account. */
  allowedUserIds?: string[];
  /** Composio `userId`s denied access. Deny always wins. */
  notAllowedUserIds?: string[];
}

export interface ComposioSharedConnectionsConfig {
  /**
   * Initial ACL for newly created SHARED accounts. Omit to keep Composio's
   * deny-by-default (only the creating `userId` can use the account).
   */
  acl?: ComposioSharedAccountAcl;
}

/** Inputs handed to {@link ComposioToolProviderConfig.userIdResolver}. */
export interface ComposioUserIdResolverInput {
  /** Live per-request context. Use `get()` for declared keys and `getRaw()` for reserved runtime keys. */
  requestContext?: RequestContext;
  /** Toolkit slug the identity is being resolved for, when known. */
  toolkit?: string;
  /**
   * The stored connection pin being resolved, when one exists. Hosts can use
   * it to validate that the invoker is allowed to use this exact account.
   */
  connectedAccountId?: string;
}

/**
 * Server-side resolver returning the effective Composio `userId` for a
 * request. Returning `undefined` falls back to the provider's default
 * identity resolution. Must never trust client-supplied context values.
 */
export type ComposioUserIdResolver = (
  input: ComposioUserIdResolverInput,
) => Promise<string | undefined> | string | undefined;

const COMPOSIO_PROVIDER_ID = 'composio' as const;
const DEFAULT_INTERNAL_USER_ID = 'default';
const COMPOSIO_CONNECTION_MANAGEMENT_TOOLS = new Set(['COMPOSIO_MANAGE_CONNECTIONS', 'COMPOSIO_WAIT_FOR_CONNECTIONS']);

/**
 * Composio implementation of the {@link BaseToolProvider} contract.
 *
 * Discovery (`listAllToolkits`, `listAllTools`) uses the raw Composio
 * client. Runtime (`resolveToolsVNext`) uses {@link MastraProvider} so resolved
 * tools are already in `createTool()` shape. Ordinary tools use Composio's
 * direct-tools API, while connection-management tools use a caller-scoped
 * Tool Router session. Resolved tools keep the `outputSchema` supplied by
 * `@composio/mastra`, which pre-relaxes Composio's strict API schemas
 * (nullable fields, extra properties, no `required`) so real third-party
 * responses validate while structurally invalid output is still rejected.
 *
 * Allowlist filtering is layered by {@link BaseToolProvider}; this class
 * never reads `allowedToolkits` / `allowedTools` directly.
 */
export class ComposioToolProvider extends BaseToolProvider {
  readonly info: ToolProviderInfo = {
    id: COMPOSIO_PROVIDER_ID,
    name: 'Composio',
    description: 'Access 10,000+ tools from 150+ apps via Composio',
  };
  readonly capabilities: ToolProviderCapabilities = {
    multipleConnectionsPerToolkit: true,
    batchConnectionStatus: true,
    reauthorizeReusesConnectionId: true,
    supportsRevoke: true,
  };

  readonly userIdResolver?: ComposioUserIdResolver;

  private readonly apiKey: string;
  private readonly requireToolApproval?: boolean | ComposioRequireToolApproval;
  private readonly allowTracking?: boolean;
  private readonly sharedConnections?: ComposioSharedConnectionsConfig;
  private rawClient: Composio | null = null;
  private mastraClient: Composio<MastraProvider> | null = null;

  constructor(config: ComposioToolProviderConfig) {
    super({
      allowedToolkits: config.allowedToolkits,
      allowedTools: config.allowedTools,
      defaultScope: config.defaultScope,
    });
    this.apiKey = config.apiKey;
    this.userIdResolver = config.userIdResolver;
    this.sharedConnections = config.sharedConnections;
    this.requireToolApproval = config.requireToolApproval;
    this.allowTracking = config.allowTracking;
  }

  /**
   * Options shared by both clients. `disableVersionCheck` stops the SDK from
   * asking the npm registry for its latest version whenever a client is
   * built: a server provider must not depend on npm egress.
   */
  private clientOptions(): { allowTracking?: boolean; disableVersionCheck: true } {
    return {
      disableVersionCheck: true,
      ...(this.allowTracking === undefined ? {} : { allowTracking: this.allowTracking }),
    };
  }

  // ── client cache ──────────────────────────────────────────────────────

  private getRawClient(): Composio {
    if (!this.rawClient) {
      this.rawClient = new Composio({ apiKey: this.apiKey, ...this.clientOptions() });
    }
    return this.rawClient;
  }

  private getMastraClient(): Composio<MastraProvider> {
    if (!this.mastraClient) {
      this.mastraClient = new Composio({
        apiKey: this.apiKey,
        provider: new MastraProvider(),
        ...this.clientOptions(),
      });
    }
    return this.mastraClient;
  }

  // ── catalog (BaseToolProvider adds allowlist filter on top) ───────────

  protected async listAllToolkits(): Promise<ToolProviderToolkit[]> {
    const composio = this.getRawClient();
    const toolkits: ToolKitItem[] = await composio.toolkits.get({});
    return toolkits.map(tk => ({
      slug: tk.slug,
      name: tk.name,
      description: tk.meta?.description,
      icon: tk.meta?.logo,
    }));
  }

  protected async listAllTools(opts: ListToolsOpts): Promise<ListToolsResult> {
    const composio = this.getRawClient();

    // Composio's `getRawComposioTools` query is a discriminated union — every
    // variant accepts `limit`, but the toolkits/search keys are exclusive in
    // the TS types. We build the variant we need, then cast to the union.
    //
    // When the caller doesn't scope to a specific toolkit, we fall back to
    // the admin allowlist so the SDK returns a flat list across allowed
    // toolkits in a single hop (vs. fanning out per toolkit).
    const limit = opts.perPage;
    const fallbackToolkits = this.allowedToolkits.length > 0 ? [...this.allowedToolkits] : undefined;
    const query: ComposioToolListParams = (
      opts.toolkit
        ? { toolkits: [opts.toolkit], limit, search: opts.search }
        : fallbackToolkits
          ? { toolkits: fallbackToolkits, limit, search: opts.search }
          : opts.search
            ? { search: opts.search, limit }
            : { toolkits: [] as string[], limit }
    ) as ComposioToolListParams;

    const rawTools: ComposioTool[] = await composio.tools.getRawComposioTools(query);

    const data = rawTools.map(tool => ({
      slug: tool.slug,
      name: tool.name ?? tool.slug,
      description: tool.description,
      toolkit: tool.toolkit?.slug ?? opts.toolkit ?? '',
    }));

    return {
      data,
      pagination: {
        page: opts.page ?? 1,
        perPage: limit,
        hasMore: limit !== undefined && rawTools.length >= limit,
      },
    };
  }

  // ── runtime ───────────────────────────────────────────────────────────

  async resolveToolsVNext(opts: ResolveToolsOpts): Promise<Record<string, ToolAction<any, any, any>>> {
    if (opts.toolSlugs.length === 0) return {};

    const identity = await this.resolveExecutionIdentity(opts);
    const composio = this.getMastraClient();
    const sessionToolSlugs = opts.toolSlugs.filter(slug => COMPOSIO_CONNECTION_MANAGEMENT_TOOLS.has(slug));
    const directToolSlugs = opts.toolSlugs.filter(slug => !COMPOSIO_CONNECTION_MANAGEMENT_TOOLS.has(slug));
    const mastraTools: MastraToolCollection = {};
    // Raw tags and toolkit per slug, read from the raw schema `tools.get`
    // hands `modifySchema` before `@composio/mastra` wraps it (the wrapper
    // drops tags). Feeds `requireToolApproval`.
    const rawToolFacts: ToolFactsBySlug = new Map();

    if (directToolSlugs.length > 0) {
      const modifiers = {
        // `connectedAccountId` is not threaded through Composio's `execute`
        // option bag in @composio/mastra; the only documented per-call hook
        // is `beforeExecute`, which receives the params object that flows
        // into the API call. Mutating `params.connectedAccountId` routes
        // the call to a specific account.
        beforeExecute: ({ params }: { params: { connectedAccountId?: string; userId?: string } }) => {
          if (identity.connectionId) {
            params.connectedAccountId = identity.connectionId;
          }
          return params;
        },
        modifySchema: captureToolFacts(rawToolFacts),
      };

      Object.assign(
        mastraTools,
        (await composio.tools.get(identity.userId, { tools: directToolSlugs }, modifiers)) as MastraToolCollection,
      );
    }

    if (sessionToolSlugs.length > 0) {
      const selectedToolkits = [
        ...new Set(
          Object.values(opts.toolMeta)
            .map(meta => meta.toolkit)
            .filter(
              (toolkit): toolkit is string =>
                typeof toolkit === 'string' && toolkit.toLowerCase() !== COMPOSIO_PROVIDER_ID,
            ),
        ),
      ];
      const session = await composio.sessions.create(identity.userId, {
        ...(selectedToolkits.length > 0 ? { toolkits: selectedToolkits } : {}),
        manageConnections: { enable: true, waitForConnections: true },
        sandbox: { enable: false },
      });
      const sessionTools = (await session.tools()) as MastraToolCollection;

      for (const slug of sessionToolSlugs) {
        const tool = sessionTools[slug];
        if (tool) mastraTools[slug] = tool;
      }
    }

    const result: Record<string, ToolAction<any, any, any>> = {};

    for (const [key, tool] of Object.entries(mastraTools)) {
      if (!tool) continue;
      const slug = (tool as { id?: string }).id ?? key;

      const descOverride = opts.toolMeta?.[slug]?.description;
      if (descOverride) {
        try {
          (tool as unknown as { description: string }).description = descOverride;
        } catch {
          // ignore
        }
      }

      const approval = this.approvalFor(
        slug,
        rawToolFacts.get(slug) ?? rawToolFacts.get(key),
        opts.toolMeta?.[slug]?.toolkit,
      );
      if (approval !== undefined) {
        (tool as unknown as { requireApproval?: boolean }).requireApproval = approval;
      }

      result[slug] = tool as ToolAction<any, any, any>;
    }

    return result;
  }

  /** The configured approval decision for one resolved tool; `undefined` leaves the tool as built. */
  private approvalFor(
    slug: string,
    facts: ToolFacts | undefined,
    fallbackToolkit: string | undefined,
  ): boolean | undefined {
    const policy = this.requireToolApproval;
    if (policy === undefined || policy === false) return undefined;
    if (policy === true) return true;
    const toolkit = facts?.toolkit ?? fallbackToolkit;
    return policy({ slug, ...(toolkit ? { toolkit } : {}), tags: facts?.tags ?? [] }) === true;
  }

  /**
   * Run the configured `userIdResolver` and validate its result. Returns
   * the resolved user id, or `undefined` when no resolver is configured or
   * the resolver declined (returned `undefined`). Throws when the resolver
   * returns an empty or non-string value — an empty execution identity must
   * fail closed instead of silently falling back.
   */
  private async runUserIdResolver(input: ComposioUserIdResolverInput): Promise<string | undefined> {
    if (!this.userIdResolver) return undefined;
    const resolved = await this.userIdResolver(input);
    if (resolved === undefined) return undefined;
    if (typeof resolved !== 'string') {
      throw new Error('[composio] userIdResolver must return a non-empty string or undefined');
    }
    const normalized = resolved.trim();
    if (normalized.length === 0) {
      throw new Error('[composio] userIdResolver must return a non-empty string or undefined');
    }
    return normalized;
  }

  /**
   * Resolve the effective Composio execution identity for one
   * `resolveToolsVNext` call: the `userId` bucket to fetch tools under and
   * the exact `connectedAccountId` to route execution to (absent = let
   * Composio auto-resolve within the bucket).
   */
  private async resolveExecutionIdentity(opts: ResolveToolsOpts): Promise<{ userId: string; connectionId?: string }> {
    // The unpinned caller-supplied bootstrap fan-out passes the user bucket
    // itself as `connectionId` (connectionId === authorId). That is not an
    // account pin, so execution must stay on Composio's per-bucket
    // auto-resolve.
    const hasAccountPin = opts.connectionId !== opts.authorId;

    if (opts.kind === 'invoker') {
      const resolvedUserId = await this.runUserIdResolver({
        requestContext: opts.requestContext,
        toolkit: opts.toolkit,
        connectedAccountId: opts.connectionId,
      });
      // Invoker connections execute as the authenticated user — never the
      // Memory resource id — against the exact stored account pin (which may
      // be an account another user shared with the invoker via Composio ACL).
      return {
        userId: resolvedUserId ?? resolveInvokerUserId(opts.requestContext),
        connectionId: opts.connectionId,
      };
    }

    if (opts.scope === 'caller-supplied') {
      const resolvedUserId = await this.runUserIdResolver({
        requestContext: opts.requestContext,
        toolkit: opts.toolkit,
        connectedAccountId: hasAccountPin ? opts.connectionId : undefined,
      });
      return {
        userId: resolvedUserId ?? resolveInternalUserId(opts.requestContext),
        connectionId: hasAccountPin ? opts.connectionId : undefined,
      };
    }

    // Author-bound (and legacy) connections: the runtime fan-out passes the
    // agent author's id explicitly. Use it as the Composio user bucket so the
    // pin resolves for any invoker (not just the original author), and always
    // route execution to the pinned account.
    return {
      userId: opts.authorId && opts.authorId.length > 0 ? opts.authorId : resolveInternalUserId(opts.requestContext),
      connectionId: opts.connectionId,
    };
  }

  /**
   * Tools for a catalog that is built once and shared by every caller (for
   * example a ToolSearch catalog registered when the agent is constructed).
   *
   * {@link resolveToolsVNext} binds one identity when it resolves. Here the
   * schemas are resolved once and each call resolves its own identity from
   * its own request context through `resolveIdentity`, so one registered
   * tool executes as whichever user called it, against that user's account.
   * A call whose identity does not resolve throws before reaching Composio.
   * `requireToolApproval` applies as in {@link resolveToolsVNext}.
   */
  async resolveCatalogTools(opts: ResolveComposioCatalogToolsOpts): Promise<Record<string, ToolAction<any, any, any>>> {
    if (opts.toolSlugs.length === 0) return {};
    const slugs = opts.toolSlugs.filter(slug => !COMPOSIO_CONNECTION_MANAGEMENT_TOOLS.has(slug));
    if (slugs.length !== opts.toolSlugs.length) {
      throw new Error('[composio] connection-management tools need a caller-scoped session, not a shared catalog');
    }
    const callIdentity = new AsyncLocalStorage<ComposioCallIdentity>();
    const rawToolFacts: ToolFactsBySlug = new Map();
    const modifiers = {
      beforeExecute: ({ params }: { params: { connectedAccountId?: string; userId?: string } }) => {
        const identity = callIdentity.getStore();
        if (!identity) {
          throw new Error('[composio] a catalog tool executed without its call identity');
        }
        params.userId = identity.userId;
        if (identity.connectedAccountId) {
          params.connectedAccountId = identity.connectedAccountId;
        } else {
          delete params.connectedAccountId;
        }
        return params;
      },
      modifySchema: captureToolFacts(rawToolFacts),
    };
    const resolved = (await this.getMastraClient().tools.get(
      CATALOG_SCHEMA_USER_ID,
      { tools: slugs },
      modifiers,
    )) as MastraToolCollection;

    const result: Record<string, ToolAction<any, any, any>> = {};
    for (const [key, tool] of Object.entries(resolved)) {
      if (!tool) continue;
      const slug = (tool as { id?: string }).id ?? key;
      const facts = rawToolFacts.get(slug) ?? rawToolFacts.get(key);
      const toolkit = facts?.toolkit ?? opts.toolMeta?.[slug]?.toolkit;
      const descOverride = opts.toolMeta?.[slug]?.description;
      if (descOverride) {
        (tool as unknown as { description: string }).description = descOverride;
      }
      const execute = (tool as { execute?: (input: unknown, context?: unknown) => Promise<unknown> }).execute;
      if (typeof execute !== 'function') continue;
      (tool as { execute?: (input: unknown, context?: unknown) => Promise<unknown> }).execute = async (
        input: unknown,
        context?: unknown,
      ) => {
        const { requestContext, abortSignal } =
          (context as { requestContext?: RequestContext; abortSignal?: AbortSignal } | undefined) ?? {};
        // A Stop that lands before or while the identity resolves must not
        // reach Composio: the action may already be approved and act on the
        // user's external account.
        abortSignal?.throwIfAborted();
        const identity = await opts.resolveIdentity({
          requestContext,
          toolSlug: slug,
          ...(toolkit ? { toolkit } : {}),
        });
        abortSignal?.throwIfAborted();
        const userId = typeof identity?.userId === 'string' ? identity.userId.trim() : '';
        if (!identity || userId.length === 0 || userId === CATALOG_SCHEMA_USER_ID) {
          throw new Error(`[composio] no execution identity for ${slug}`);
        }
        return callIdentity.run(
          {
            userId,
            ...(identity.connectedAccountId ? { connectedAccountId: identity.connectedAccountId } : {}),
          },
          () => execute.call(tool, input, context),
        );
      };
      const approval = this.approvalFor(slug, facts, toolkit);
      if (approval !== undefined) {
        (tool as unknown as { requireApproval?: boolean }).requireApproval = approval;
      }
      result[slug] = tool as ToolAction<any, any, any>;
    }
    return result;
  }

  // ── auth surface ──────────────────────────────────────────────────────

  async authorize(opts: AuthorizeOpts): Promise<{ url: string; authId: string }> {
    const composio = this.getRawClient();
    const { id: authConfigId, authScheme } = await this.resolveAuthConfig(opts.toolkit);

    // `connectionId` carries the internal user bucket for the runtime fan-out;
    // for authorize we treat it as the Composio `userId` so the new connected
    // account lands under the same bucket as the agent's resolved identity.
    const internalUserId = opts.connectionId || DEFAULT_INTERNAL_USER_ID;

    // `config` carries provider-specific user-supplied fields (e.g. Confluence
    // subdomain) collected by the picker via `listConnectionFields`. When it is
    // present we must use `connectedAccounts.initiate`, which accepts a
    // discriminated `{ authScheme, val }` config for programmatic account
    // creation. Composio's non-deprecated `connectedAccounts.link` (hosted
    // Connect Link) has no `config` parameter, so it cannot carry these fields.
    const initiateConfig =
      opts.config && Object.keys(opts.config).length > 0 && authScheme
        ? ({ authScheme, val: opts.config } as unknown as Parameters<
            typeof composio.connectedAccounts.initiate
          >[2] extends infer O
            ? O extends { config?: infer C }
              ? C
              : never
            : never)
        : undefined;

    // Only `link` accepts the `experimental` block, so a SHARED account that
    // also needs custom config fields cannot be created. Fail instead of
    // silently creating a PRIVATE account.
    const createShared = opts.scope === 'shared' && this.sharedConnections !== undefined;
    if (createShared && initiateConfig) {
      throw new Error(
        `[composio] Cannot create a shared connection for toolkit "${opts.toolkit}": Composio SHARED accounts do not support custom connection fields.`,
      );
    }

    // Prefer `link` for the Composio-managed OAuth redirect flow: `initiate`
    // is deprecated for managed OAuth. Both reject a second ACTIVE account per
    // (user, auth config) unless `allowMultiple` is set, and this provider
    // supports multiple connections per toolkit. Fall back to `initiate` only
    // when custom `config` fields are supplied, since `link` cannot forward them.
    const acl = this.sharedConnections?.acl;
    const request = initiateConfig
      ? await composio.connectedAccounts.initiate(internalUserId, authConfigId, {
          allowMultiple: true,
          config: initiateConfig,
        })
      : await composio.connectedAccounts.link(internalUserId, authConfigId, {
          allowMultiple: true,
          ...(createShared
            ? { experimental: { accountType: 'SHARED' as const, ...(acl ? { aclConfigForShared: acl } : {}) } }
            : {}),
        });

    if (!request.redirectUrl) {
      throw new Error(`[composio] authorize did not return a redirectUrl for toolkit "${opts.toolkit}"`);
    }

    return { url: request.redirectUrl, authId: request.id };
  }

  async listConnectionFields({ toolkit }: { toolkit: string }): Promise<ConnectionField[]> {
    const composio = this.getRawClient();
    const { authScheme } = await this.resolveAuthConfig(toolkit);
    if (!authScheme) {
      // Without a known auth scheme we can't query the field schema — fall
      // back to no fields rather than blocking the user.
      return [];
    }
    const fields = await composio.toolkits.getConnectedAccountInitiationFields(toolkit, authScheme, {
      requiredOnly: false,
    });
    return fields.map(f => ({
      name: f.name,
      displayName: f.displayName,
      description: f.description,
      type: coerceFieldType(f.type),
      required: f.required ?? false,
      default: f.default ?? undefined,
    }));
  }

  async getAuthStatus(authId: string): Promise<AuthFlowStatus> {
    const composio = this.getRawClient();
    const account = await composio.connectedAccounts.get(authId);
    switch (account.status) {
      case 'ACTIVE':
        return 'completed';
      case 'INITIALIZING':
      case 'INITIATED':
        return 'pending';
      case 'FAILED':
      case 'EXPIRED':
      case 'INACTIVE':
        return 'failed';
      default:
        return 'pending';
    }
  }

  async getConnectionStatus(opts: {
    items: Array<{ connectionId: string; toolkit: string }>;
  }): Promise<Record<string, { connected: boolean }>> {
    if (opts.items.length === 0) return {};

    const composio = this.getRawClient();
    const toolkitSlugs = Array.from(new Set(opts.items.map(i => i.toolkit)));

    // One SDK call per `getConnectionStatus`, regardless of N items.
    // Filter by all referenced toolkits, then bucket locally by id.
    // `accountType: 'ALL'` so pinned SHARED accounts resolve too; Composio
    // lists PRIVATE accounts only by default.
    const list: ConnectedAccountListResponse = await composio.connectedAccounts.list({
      toolkitSlugs,
      accountType: 'ALL',
    });

    const liveById = new Map<string, { status: string; isDisabled: boolean }>();
    for (const item of list.items) {
      liveById.set(item.id, { status: item.status, isDisabled: item.isDisabled });
    }

    const result: Record<string, { connected: boolean }> = {};
    for (const { connectionId } of opts.items) {
      const live = liveById.get(connectionId);
      result[connectionId] = { connected: live ? live.status === 'ACTIVE' && !live.isDisabled : false };
    }
    return result;
  }

  async listConnections(opts: ComposioListConnectionsOpts): Promise<ComposioListConnectionsResult> {
    const composio = this.getRawClient();
    const page = opts.page ?? 1;
    const perPage = clampLimit(opts.perPage);

    // Normalize userIds[] / userId. Empty array = no buckets to list against,
    // short-circuit to avoid an unbounded Composio response.
    const userIds = resolveUserIds(opts);
    if (userIds && userIds.length === 0) {
      return { items: [], pagination: { page, perPage, hasMore: false } };
    }

    // Composio SDK uses cursor-based pagination on the wire. We surface
    // page-based pagination to keep the Mastra contract consistent with every
    // other list API. For now we only fetch the first page (page=1); paginated
    // requests for page > 1 are a follow-up — the UI does not yet paginate.
    //
    // Composio lists PRIVATE accounts only by default, so request `'ALL'` to
    // include SHARED accounts owned by the requested buckets.
    const owned: ConnectedAccountListResponse = await composio.connectedAccounts.list({
      toolkitSlugs: [opts.toolkit],
      ...(userIds ? { userIds } : {}),
      accountType: 'ALL',
      limit: perPage,
    });
    const lists = [owned];
    let sharedItems: ConnectedAccountListResponse['items'] = [];

    // SHARED accounts owned by other users are usable by the requested buckets
    // when their ACL grants access. List them separately and evaluate the ACL
    // locally so the result doesn't depend on how Composio combines `userIds`
    // with `accountType`. `ownedOnly` skips them.
    if (userIds && opts.ownedOnly !== true) {
      const shared: ConnectedAccountListResponse = await composio.connectedAccounts.list({
        toolkitSlugs: [opts.toolkit],
        accountType: 'SHARED',
        limit: perPage,
      });
      sharedItems = (shared.items ?? []).filter(account =>
        userIds.some(userId => isSharedAccountAccessible(account.experimental?.aclConfigForShared, userId)),
      );
      lists.push(shared);
    }

    // Defensive: tolerate undocumented SDK shape drift where `items` is
    // missing or `nextCursor` is `null`/`undefined`/`''`.
    //
    // The SDK's response schema drops the raw `user_id`, so an account's owner
    // is known only from which list it came from. The first list holds the
    // requested buckets' own accounts; with one bucket, that bucket owns it.
    const items: ComposioExistingConnection[] = [];
    const seen = new Set<string>();
    const add = (account: ConnectedAccountListResponse['items'][number], isOwned: boolean) => {
      if (seen.has(account.id)) return;
      seen.add(account.id);
      const accountType = account.experimental?.accountType;
      items.push({
        connectionId: account.id,
        status: mapComposioStatus(account.status, account.isDisabled),
        createdAt: account.createdAt,
        ...(isOwned && userIds?.length === 1 ? { authorId: userIds[0] } : {}),
        owned: isOwned,
        ...(accountType ? { accountType } : {}),
      });
    };
    for (const account of owned.items ?? []) add(account, true);
    for (const account of sharedItems) add(account, false);

    const hasMore = lists.some(list => {
      const nextCursor = (list as { nextCursor?: string | null }).nextCursor ?? null;
      return typeof nextCursor === 'string' && nextCursor.length > 0;
    });
    return { items, pagination: { page, perPage, hasMore } };
  }

  /**
   * Revoke a Composio connected account via
   * `DELETE /api/v3/connected_accounts/:nanoid`. Composio performs a soft
   * delete and responds with `{ success: boolean }`.
   *
   * Treats a 404 (account already deleted or never existed) as success so
   * the caller can drop its local pin without an error path. A `success:
   * false` response means the provider refused the delete and is surfaced
   * as an error so the caller does not delete its local row.
   */
  async revokeConnection(connectionId: string): Promise<void> {
    const composio = this.getRawClient();
    try {
      const res = (await composio.connectedAccounts.delete(connectionId)) as { success?: boolean } | undefined;
      if (res && res.success === false) {
        throw new Error(`Composio refused to delete connected account ${connectionId} (success=false)`);
      }
    } catch (err) {
      if (isNotFoundError(err)) return;
      throw err;
    }
  }

  async getHealth(): Promise<ToolProviderHealth> {
    try {
      const composio = this.getRawClient();
      await composio.toolkits.get({ limit: 1 } as Parameters<typeof composio.toolkits.get>[0]);
      return { ok: true };
    } catch (err) {
      return {
        ok: false,
        message: err instanceof Error ? err.message : 'Composio SDK reachability check failed',
      };
    }
  }

  // ── helpers ───────────────────────────────────────────────────────────

  /**
   * Resolve the single ENABLED auth config for `toolkit`. Throws if zero
   * or multiple configs match — the admin must enable exactly one in the
   * Composio dashboard before agents can connect.
   */
  private async resolveAuthConfig(toolkit: string): Promise<{ id: string; authScheme?: ComposioAuthScheme }> {
    const composio = this.getRawClient();
    const response = await composio.authConfigs.list({ toolkit });
    const enabled = response.items.filter(item => item.status === 'ENABLED');

    if (enabled.length === 0) {
      throw new Error(
        `[composio] No ENABLED auth config for toolkit "${toolkit}". Enable one in the Composio dashboard.`,
      );
    }
    if (enabled.length > 1) {
      const ids = enabled.map(item => item.id).join(', ');
      throw new Error(
        `[composio] Multiple ENABLED auth configs for toolkit "${toolkit}" (${ids}). Keep exactly one enabled.`,
      );
    }
    return { id: enabled[0]!.id, authScheme: enabled[0]!.authScheme };
  }
}

type ComposioAuthScheme = NonNullable<
  Awaited<ReturnType<Composio['authConfigs']['list']>>['items'][number]['authScheme']
>;

/**
 * Composio's ACL rule for SHARED accounts: deny list wins, then
 * `allowAllUsers`, then the allow list, otherwise deny. Owners are not covered
 * here; they are listed by bucket.
 */
function isSharedAccountAccessible(acl: Partial<ComposioSharedAccountAcl> | undefined, userId: string): boolean {
  if (!acl) return false;
  if (acl.notAllowedUserIds?.includes(userId)) return false;
  if (acl.allowAllUsers) return true;
  return acl.allowedUserIds?.includes(userId) ?? false;
}

/**
 * Best-effort 404 detection across the various error shapes the Composio
 * SDK surfaces (typed error with `statusCode`, HTTP-like error with
 * `status`, or a plain message containing "404" / "not found").
 */
function isNotFoundError(err: unknown): boolean {
  if (!err || typeof err !== 'object') return false;
  const e = err as { statusCode?: number; status?: number; message?: string };
  if (e.statusCode === 404 || e.status === 404) return true;
  const msg = typeof e.message === 'string' ? e.message.toLowerCase() : '';
  return msg.includes('not found') || msg.includes('404');
}

/**
 * Composio reports a free-form `type` string. Map common values to our
 * generic ConnectionField type vocabulary; everything else falls back to
 * `'string'`.
 */
function coerceFieldType(type: string): 'string' | 'number' | 'boolean' {
  switch (type.toLowerCase()) {
    case 'number':
    case 'integer':
    case 'int':
    case 'float':
      return 'number';
    case 'bool':
    case 'boolean':
      return 'boolean';
    default:
      return 'string';
  }
}

/**
 * Map Composio account status + `isDisabled` to the {@link ExistingConnection}
 * status vocabulary surfaced to the picker UI.
 */
function mapComposioStatus(status: string, isDisabled: boolean): ExistingConnection['status'] {
  if (isDisabled) return 'inactive';
  switch (status) {
    case 'ACTIVE':
      return 'active';
    case 'INITIALIZING':
    case 'INITIATED':
      return 'pending';
    case 'FAILED':
    case 'EXPIRED':
      return 'failed';
    case 'INACTIVE':
      return 'inactive';
    default:
      return 'pending';
  }
}

/** Tags and toolkit Composio reports for one tool, captured before the Mastra wrapper drops them. */
type ToolFacts = { toolkit?: string; tags: readonly string[] };
type ToolFactsBySlug = Map<string, ToolFacts>;

/**
 * A `modifySchema` modifier that records each raw tool's toolkit and tags and
 * returns the schema unchanged. The SDK passes `toolkitSlug` as `"unknown"`
 * when Composio omits the toolkit; that is recorded as absent so a caller's
 * `toolMeta` toolkit still applies.
 */
function captureToolFacts(into: ToolFactsBySlug) {
  return ({ toolSlug, toolkitSlug, schema }: { toolSlug: string; toolkitSlug: string; schema: ComposioTool }) => {
    const reported = [toolkitSlug, schema.toolkit?.slug].find(
      (slug): slug is string => typeof slug === 'string' && slug.length > 0 && slug !== 'unknown',
    );
    into.set(toolSlug, {
      ...(reported ? { toolkit: reported } : {}),
      tags: Array.isArray(schema.tags) ? schema.tags.filter((tag): tag is string => typeof tag === 'string') : [],
    });
    return schema;
  };
}

// Mirror of `MASTRA_USER_KEY` from `@mastra/server`. Inlined to avoid a
// reverse dependency from `editor` onto `server`.
const MASTRA_USER_KEY = 'mastra__user';

function readAuthenticatedUserId(requestContext?: RequestContext): string | undefined {
  const user = requestContext?.getRaw(MASTRA_USER_KEY);
  if (!user || typeof user !== 'object' || !('id' in user)) return undefined;
  return typeof user.id === 'string' && user.id.length > 0 ? user.id : undefined;
}

/**
 * Read the internal user id (Composio `userId`) from per-request context.
 *
 * The runtime fan-out is responsible for stamping the agent's resolved
 * author id (or `'default'`) into `requestContext` under
 * {@link MASTRA_RESOURCE_ID_KEY}.
 */
function resolveInternalUserId(requestContext?: RequestContext): string {
  const resourceId = requestContext?.getRaw(MASTRA_RESOURCE_ID_KEY);
  if (typeof resourceId === 'string' && resourceId.length > 0) {
    return resourceId;
  }

  return readAuthenticatedUserId(requestContext) ?? DEFAULT_INTERNAL_USER_ID;
}

/**
 * Read the authenticated invoker's Composio `userId` from per-request
 * context. Invoker connections must never fall back to the Memory resource id
 * because a project or thread is not an authenticated connector principal.
 */
function resolveInvokerUserId(requestContext?: RequestContext): string {
  const userId = readAuthenticatedUserId(requestContext);
  if (userId) return userId;
  throw new Error('[composio] kind "invoker" requires an authenticated user or a userIdResolver result');
}

/**
 * Resolve `userIds[]` from `listConnections` opts.
 *
 * - If `userIds` is provided, use it as-is (including empty array, which
 *   means "no buckets to list against").
 * - If `userId` is provided, normalize to `[userId]`.
 * - Otherwise fall back to the default internal user id (single-bucket).
 */
function resolveUserIds(opts: ListConnectionsOpts): string[] | undefined {
  if (Array.isArray(opts.userIds)) return opts.userIds;
  if (typeof opts.userId === 'string' && opts.userId.length > 0) return [opts.userId];
  return [DEFAULT_INTERNAL_USER_ID];
}

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 200;

function clampLimit(limit: number | undefined): number {
  if (typeof limit !== 'number' || !Number.isFinite(limit) || limit <= 0) {
    return DEFAULT_LIMIT;
  }
  return Math.min(Math.floor(limit), MAX_LIMIT);
}
