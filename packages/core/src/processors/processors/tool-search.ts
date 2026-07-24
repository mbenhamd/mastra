import { z } from 'zod/v4';
import { MASTRA_THREAD_ID_KEY } from '../../request-context';
import type { RequestContext } from '../../request-context';
import { createTool } from '../../tools';
import type { Tool } from '../../tools';
import { BM25Index } from '../../workspace/search/bm25';
import type { TokenizeOptions } from '../../workspace/search/bm25';
import type { ProcessInputStepArgs, Processor } from '../index';
import type { LoadedToolStore, LoadedToolStoreContext } from './tool-search-stores';
import {
  LegacyMapLoadedToolStore,
  ContextLoadedToolStore,
  TOOL_SEARCH_AUTO_LOAD_ACTIVATION_TYPE,
  TOOL_SEARCH_AUTO_LOAD_ACTIVATION_VERSION,
} from './tool-search-stores';

export type ToolSearchFilterPhase = 'search' | 'load' | 'active';

export type ToolSearchFilterArgs = {
  /** The resolved tool id. */
  toolName: string;
  tool: Tool<any, any>;
  requestContext?: RequestContext;
  phase: ToolSearchFilterPhase;
};

/**
 * Configuration options for ToolSearchProcessor
 */
export interface ToolSearchProcessorOptions {
  /**
   * All tools that can be searched and loaded dynamically.
   * These tools are not immediately available - they must be discovered via search and loaded on demand.
   */
  tools: Record<string, Tool<any, any>>;

  /**
   * Configuration for the search behavior
   */
  search?: {
    /**
     * Maximum number of tools to return in search results
     * @default 5
     */
    topK?: number;

    /**
     * Minimum relevance score (0-1) for including a tool in search results
     * @default 0
     */
    minScore?: number;

    /**
     * When true, tools returned by `search_tools` are activated immediately as a
     * side effect of the search — there is no separate `load_tool` step and the
     * `load_tool` meta-tool is not exposed. The discovered tools become available
     * on the model's next turn.
     *
     * This collapses the two-turn `search -> load -> use` flow into a single
     * `search -> use` flow, mirroring native provider tool-search features that
     * auto-expand the discovered tool references. Discovery stays model-driven;
     * only the explicit load decision is removed.
     *
     * Because every match is activated, keep `topK` conservative in this mode.
     * @default false
     */
    autoLoad?: boolean;
  };

  /**
   * Where loaded-tool state lives. The `'context'` store is opt-in.
   *
   * - `'in-memory'` (default): the original behavior — loaded state lives in an
   *   in-memory `Map<threadId, Set>` with TTL cleanup (see `ttl`). Lost on restart;
   *   anonymous requests share a `'default'` entry.
   * - `'context'`: derived from canonical assistant tool results in the
   *   conversation. `load_tool` results must contain `loaded`; `search_tools`
   *   results activate tools only when auto-load emitted an explicit activation
   *   receipt. Restart-safe, requires no memory, and de-loads automatically when
   *   the result block is no longer present in the messages.
   *
   * @default 'in-memory'
   */
  storage?: 'in-memory' | 'context';

  /**
   * Time-to-live for in-memory thread state, in milliseconds. Only applies to the
   * default `storage: 'in-memory'` store. After this duration of inactivity, thread
   * state is eligible for cleanup. Set to 0 to disable cleanup.
   *
   * Ignored for `storage: 'context'`.
   *
   * @default 3600000 (1 hour)
   */
  ttl?: number;

  /**
   * Optional request-aware hook for filtering tools during search, load, and active tool injection.
   * Return false to hide or block a tool for the current request.
   */
  filter?: (args: ToolSearchFilterArgs) => boolean | Promise<boolean>;
}

/**
 * Search result with ranking score
 */
interface SearchResult {
  name: string;
  description: string;
  score: number;
}

/**
 * Tokenization options tuned for tool names and descriptions.
 * Splits on underscores, hyphens, and punctuation (common in tool IDs).
 * No stopwords filtering since tool descriptions are short.
 */
const TOOL_SEARCH_TOKENIZE_OPTIONS: TokenizeOptions = {
  lowercase: true,
  removePunctuation: false,
  minLength: 2,
  stopwords: new Set(),
  splitPattern: /[\s\-_.,;:!?()[\]{}'"]+/,
};

const TOOL_SEARCH_META_TOOL_NAMES = new Set(['search_tools', 'load_tool']);

/**
 * Processor that enables dynamic tool discovery and loading.
 *
 * Instead of providing all tools to the agent upfront, this processor:
 * 1. Gives the agent two meta-tools: search_tools and load_tool
 * 2. Agent searches for relevant tools using keywords
 * 3. Agent loads specific tools into the conversation on demand
 * 4. Loaded tools become immediately available for use
 *
 * This pattern dramatically reduces context usage when working with many tools (100+).
 *
 * @example
 * ```typescript
 * const toolSearch = new ToolSearchProcessor({
 *   tools: {
 *     createIssue: githubTools.createIssue,
 *     sendEmail: emailTools.send,
 *     // ... 100+ tools
 *   },
 *   search: { topK: 5, minScore: 0 },
 *   ttl: 3600000, // 1 hour (default)
 * });
 *
 * const agent = new Agent({
 *   name: 'my-agent',
 *   inputProcessors: [toolSearch],
 *   tools: {}, // Always-available tools (if any)
 * });
 * ```
 */
export class ToolSearchProcessor implements Processor<'tool-search'> {
  readonly id = 'tool-search';
  readonly name = 'Tool Search Processor';
  readonly description = 'Enables dynamic tool discovery and loading via search';

  private allTools: Record<string, Tool<any, any>>;
  private toolsById = new Map<string, Tool<any, any>>();
  private searchConfig: Required<NonNullable<ToolSearchProcessorOptions['search']>>;
  private filter?: ToolSearchProcessorOptions['filter'];

  /** Pluggable backend for loaded-tool state. */
  private store: LoadedToolStore;

  /**
   * Context-backed activation receipts are caller-visible conversation data.
   * They are durable evidence, but not an authorization credential: Agent APIs
   * accept assistant-shaped history from callers. Re-run the load policy when
   * materializing those names so forged or stale receipts cannot bypass it.
   */
  private reauthorizeLoadedNames: boolean;

  /** BM25 index for tool search */
  private bm25Index: BM25Index;
  /** Map from tool ID to full description (for result formatting) */
  private toolDescriptions = new Map<string, string>();

  constructor(options: ToolSearchProcessorOptions) {
    this.allTools = options.tools;
    this.validateToolCatalog();
    this.filter = options.filter;
    this.searchConfig = {
      topK: options.search?.topK ?? 5,
      minScore: options.search?.minScore ?? 0,
      autoLoad: options.search?.autoLoad ?? false,
    };

    const storage = options.storage ?? 'in-memory';
    this.reauthorizeLoadedNames = storage === 'context';

    this.store =
      storage === 'context' ? new ContextLoadedToolStore() : new LegacyMapLoadedToolStore({ ttl: options.ttl });

    // Create BM25 index with tool-search-specific tokenization
    this.bm25Index = new BM25Index({}, TOOL_SEARCH_TOKENIZE_OPTIONS);

    // Index all tools
    this.indexTools();
  }

  /**
   * Get the thread ID from the request context, or undefined when no thread is active.
   * Both stores tolerate an undefined thread ID.
   */
  private getThreadId(args: ProcessInputStepArgs): string | undefined {
    return (args.requestContext?.get(MASTRA_THREAD_ID_KEY) as string | undefined) || undefined;
  }

  private makeStoreContext(args: ProcessInputStepArgs): LoadedToolStoreContext {
    return { threadId: this.getThreadId(args), args };
  }

  private findToolById(toolId: string): Tool<any, any> | undefined {
    return this.toolsById.get(toolId);
  }

  private findToolForDynamicName(toolName: string): Tool<any, any> | undefined {
    const toolByKey = this.allTools[toolName];
    const toolById = this.findToolById(toolName);
    return toolById ?? toolByKey;
  }

  private validateToolCatalog(): void {
    const keyOwners = new Map(Object.entries(this.allTools));

    for (const [key, tool] of Object.entries(this.allTools)) {
      if (TOOL_SEARCH_META_TOOL_NAMES.has(key) || TOOL_SEARCH_META_TOOL_NAMES.has(tool.id)) {
        throw new Error(
          `ToolSearchProcessor tool key and id must not use reserved meta-tool names: key="${key}", id="${tool.id}".`,
        );
      }

      const duplicateId = this.toolsById.get(tool.id);
      if (duplicateId) {
        throw new Error(`ToolSearchProcessor requires unique tool ids; duplicate id "${tool.id}" was provided.`);
      }
      this.toolsById.set(tool.id, tool);
    }

    for (const [key, tool] of Object.entries(this.allTools)) {
      const idOwner = this.toolsById.get(key);
      if (idOwner && idOwner !== tool) {
        const conflictingKey = [...keyOwners.entries()].find(([, candidate]) => candidate === idOwner)?.[0];
        throw new Error(
          `ToolSearchProcessor tool key "${key}" conflicts with the id of tool key "${conflictingKey ?? '<unknown>'}". Tool keys and ids must resolve unambiguously.`,
        );
      }
    }
  }

  private async isToolAllowed(
    tool: Tool<any, any>,
    requestContext: RequestContext | undefined,
    phase: ToolSearchFilterPhase,
  ): Promise<boolean> {
    if (!this.filter) {
      return true;
    }

    try {
      return await this.filter({ toolName: tool.id, tool, requestContext, phase });
    } catch {
      return false;
    }
  }

  private async getSuggestedToolNames(toolName: string, requestContext?: RequestContext): Promise<string[]> {
    const matchesToolName = (name: string) =>
      name.toLowerCase().includes(toolName.toLowerCase()) || toolName.toLowerCase().includes(name.toLowerCase());

    if (!this.filter) {
      return Object.keys(this.allTools).filter(matchesToolName);
    }

    const allowedNames: string[] = [];

    for (const name of Object.keys(this.allTools)) {
      if (!matchesToolName(name)) continue;

      const tool = this.findToolForDynamicName(name);
      if (!tool) continue;

      const isAllowed = await this.isToolAllowed(tool, requestContext, 'load');
      if (isAllowed) {
        allowedNames.push(name);
        if (allowedNames.length >= 3) break;
      }
    }

    return allowedNames;
  }

  /**
   * Get loaded tools as Tool objects for the given loaded names.
   * Loaded names are resolved by the configured store.
   */
  private async getLoadedTools(
    loadedNames: Set<string>,
    requestContext?: RequestContext,
  ): Promise<Record<string, Tool<any, any>>> {
    const loadedTools: Record<string, Tool<any, any>> = {};

    for (const toolName of loadedNames) {
      const tool = this.findToolForDynamicName(toolName);
      if (tool) {
        if (this.reauthorizeLoadedNames && !(await this.isToolAllowed(tool, requestContext, 'load'))) {
          continue;
        }
        const isAllowed = await this.isToolAllowed(tool, requestContext, 'active');
        if (isAllowed) {
          loadedTools[toolName] = tool;
        }
      }
    }

    return loadedTools;
  }

  /**
   * Get loaded tools for the given request context.
   * Used by agent resume paths to rebuild tool executors after approval suspension.
   *
   * Resolution:
   * - If `stepArgs` are supplied, resolve through the store with the live messages.
   * - Otherwise (resume path), the context store reconstructs from supplied
   *   persisted messages. The in-memory store resolves by thread ID.
   */
  public async getLoadedToolsForRequestContext(args?: {
    requestContext?: RequestContext;
    stepArgs?: ProcessInputStepArgs;
    /** Persisted messages supplied by a resume boundary before a live step exists. */
    messages?: ProcessInputStepArgs['messages'];
  }): Promise<Record<string, Tool<any, any>>> {
    if (args?.stepArgs) {
      const loadedNames = await this.store.getLoadedNames(this.makeStoreContext(args.stepArgs));
      // Fall back to the step's own request context so active-phase filtering still
      // runs when the caller only supplies stepArgs.
      return this.getLoadedTools(loadedNames, args.requestContext ?? args.stepArgs.requestContext);
    }

    const threadId = (args?.requestContext?.get(MASTRA_THREAD_ID_KEY) as string | undefined) || undefined;
    const loadedNames = await this.store.getLoadedNames({
      threadId,
      args: undefined,
      messages: args?.messages,
    });
    return this.getLoadedTools(loadedNames, args?.requestContext);
  }

  /**
   * Clear loaded tools for a specific thread (useful for testing).
   *
   * This is a no-op for the `'context'` store. Its durable state is owned by
   * conversation messages and its same-run bridge is owned by request state.
   *
   * @param threadId - The thread ID to clear, or 'default' if not provided
   */
  public clearState(threadId: string = 'default'): void {
    this.store.clearState(threadId);
  }

  /**
   * Clear all thread state for this processor instance (useful for testing).
   *
   * This is a no-op for the `'context'` store.
   */
  public clearAllState(): void {
    this.store.clearAllState();
  }

  /** Release cleanup timers and process-local loaded-tool state. */
  public dispose(): void {
    this.store.dispose();
  }

  /**
   * Get statistics about current in-memory thread state (useful for monitoring).
   *
   * Only meaningful for the default `storage: 'in-memory'` store; returns zero
   * counts for the `'context'` store.
   */
  public getStateStats(): { threadCount: number; oldestAccessTime: number | null } {
    return this.store instanceof LegacyMapLoadedToolStore
      ? this.store.getStateStats()
      : { threadCount: 0, oldestAccessTime: null };
  }

  /**
   * Manually trigger cleanup of stale in-memory state (useful for testing).
   *
   * Only affects the default `storage: 'in-memory'` store; returns 0 for the
   * `'context'` store.
   *
   * @returns Number of threads cleaned up
   */
  public cleanupNow(): number {
    return this.store instanceof LegacyMapLoadedToolStore ? this.store.cleanupStaleState() : 0;
  }

  /**
   * Index all tools into the BM25 index
   */
  private indexTools(): void {
    for (const tool of Object.values(this.allTools)) {
      const name = tool.id;
      const description = tool.description || '';
      this.bm25Index.add(name, `${name} ${description}`);
      this.toolDescriptions.set(name, description);
    }
  }

  /**
   * Search for tools matching the query using BM25 ranking
   * with name-match boosting.
   *
   * @param query - Search keywords
   * @returns Array of matching tools with scores, sorted by relevance
   */
  private async searchTools(query: string, requestContext?: RequestContext): Promise<SearchResult[]> {
    if (this.bm25Index.size === 0) return [];

    // Get BM25 results (request more than topK to allow for re-ranking after boosting).
    // When filtering is enabled, inspect every BM25 match so denied high-ranking tools
    // do not prevent lower-ranking allowed tools from filling the result set.
    const searchLimit = this.filter ? this.bm25Index.size : this.searchConfig.topK * 2;
    const bm25Results = this.bm25Index.search(query, searchLimit, 0);

    if (bm25Results.length === 0) return [];

    // Apply name-match boosting on top of BM25 scores
    const queryTokens = query
      .toLowerCase()
      .split(/[\s\-_.,;:!?()[\]{}'"]+/)
      .filter(t => t.length > 1);

    const boostedResults = bm25Results.map(result => {
      let score = result.score;
      const nameLower = result.id.toLowerCase();

      for (const term of queryTokens) {
        if (nameLower === term) {
          score += 5;
        } else if (nameLower.includes(term)) {
          score += 2;
        }
      }

      return { id: result.id, score };
    });

    const filteredResults: typeof boostedResults = [];
    for (const result of boostedResults.sort((a, b) => b.score - a.score)) {
      if (result.score <= this.searchConfig.minScore) continue;

      const tool = this.findToolById(result.id);
      if (!tool) continue;

      const isAllowed = await this.isToolAllowed(tool, requestContext, 'search');
      if (isAllowed) {
        filteredResults.push(result);
        if (filteredResults.length >= this.searchConfig.topK) break;
      }
    }

    // Apply topK and format results.
    return filteredResults.slice(0, this.searchConfig.topK).map(r => {
      const description = this.toolDescriptions.get(r.id) || '';
      return {
        name: r.id,
        description: description.length > 150 ? description.slice(0, 147) + '...' : description,
        score: Math.round(r.score * 100) / 100,
      };
    });
  }

  async processInputStep(args: ProcessInputStepArgs) {
    const { tools, messageList } = args;
    for (const metaToolName of TOOL_SEARCH_META_TOOL_NAMES) {
      if (Object.prototype.hasOwnProperty.call(tools ?? {}, metaToolName)) {
        throw new Error(
          `ToolSearchProcessor cannot inject its "${metaToolName}" meta-tool because the input tool set already defines that name.`,
        );
      }
    }
    const storeContext = this.makeStoreContext(args);
    // Snapshot of names already loaded as of this step. Newly activated tools are
    // recorded via the store and become available on the model's next turn.
    const loadedToolNames = await this.store.getLoadedNames(storeContext);

    const autoLoad = this.searchConfig.autoLoad;

    // Add system instruction about the meta-tools
    messageList.addSystem(
      autoLoad
        ? 'To discover available tools, call search_tools with a keyword query. ' +
            'Matching tools are loaded automatically and become available on your next turn — ' +
            'there is no separate load step. After searching, use the tool directly.'
        : 'To discover available tools, call search_tools with a keyword query. ' +
            'To add one or more tools to the conversation, call load_tool with a toolName or toolNames array. ' +
            'Tools must be loaded before they can be used.',
    );

    // Create the search tool with BM25 ranking
    const searchTool = createTool({
      id: 'search_tools',
      description: autoLoad
        ? 'Search for available tools by keyword. ' +
          "Use this when you need a capability you don't currently have. " +
          'Returns a list of matching tools, which are loaded automatically and ' +
          'become available on your next turn — no separate load step is required.'
        : 'Search for available tools by keyword. ' +
          "Use this when you need a capability you don't currently have. " +
          'Returns a list of matching tools with their names and descriptions. ' +
          'After finding a useful tool, use load_tool to make it available.',
      inputSchema: z.object({
        query: z.string().describe('Search keywords (e.g., "weather", "github issue", "database query")'),
      }),
      outputSchema: z.object({
        results: z.array(
          z.object({
            name: z.string(),
            description: z.string(),
            score: z.number(),
          }),
        ),
        message: z.string(),
        activation: z
          .object({
            type: z.literal(TOOL_SEARCH_AUTO_LOAD_ACTIVATION_TYPE),
            version: z.literal(TOOL_SEARCH_AUTO_LOAD_ACTIVATION_VERSION),
            loaded: z.array(z.string()),
          })
          .optional(),
      }),
      execute: async ({ query }) => {
        // Use BM25 search for relevance-ranked results
        let results = await this.searchTools(query, args.requestContext);

        if (results.length === 0) {
          return {
            results: [],
            message: `No tools found matching "${query}". Try different keywords.`,
          };
        }

        if (autoLoad) {
          // Auto-load collapses discovery and loading into one operation, but it
          // must not collapse their policy checks. A tool may be discoverable
          // while loading it is forbidden for this request. Only expose and
          // persist matches that pass the same load-phase gate as `load_tool`.
          const loadAllowedResults: SearchResult[] = [];
          for (const result of results) {
            const tool = this.findToolById(result.name);
            if (!tool) continue;
            if (await this.isToolAllowed(tool, args.requestContext, 'load')) {
              loadAllowedResults.push(result);
            }
          }
          results = loadAllowedResults;

          if (results.length === 0) {
            return {
              results: [],
              message: `No tools available to load matching "${query}". Try different keywords.`,
            };
          }

          // Activate the matches immediately. They become usable on the next turn —
          // no explicit load_tool call needed. The store records the activation;
          // for the context store this result in the conversation messages is the durable record.
          const newlyLoaded: string[] = [];
          for (const result of results) {
            if (!loadedToolNames.has(result.name)) {
              newlyLoaded.push(result.name);
            }
          }
          await this.store.addLoaded(newlyLoaded, storeContext);
          for (const name of newlyLoaded) loadedToolNames.add(name);

          const activation: {
            type: typeof TOOL_SEARCH_AUTO_LOAD_ACTIVATION_TYPE;
            version: typeof TOOL_SEARCH_AUTO_LOAD_ACTIVATION_VERSION;
            loaded: string[];
          } = {
            type: TOOL_SEARCH_AUTO_LOAD_ACTIVATION_TYPE,
            version: TOOL_SEARCH_AUTO_LOAD_ACTIVATION_VERSION,
            // Mark every result, including a repeat match that was already
            // active. A cold replay can then recover from this result alone.
            loaded: results.map(result => result.name),
          };

          return {
            results,
            activation,
            message:
              `Found and loaded ${results.length} tool(s): ${results.map(r => r.name).join(', ')}. ` +
              `They are available on your next turn — call them directly.` +
              (newlyLoaded.length < results.length ? ' Some were already loaded.' : ''),
          };
        }

        return {
          results,
          message: `Found ${results.length} tool(s). Use load_tool with an exact toolName or a toolNames array to make them available.`,
        };
      },
    });

    // Create the load tool that uses thread-scoped state.
    // In auto-load mode this meta-tool is not exposed (search_tools activates matches itself).
    const loadTool = createTool({
      id: 'load_tool',
      description:
        'Load one or more tools into your context. ' +
        'Call this after finding tools with search_tools. ' +
        'Once loaded, tools will be available for use. ' +
        'Pass a single toolName or an array of toolNames to load multiple tools at once.',
      inputSchema: z.object({
        toolName: z.string().optional().describe('The exact name of a tool to load (from search results)'),
        toolNames: z
          .array(z.string())
          .optional()
          .describe('Array of exact tool names to load in one call (from search results)'),
      }),
      outputSchema: z.object({
        success: z.boolean(),
        message: z.string(),
        loadedCount: z.number().optional(),
        toolName: z.string().optional(),
        loaded: z.array(z.string()).optional(),
        notFound: z.array(z.string()).optional(),
        alreadyLoaded: z.array(z.string()).optional(),
      }),
      execute: async ({ toolName, toolNames }) => {
        // Determine which tools to load
        let toLoad: string[];
        const toolNamesProvided = toolNames !== undefined;
        if (toolNamesProvided && toolNames!.length === 0 && !toolName) {
          return {
            success: false,
            message: 'toolNames array must not be empty.',
          };
        }
        if (toolNamesProvided && toolNames!.length > 0) {
          // Merge toolName into toolNames if both provided, then dedupe
          const base: string[] = [...toolNames!];
          if (toolName) base.push(toolName);
          toLoad = Array.from(new Set(base));
        } else if (toolName) {
          toLoad = [toolName];
        } else {
          return {
            success: false,
            message: 'You must provide either toolName (string) or toolNames (array) to load.',
          };
        }

        const notFound: string[] = [];
        const alreadyLoaded: string[] = [];
        const loaded: string[] = [];

        for (const name of toLoad) {
          // Check if tool exists
          const matchingTool = this.findToolForDynamicName(name);

          if (!matchingTool) {
            notFound.push(name);
            continue;
          }

          const isAllowed = await this.isToolAllowed(matchingTool, args.requestContext, 'load');
          if (!isAllowed) {
            notFound.push(name);
            continue;
          }

          // Check if already loaded (snapshot of prior steps, plus this call).
          if (loadedToolNames.has(name) || loaded.includes(name)) {
            alreadyLoaded.push(name);
            continue;
          }

          loaded.push(name);
        }

        // Record newly loaded tools in the store. For the context store the
        // canonical assistant result is the durable record; request state only
        // bridges execution to later steps in this run.
        await this.store.addLoaded(loaded, storeContext);
        for (const name of loaded) loadedToolNames.add(name);

        // Build response based on how many tools were requested
        // Only use single-tool backward-compatible shape when using the legacy toolName param
        if (toLoad.length === 1 && !toolNamesProvided) {
          // Single-tool response (backward compatible shape)
          if (notFound.length > 0) {
            const name = toLoad[0]!;
            const suggestions = await this.getSuggestedToolNames(name, args.requestContext);
            let message = `Tool "${name}" not found.`;
            if (suggestions.length > 0) {
              message += ` Did you mean: ${suggestions.slice(0, 3).join(', ')}?`;
            } else {
              message += ' Use search_tools to find available tools.';
            }
            return { success: false, message, toolName: name };
          }
          if (alreadyLoaded.length > 0) {
            return {
              success: true,
              message: `Tool "${alreadyLoaded[0]}" is already loaded and available.`,
              toolName: alreadyLoaded[0],
              loaded: [alreadyLoaded[0]!],
            };
          }
          return {
            success: true,
            message: `Tool "${loaded[0]}" loaded successfully. It will be available on your next turn.`,
            toolName: loaded[0],
            loaded: [loaded[0]!],
          };
        }

        // Multi-tool response
        const parts: string[] = [];
        if (loaded.length > 0) parts.push(`Loaded: ${loaded.join(', ')} — available on your next turn`);
        if (alreadyLoaded.length > 0) parts.push(`Already loaded: ${alreadyLoaded.join(', ')}`);
        if (notFound.length > 0) parts.push(`Not found: ${notFound.join(', ')}`);

        return {
          success: notFound.length === 0,
          message: parts.join(' | '),
          loadedCount: loaded.length,
          loaded: loaded.length > 0 ? loaded : undefined,
          notFound: notFound.length > 0 ? notFound : undefined,
          alreadyLoaded: alreadyLoaded.length > 0 ? alreadyLoaded : undefined,
        };
      },
    });

    // Get loaded tools as of this step's snapshot.
    const loadedTools = await this.getLoadedTools(loadedToolNames, args.requestContext);
    for (const loadedToolName of Object.keys(loadedTools)) {
      if (Object.prototype.hasOwnProperty.call(tools ?? {}, loadedToolName)) {
        throw new Error(
          `ToolSearchProcessor loaded tool "${loadedToolName}" conflicts with an always-available input tool.`,
        );
      }
    }

    // Return merged tools, ordered to keep the cacheable prefix stable:
    // meta-tool(s) first (always present, fixed position), then existing tools,
    // then loaded tools appended last. Appending newly activated tools rather than
    // interleaving them preserves the tool-definition prefix so prompt caching is
    // not invalidated when a tool is loaded mid-conversation.
    return {
      tools: {
        search_tools: searchTool,
        // load_tool is omitted in auto-load mode — search_tools activates matches directly.
        ...(autoLoad ? {} : { load_tool: loadTool }),
        ...(tools ?? {}),
        ...loadedTools,
      },
    };
  }
}
