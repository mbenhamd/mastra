/**
 * SkillSearchProcessor - On-demand skill discovery for agents with many skills.
 *
 * Instead of injecting all skill metadata upfront (like SkillsProcessor),
 * this processor provides search_skills and load_skill meta-tools so skills
 * are discovered on demand. Loaded state can be derived from conversation
 * results with context storage, while in-memory storage remains the default.
 *
 * Mirrors the ToolSearchProcessor pattern but for skills.
 *
 * @example
 * ```typescript
 * const skillSearch = new SkillSearchProcessor({
 *   workspace,
 *   search: { topK: 5, minScore: 0 },
 *   storage: 'context',
 * });
 *
 * const agent = new Agent({
 *   workspace,
 *   inputProcessors: [skillSearch],
 * });
 * ```
 */
import { createHash } from 'node:crypto';

import { z } from 'zod/v4';
import type { MastraDBMessage } from '../../agent/message-list';
import { MASTRA_THREAD_ID_KEY } from '../../request-context';
import { createTool } from '../../tools';
import type { Skill, WorkspaceSkills } from '../../workspace/skills';
import type { Workspace } from '../../workspace/workspace';
import type { ProcessInputStepArgs, Processor } from '../index';

const SEARCH_SKILLS_TOOL_NAME = 'search_skills';
const LOAD_SKILL_TOOL_NAME = 'load_skill';
const SKILL_SEARCH_AUTO_LOAD_ACTIVATION_TYPE = 'skill-search-auto-load' as const;
const SKILL_SEARCH_AUTO_LOAD_ACTIVATION_VERSION = 1 as const;
const CURRENT_RUN_LOADED_SKILLS_KEY = 'skillSearchLoadedSkills';
const CURRENT_RUN_CONTEXT_INITIALIZED_KEY = 'skillSearchContextInitialized';
const SHA256_DIGEST_PATTERN = /^sha256:[a-f0-9]{64}$/;

interface LoadedSkillReference {
  skillName: string;
  contentDigest: string;
}

/**
 * Thread state used by the in-memory backend.
 */
interface ThreadState {
  skills: Map<string, LoadedSkillReference>;
  lastAccessed: number;
}

function getSkillContentDigest(skill: Skill): string {
  const digest = createHash('sha256')
    .update(skill.name)
    .update('\0')
    .update(skill.path)
    .update('\0')
    .update(skill.description)
    .update('\0')
    .update(skill.instructions)
    .digest('hex');

  return `sha256:${digest}`;
}

function readLoadedSkillReference(result: unknown): LoadedSkillReference | undefined {
  if (!result || typeof result !== 'object') return undefined;

  const candidate = result as { success?: unknown; skillName?: unknown; contentDigest?: unknown };
  if (
    candidate.success !== true ||
    typeof candidate.skillName !== 'string' ||
    candidate.skillName.length === 0 ||
    typeof candidate.contentDigest !== 'string' ||
    !SHA256_DIGEST_PATTERN.test(candidate.contentDigest)
  ) {
    return undefined;
  }

  return { skillName: candidate.skillName, contentDigest: candidate.contentDigest };
}

/**
 * Read the explicit activation receipt emitted by `search_skills` in auto-load
 * mode. The references must exactly match the canonical search result names;
 * an ordinary discovery-only result therefore cannot activate instructions.
 */
function readAutoLoadedSkillReferences(result: unknown): LoadedSkillReference[] {
  if (!result || typeof result !== 'object') return [];

  const candidate = result as { results?: unknown; activation?: unknown };
  if (!Array.isArray(candidate.results) || !candidate.activation || typeof candidate.activation !== 'object') {
    return [];
  }

  const resultNames: string[] = [];
  for (const entry of candidate.results) {
    const searchResult = entry as { name?: unknown; description?: unknown; score?: unknown };
    if (
      typeof searchResult.name !== 'string' ||
      searchResult.name.length === 0 ||
      typeof searchResult.description !== 'string' ||
      typeof searchResult.score !== 'number' ||
      !Number.isFinite(searchResult.score)
    ) {
      return [];
    }
    resultNames.push(searchResult.name);
  }

  const activation = candidate.activation as { type?: unknown; version?: unknown; loaded?: unknown };
  if (
    activation.type !== SKILL_SEARCH_AUTO_LOAD_ACTIVATION_TYPE ||
    activation.version !== SKILL_SEARCH_AUTO_LOAD_ACTIVATION_VERSION ||
    !Array.isArray(activation.loaded) ||
    activation.loaded.length !== resultNames.length
  ) {
    return [];
  }

  const references: LoadedSkillReference[] = [];
  for (let index = 0; index < activation.loaded.length; index += 1) {
    const entry = activation.loaded[index] as { skillName?: unknown; contentDigest?: unknown };
    if (
      typeof entry.skillName !== 'string' ||
      entry.skillName !== resultNames[index] ||
      typeof entry.contentDigest !== 'string' ||
      !SHA256_DIGEST_PATTERN.test(entry.contentDigest)
    ) {
      return [];
    }
    references.push({ skillName: entry.skillName, contentDigest: entry.contentDigest });
  }

  return references;
}

/**
 * Derive loaded skills from completed `load_skill` results and marked auto-load
 * `search_skills` results still present in the conversation. The latest result
 * for a skill wins. Removing a result during edit/regenerate therefore removes
 * that activation without process-local cleanup.
 */
function deriveLoadedSkillsFromMessages(messages: MastraDBMessage[] | undefined): Map<string, LoadedSkillReference> {
  const loaded = new Map<string, LoadedSkillReference>();
  if (!Array.isArray(messages)) return loaded;

  for (const message of messages) {
    // Canonical Mastra tool invocations are stored on assistant messages. Do not
    // let user-authored tool-shaped parts activate skill instructions.
    if (message.role !== 'assistant') continue;

    for (const part of message.content?.parts ?? []) {
      if (part.type !== 'tool-invocation') continue;
      const invocation = part.toolInvocation;
      if (invocation?.state !== 'result' || invocation.result === undefined) {
        continue;
      }

      if (invocation.toolName === LOAD_SKILL_TOOL_NAME) {
        const reference = readLoadedSkillReference(invocation.result);
        if (reference) loaded.set(reference.skillName, reference);
      } else if (invocation.toolName === SEARCH_SKILLS_TOOL_NAME) {
        for (const reference of readAutoLoadedSkillReferences(invocation.result)) {
          loaded.set(reference.skillName, reference);
        }
      }
    }
  }

  return loaded;
}

function getCurrentRunLoadedSkills(state: Record<string, unknown>): Record<string, string> {
  const value = state[CURRENT_RUN_LOADED_SKILLS_KEY];
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};

  const loaded: Record<string, string> = {};
  for (const [skillName, contentDigest] of Object.entries(value)) {
    if (skillName.length > 0 && typeof contentDigest === 'string' && SHA256_DIGEST_PATTERN.test(contentDigest)) {
      loaded[skillName] = contentDigest;
    }
  }
  return loaded;
}

/**
 * Configuration options for SkillSearchProcessor
 */
export interface SkillSearchProcessorOptions {
  /**
   * Workspace instance containing skills.
   * Skills are accessed via workspace.skills.
   */
  workspace: Workspace;

  /**
   * Configuration for the search behavior
   */
  search?: {
    /**
     * Maximum number of skills to return in search results
     * @default 5
     */
    topK?: number;

    /**
     * Minimum relevance score for including a skill in search results
     * @default 0
     */
    minScore?: number;

    /**
     * When true, skills returned by `search_skills` are activated immediately
     * and `load_skill` is not exposed. Their instructions become available on
     * the model's next turn.
     *
     * This removes the separate load turn while keeping discovery model-driven.
     * Because every match is activated, keep `topK` conservative in this mode.
     * @default false
     */
    autoLoad?: boolean;
  };

  /**
   * Where loaded-skill state lives.
   *
   * - `'context'`: derives loaded skills from completed `load_skill` results or
   *   explicitly marked auto-load `search_skills` results in the conversation.
   *   This survives process restart and naturally unloads a skill when
   *   edit/regenerate removes its activation result.
   * - `'in-memory'` (default): backed by a thread-scoped map. State is
   *   lost on restart and anonymous requests share a `'default'` entry.
   *
   * Context-backed results include a content digest. If the current skill no
   * longer matches that digest, its instructions are not injected until the
   * model loads the skill again.
   *
   * @default 'in-memory'
   */
  storage?: 'context' | 'in-memory';

  /**
   * Time-to-live for in-memory thread state in milliseconds.
   * After this duration of inactivity, thread state will be eligible for cleanup.
   * Set to 0 to disable TTL cleanup.
   * Ignored when `storage` is `'context'`.
   * @default 3600000 (1 hour)
   */
  ttl?: number;
}

/**
 * Processor that enables on-demand skill discovery and loading.
 *
 * Instead of injecting all skill metadata upfront, this processor:
 * 1. Gives the agent two meta-tools: search_skills and load_skill
 * 2. Agent searches for relevant skills using keywords
 * 3. Agent loads specific skills into the conversation on demand
 * 4. Loaded skill instructions appear as system messages
 *
 * This pattern reduces context usage when workspaces have many skills.
 */
export class SkillSearchProcessor implements Processor<'skill-search'> {
  readonly id = 'skill-search';
  readonly name = 'Skill Search Processor';
  readonly description = 'Enables on-demand skill discovery and loading via search';
  readonly providesSkillDiscovery: Processor['providesSkillDiscovery'] = 'on-demand';

  private readonly workspace: Workspace;
  private readonly searchConfig: { topK: number; minScore: number; autoLoad: boolean };
  private readonly storage: 'context' | 'in-memory';
  private readonly ttl: number;
  private cleanupIntervalId?: ReturnType<typeof setInterval>;

  /**
   * State for `storage: 'in-memory'`. Context mode has no
   * processor-global loaded-skill state; it uses messages plus request-local
   * processor state, so a discarded request cannot contaminate a replacement.
   */
  private threadLoadedSkills = new Map<string, ThreadState>();

  constructor(options: SkillSearchProcessorOptions) {
    this.workspace = options.workspace;
    this.searchConfig = {
      topK: options.search?.topK ?? 5,
      minScore: options.search?.minScore ?? 0,
      autoLoad: options.search?.autoLoad ?? false,
    };
    this.storage = options.storage ?? 'in-memory';
    this.ttl = options.ttl ?? 3600000; // Default: 1 hour

    if (this.storage === 'in-memory' && this.ttl > 0) {
      this.scheduleCleanup();
    }
  }

  /**
   * Dispose of this processor, clearing the cleanup interval and all thread state.
   * Call this when the processor is no longer needed to prevent timer leaks.
   */
  public dispose(): void {
    if (this.cleanupIntervalId) {
      clearInterval(this.cleanupIntervalId);
      this.cleanupIntervalId = undefined;
    }
    this.clearAllState();
  }

  /**
   * Get the workspace skills interface
   */
  private get skills(): WorkspaceSkills | undefined {
    return this.workspace.skills;
  }

  private getThreadId(args: ProcessInputStepArgs): string | undefined {
    return (args.requestContext?.get(MASTRA_THREAD_ID_KEY) as string | undefined) || undefined;
  }

  /**
   * Get or create in-memory state for the given thread.
   * Updates the lastAccessed timestamp for TTL management.
   */
  private getThreadState(threadId: string): ThreadState {
    if (!this.threadLoadedSkills.has(threadId)) {
      this.threadLoadedSkills.set(threadId, {
        skills: new Map(),
        lastAccessed: Date.now(),
      });
    }
    const state = this.threadLoadedSkills.get(threadId)!;
    state.lastAccessed = Date.now();
    return state;
  }

  private getLoadedSkillReferences(args: ProcessInputStepArgs): Map<string, LoadedSkillReference> {
    if (this.storage === 'in-memory') {
      const threadId = this.getThreadId(args) ?? 'default';
      return new Map(this.getThreadState(threadId).skills);
    }

    const currentRun = getCurrentRunLoadedSkills(args.state);
    if (args.state[CURRENT_RUN_CONTEXT_INITIALIZED_KEY] !== true) {
      for (const reference of deriveLoadedSkillsFromMessages(args.messages).values()) {
        currentRun[reference.skillName] = reference.contentDigest;
      }
      args.state[CURRENT_RUN_LOADED_SKILLS_KEY] = currentRun;
      args.state[CURRENT_RUN_CONTEXT_INITIALIZED_KEY] = true;
    }

    // `state` is scoped to one ProcessorRunner request. It snapshots durable
    // message evidence once, then bridges load execution to later steps without
    // rescanning the full history. It disappears on restart, edit, or regenerate.
    const loaded = new Map<string, LoadedSkillReference>();
    for (const [skillName, contentDigest] of Object.entries(currentRun)) {
      loaded.set(skillName, { skillName, contentDigest });
    }
    return loaded;
  }

  private recordLoadedSkill(reference: LoadedSkillReference, args: ProcessInputStepArgs): void {
    if (this.storage === 'in-memory') {
      const threadId = this.getThreadId(args) ?? 'default';
      this.getThreadState(threadId).skills.set(reference.skillName, reference);
      return;
    }

    const currentRun = getCurrentRunLoadedSkills(args.state);
    currentRun[reference.skillName] = reference.contentDigest;
    args.state[CURRENT_RUN_LOADED_SKILLS_KEY] = currentRun;
  }

  /**
   * Clear in-memory state for a specific thread.
   *
   * Context-backed state is owned by conversation messages rather than this
   * processor. Remove the corresponding `load_skill` result to unload it; any
   * request-local bridge state is released with that request.
   */
  public clearState(threadId: string = 'default'): void {
    this.threadLoadedSkills.delete(threadId);
  }

  /**
   * Clear all processor-owned in-memory state.
   */
  public clearAllState(): void {
    this.threadLoadedSkills.clear();
  }

  /**
   * Clean up stale thread state based on TTL.
   * @returns Number of threads cleaned up
   */
  private cleanupStaleState(): number {
    if (this.ttl <= 0) return 0;

    const now = Date.now();
    let cleanedCount = 0;

    for (const [threadId, state] of this.threadLoadedSkills.entries()) {
      if (now - state.lastAccessed > this.ttl) {
        this.threadLoadedSkills.delete(threadId);
        cleanedCount++;
      }
    }

    return cleanedCount;
  }

  /**
   * Schedule periodic cleanup of stale thread state.
   */
  private scheduleCleanup(): void {
    const cleanupInterval = Math.max(this.ttl / 2, 60000); // Minimum 1 minute
    this.cleanupIntervalId = setInterval(() => {
      this.cleanupStaleState();
    }, cleanupInterval);

    if (this.cleanupIntervalId.unref) {
      this.cleanupIntervalId.unref();
    }
  }

  /**
   * Get statistics about current thread state.
   */
  public getStateStats(): { threadCount: number; oldestAccessTime: number | null } {
    if (this.threadLoadedSkills.size === 0) {
      return { threadCount: 0, oldestAccessTime: null };
    }

    let oldest = Date.now();
    for (const state of this.threadLoadedSkills.values()) {
      if (state.lastAccessed < oldest) {
        oldest = state.lastAccessed;
      }
    }

    return {
      threadCount: this.threadLoadedSkills.size,
      oldestAccessTime: oldest,
    };
  }

  /**
   * Manually trigger cleanup of stale state.
   * @returns Number of threads cleaned up
   */
  public cleanupNow(): number {
    return this.cleanupStaleState();
  }

  async processInputStep(args: ProcessInputStepArgs) {
    const { tools, messageList } = args;
    const skills = this.skills;

    if (!skills) {
      return { tools };
    }

    // Refresh skills on first step only
    if (args.stepNumber === 0) {
      await skills.maybeRefresh({ requestContext: args.requestContext });
    }

    const loadedSkillReferences = this.getLoadedSkillReferences(args);
    const activeLoadedSkills = new Map<string, { skill: Skill; contentDigest: string }>();
    const staleSkillNames: string[] = [];

    for (const reference of loadedSkillReferences.values()) {
      let skill: Skill | null = null;
      try {
        skill = await skills.get(reference.skillName);
      } catch {
        // A restored activation must fail closed when its current source cannot
        // be verified. The model receives a reload hint below.
      }

      if (skill && getSkillContentDigest(skill) === reference.contentDigest) {
        activeLoadedSkills.set(reference.skillName, { skill, contentDigest: reference.contentDigest });
      } else {
        staleSkillNames.push(reference.skillName);
      }
    }

    const autoLoad = this.searchConfig.autoLoad;

    // Add system instruction about the meta-tools
    messageList.addSystem(
      autoLoad
        ? 'To discover available skills, call search_skills with a keyword query. ' +
            'Matching skills are loaded automatically and their instructions become available on your next turn. ' +
            'There is no separate load step; after searching, follow the loaded instructions.'
        : 'To discover available skills, call search_skills with a keyword query. ' +
            "To load a skill's instructions, call load_skill with the skill name. " +
            'Loaded skills provide context and instructions for the conversation.',
    );

    if (staleSkillNames.length > 0) {
      messageList.addSystem(
        `Previously loaded skills changed or became unavailable and are no longer active: ${staleSkillNames.join(
          ', ',
        )}. Call ${autoLoad ? 'search_skills' : 'load_skill'} again before relying on them.`,
      );
    }

    // Create the search_skills meta-tool
    const searchSkillTool = createTool({
      id: 'search_skills',
      description: autoLoad
        ? 'Search for available skills by keyword. ' +
          'Matching skills are loaded automatically and their instructions become available on your next turn. ' +
          'No separate load step is required.'
        : 'Search for available skills by keyword. ' +
          'Returns a list of matching skills with their names and descriptions. ' +
          'After finding a useful skill, use load_skill to load its instructions.',
      inputSchema: z.object({
        query: z
          .string()
          .trim()
          .min(1, 'Query is required')
          .describe('Search keywords (e.g., "api design", "testing", "deployment")'),
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
            type: z.literal(SKILL_SEARCH_AUTO_LOAD_ACTIVATION_TYPE),
            version: z.literal(SKILL_SEARCH_AUTO_LOAD_ACTIVATION_VERSION),
            loaded: z.array(
              z.object({
                skillName: z.string(),
                contentDigest: z.string(),
              }),
            ),
          })
          .optional(),
      }),
      execute: async ({ query }) => {
        const searchResults = await skills.search(query, {
          topK: this.searchConfig.topK,
          minScore: this.searchConfig.minScore,
        });

        if (searchResults.length === 0) {
          return {
            results: [],
            message: `No skills found matching "${query}". Try different keywords.`,
          };
        }

        // Deduplicate by skillName (search may return multiple matches per skill)
        const seen = new Set<string>();
        const uniqueResults = searchResults.filter(r => {
          if (seen.has(r.skillName)) return false;
          seen.add(r.skillName);
          return true;
        });

        // Get metadata for descriptions
        const skillList = await skills.list();
        const metaMap = new Map(skillList.map(s => [s.name, s]));

        let results = uniqueResults.map(r => {
          const meta = metaMap.get(r.skillName);
          const description = meta?.description ?? '';
          return {
            name: r.skillName,
            description: description.length > 150 ? description.slice(0, 147) + '...' : description,
            score: Math.round(r.score * 100) / 100,
          };
        });

        if (autoLoad) {
          const loaded: LoadedSkillReference[] = [];
          const loadableResults: typeof results = [];
          let alreadyLoadedCount = 0;

          for (const result of results) {
            let skill: Skill | null = null;
            try {
              skill = await skills.get(result.name);
            } catch {
              // A search hit that cannot be read must not produce a durable
              // activation receipt or claim that instructions were loaded.
            }
            if (!skill) continue;

            const reference = {
              skillName: result.name,
              contentDigest: getSkillContentDigest(skill),
            };
            if (activeLoadedSkills.get(result.name)?.contentDigest === reference.contentDigest) {
              alreadyLoadedCount += 1;
            } else {
              this.recordLoadedSkill(reference, args);
              activeLoadedSkills.set(result.name, { skill, contentDigest: reference.contentDigest });
            }
            loadableResults.push(result);
            loaded.push(reference);
          }
          results = loadableResults;

          if (results.length === 0) {
            return {
              results: [],
              message: `No skills available to load matching "${query}". Try different keywords.`,
            };
          }

          return {
            results,
            activation: {
              type: SKILL_SEARCH_AUTO_LOAD_ACTIVATION_TYPE,
              version: SKILL_SEARCH_AUTO_LOAD_ACTIVATION_VERSION,
              loaded,
            },
            message:
              `Found and loaded ${results.length} skill(s): ${results.map(result => result.name).join(', ')}. ` +
              'Their instructions are available on your next turn.' +
              (alreadyLoadedCount > 0 ? ' Some were already loaded.' : ''),
          };
        }

        return {
          results,
          message: `Found ${results.length} skill(s). Use load_skill with the exact skill name to load its instructions.`,
        };
      },
    });

    // Create the load_skill meta-tool
    const loadSkillTool = createTool({
      id: 'load_skill',
      description:
        "Load a skill's full instructions into the conversation. " +
        'Call this after finding a skill with search_skills. ' +
        "The skill's instructions will be available as context.",
      inputSchema: z.object({
        skillName: z.string().describe('The exact name of the skill to load (from search results)'),
      }),
      outputSchema: z.object({
        success: z.boolean(),
        message: z.string(),
        skillName: z.string().optional(),
        contentDigest: z.string().optional(),
      }),
      execute: async ({ skillName }) => {
        const skill = await skills.get(skillName);
        if (!skill) {
          // Suggest similar names
          const allSkills = await skills.list();
          const suggestions = allSkills
            .filter(
              s =>
                s.name.toLowerCase().includes(skillName.toLowerCase()) ||
                skillName.toLowerCase().includes(s.name.toLowerCase()),
            )
            .slice(0, 3);

          let message = `Skill "${skillName}" not found.`;
          if (suggestions.length > 0) {
            message += ` Did you mean: ${suggestions.map(s => s.name).join(', ')}?`;
          } else {
            message += ' Use search_skills to find available skills.';
          }

          return { success: false, message };
        }

        const contentDigest = getSkillContentDigest(skill);
        const active = activeLoadedSkills.get(skillName);
        if (active?.contentDigest === contentDigest) {
          return {
            success: true,
            message: `Skill "${skillName}" is already loaded.`,
            skillName,
            contentDigest,
          };
        }

        const reference = { skillName, contentDigest };
        this.recordLoadedSkill(reference, args);
        activeLoadedSkills.set(skillName, { skill, contentDigest });

        return {
          success: true,
          message: `Skill "${skillName}" loaded. Its instructions are now available as context.`,
          skillName,
          contentDigest,
        };
      },
    });

    // Build system messages for loaded skills
    for (const [skillName, { skill }] of activeLoadedSkills) {
      messageList.addSystem(`[Skill: ${skillName}]\n\n${skill.instructions}`);
    }

    const metaTools = autoLoad
      ? { search_skills: searchSkillTool }
      : { search_skills: searchSkillTool, load_skill: loadSkillTool };
    if (tools) {
      for (const key of Object.keys(tools)) {
        if (key in metaTools) {
          console.warn(`[SkillSearchProcessor] User tool "${key}" conflicts with meta-tool and will be shadowed.`);
        }
      }
    }

    return {
      tools: {
        ...(tools ?? {}),
        ...metaTools,
      },
    };
  }
}
