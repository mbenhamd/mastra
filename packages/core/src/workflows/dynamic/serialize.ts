/**
 * Live → Storable half of the workflow round-trip: walk a live `stepFlow`
 * (runtime references, closures) and emit the JSON-safe storable form
 * (ids + serialized mapping configs, no closures).
 *
 * The static subset that round-trips:
 *  - agent / tool by id
 *  - mapping with `value`, `step`, `initData`, `requestContextPath`, `template`,
 *    `state` sources (no `fn` source — closures don't round-trip)
 *  - sleep / sleepUntil with literal duration/date
 *  - parallel (inner entries must themselves be static)
 *  - foreach with literal concurrency
 *  - conditional / loop with declarative predicates (closure predicates throw)
 *  - generic `.then(step)` falls back to a minimal step descriptor — usable
 *    only when the step's id resolves on the live Mastra at load time
 *
 * Anything outside the subset throws at `toStorableGraph` time: silent loss
 * would ship broken workflows unnoticed.
 */
import { standardSchemaToJSONSchema, toStandardSchema } from '../../schema';
import { SERIALIZED_AGENT_PASSTHROUGH_OPTION_KEYS } from '../types';
import type {
  SerializedSingleStepEntry,
  SerializedStepFlowEntry,
  SerializedStepOptions,
  SingleStepEntry,
  StepFlowEntry,
} from '../types';
import { getSingleStepEntryId } from '../utils';
import { getAdmittedJsonSchema } from './admitted-schema-source';

/**
 * Walk a live `stepFlow` and emit a JSON-safe `SerializedStepFlowEntry[]` with
 * full (un-truncated) mapping configs and all step/agent/tool references stored
 * as ids. Throws on entries that can't round-trip (closures, closure predicates).
 */
export function toStorableGraph(stepFlow: StepFlowEntry[]): SerializedStepFlowEntry[] {
  return stepFlow.map(entry => serializeEntry(entry));
}

function serializeEntry(entry: StepFlowEntry): SerializedStepFlowEntry {
  switch (entry.type) {
    case 'step':
    case 'agent':
    case 'tool':
    case 'mapping':
      return serializeSingleEntry(entry);
    case 'sleep':
      if (typeof entry.duration !== 'number') {
        throw new Error(`Sleep step "${entry.id}" cannot be stored: dynamic duration (function) is not supported.`);
      }
      return { type: 'sleep', id: entry.id, duration: entry.duration };
    case 'sleepUntil':
      if (!(entry.date instanceof Date)) {
        throw new Error(`SleepUntil step "${entry.id}" cannot be stored: dynamic date (function) is not supported.`);
      }
      return { type: 'sleepUntil', id: entry.id, date: entry.date };
    case 'parallel':
      return { type: 'parallel', steps: entry.steps.map(s => serializeSingleEntry(s)) };
    case 'foreach':
      if (entry.step.type === 'mapping') {
        throw new Error(
          `Foreach step cannot iterate a mapping: mappings project data, they don't execute per item. Use an agent, tool, or plain step as the foreach body.`,
        );
      }
      if (typeof entry.opts.concurrency === 'function') {
        // The stored `{ fn }` shape is never rehydrated back into a resolver —
        // rehydration would silently run the foreach with `concurrency: 1`.
        throw new Error(
          `Foreach step "${getSingleStepEntryId(entry.step)}" cannot be stored: dynamic concurrency (function resolver) is not supported. Use a literal number.`,
        );
      }
      return {
        type: 'foreach',
        step: serializeSingleEntry(entry.step),
        opts: { concurrency: entry.opts.concurrency },
      };
    case 'conditional': {
      const predicates = entry.predicates;
      if (!predicates || predicates.some(p => !p || typeof p !== 'object')) {
        throw new Error(
          `Conditional (branch) step cannot be stored: closure predicates do not round-trip. Use the declarative form ({ predicate: {...} }) for each branch.`,
        );
      }
      return {
        type: 'conditional',
        steps: entry.steps.map(s => serializeSingleEntry(s)),
        serializedConditions: entry.serializedConditions,
        predicates,
      };
    }
    case 'loop': {
      const predicate = entry.predicate;
      if (!predicate || typeof predicate !== 'object') {
        throw new Error(
          `Loop step "${getSingleStepEntryId(entry.step)}" cannot be stored: closure predicates do not round-trip. Use the declarative form ({ predicate: {...} }).`,
        );
      }
      return {
        type: 'loop',
        step: serializeSingleEntry(entry.step),
        serializedCondition: entry.serializedCondition,
        loopType: entry.loopType,
        predicate,
      };
    }
    default: {
      const _exhaustive: never = entry;
      throw new Error(`Unknown step entry type: ${JSON.stringify(_exhaustive)}`);
    }
  }
}

function serializeSingleEntry(entry: SingleStepEntry): SerializedSingleStepEntry {
  if (entry.type === 'agent') {
    const options = pickSerializableStepOptions(entry.options, entry.id, 'agent');
    const outputSchema = extractStructuredOutputJsonSchema(entry.options, entry.id);
    return {
      type: 'agent',
      id: entry.id,
      agentId: entry.agentId,
      description: entry.agent?.description,
      ...(outputSchema ? { outputSchema } : {}),
      ...(options ? { options } : {}),
    };
  }
  if (entry.type === 'tool') {
    const options = pickSerializableStepOptions(entry.options, entry.id, 'tool');
    return {
      type: 'tool',
      id: entry.id,
      toolId: entry.toolId,
      description: entry.tool?.description,
      ...(options ? { options } : {}),
    };
  }
  if (entry.type === 'mapping') {
    if (typeof entry.mapConfig === 'function') {
      throw new Error(
        `Mapping step "${entry.id}" cannot be stored: the function form does not round-trip. Use the declarative form (template / step / initData / value).`,
      );
    }
    const serialized: Record<string, any> = {};
    for (const [key, mapping] of Object.entries(entry.mapConfig as Record<string, any>)) {
      const m: any = mapping;
      if (m.fn !== undefined) {
        throw new Error(`Mapping step "${entry.id}" key "${key}" cannot be stored: source is a function.`);
      }
      if (m.value !== undefined) {
        serialized[key] = { value: m.value };
      } else if (m.requestContextPath) {
        serialized[key] = { requestContextPath: m.requestContextPath };
      } else if (typeof m.template === 'string') {
        serialized[key] = { template: m.template };
      } else if (m.initData) {
        // Rehydrated canonical mappings use the boolean marker while fluent
        // mappings carry a Workflow instance. Preserve both forms: omitting
        // `initData` here would make the next round trip invalid.
        serialized[key] = { initData: m.initData?.id ?? true, path: m.path };
      } else if (m.step) {
        serialized[key] = {
          step: Array.isArray(m.step) ? m.step.map((s: any) => s?.id) : m.step?.id,
          path: m.path,
        };
      } else {
        serialized[key] = m;
      }
    }
    return { type: 'mapping', id: entry.id, mapConfig: JSON.stringify(serialized) };
  }
  // A nested Workflow reached the generic `.then(step)` fallback (its
  // component discriminator is 'WORKFLOW'). Emit a declarative `workflow`
  // entry so the rehydrator can rebuild it by id. Inline the nested graph
  // when present so Studio/API consumers can expand it (same role the old
  // `type:'step' + component:'WORKFLOW'` shape played).
  if ((entry.step as any)?.component === 'WORKFLOW') {
    // Prefer the public getter (serializedStepGraph); fall back to the
    // protected/legacy serializedStepFlow field.
    const nestedFlow =
      ((entry.step as any).serializedStepGraph as SerializedStepFlowEntry[] | undefined) ??
      ((entry.step as any).serializedStepFlow as SerializedStepFlowEntry[] | undefined);
    return {
      type: 'workflow',
      id: (entry.step as any).id,
      workflowId: (entry.step as any).id,
      ...((entry.step as any).description ? { description: (entry.step as any).description } : {}),
      ...(nestedFlow ? { serializedStepFlow: nestedFlow } : {}),
    };
  }
  // generic `.then(step)` — descriptor only; rehydration looks the step up
  // by id on the live Mastra instance.
  return { type: 'step', step: stepDescriptor(entry.step) };
}

function stepDescriptor(step: any) {
  return {
    id: step.id,
    description: step.description,
    metadata: step.metadata,
    component: step.component,
    canSuspend: Boolean(step.suspendSchema || step.resumeSchema),
  };
}

/**
 * Options that can never be stored, keyed to a targeted hint. Everything here
 * is either closure-carrying by nature, a live runtime object, per-run
 * identity, or a per-call trust signal — none of which round-trip through
 * JSON, so a rehydrated step would silently run without them.
 */
const NON_STORABLE_STEP_OPTION_HINTS: Record<string, string> = {
  stopWhen: 'stop conditions are functions',
  inputProcessors: 'live processor instances do not round-trip',
  outputProcessors: 'live processor instances do not round-trip',
  errorProcessors: 'live processor instances do not round-trip',
  toolsets: 'live tool instances do not round-trip',
  clientTools: 'live tool instances do not round-trip',
  hooks: 'tool hooks are callback closures',
  prepareStep: 'callback closures do not round-trip',
  onIterationComplete: 'callback closures do not round-trip',
  delegation: 'delegation hooks are callback closures',
  isTaskComplete: 'scorer instances do not round-trip',
  experimentalTransform: 'stream transform factories do not round-trip',
  transform: 'tool-payload transform policies carry functions',
  memory: 'memory binds per-run identity and may carry live configuration',
  memoryOptions: 'memory binds per-run identity and may carry live configuration',
  runId: 'run ids are per-run identity',
  actor: 'actor is a per-call trust signal and must never be persisted',
  tracingOptions: 'tracing options are per-invocation observability state',
  untilIdle: 'background-continuation lifecycle is a live-invocation concern',
};

/**
 * Fail-closed structural check for a storable option value: only JSON-safe
 * plain data (finite numbers, strings, booleans, null, plain objects/arrays)
 * survives a storage round-trip byte-for-byte. Anything else — functions,
 * class instances, symbols, bigints, non-finite numbers, `undefined` array
 * holes — would be silently mangled or dropped by `JSON.stringify`.
 */
function assertStorableOptionValue(value: unknown, entryId: string, kind: 'agent' | 'tool', path: string): void {
  const fail = (reason: string): never => {
    throw new Error(
      `${kind === 'agent' ? 'Agent' : 'Tool'} step "${entryId}" cannot be stored: option "${path}" ${reason} and does not round-trip through storage. Remove it or move that logic outside the persisted workflow.`,
    );
  };
  if (value === null) return;
  switch (typeof value) {
    case 'string':
    case 'boolean':
      return;
    case 'number':
      if (!Number.isFinite(value)) fail('is a non-finite number');
      return;
    case 'undefined':
      // Only reachable for object properties (array holes are rejected below);
      // JSON.stringify drops the key, which is equivalent to absence.
      return;
    case 'function':
      fail('is a function');
      return;
    case 'symbol':
    case 'bigint':
      fail(`is a ${typeof value}`);
      return;
    default:
      break;
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) => {
      if (item === undefined) fail(`has an undefined element at "${path}[${index}]"`);
      assertStorableOptionValue(item, entryId, kind, `${path}[${index}]`);
    });
    return;
  }
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) {
    fail('is a class instance (not plain data)');
  }
  for (const [key, entryValue] of Object.entries(value as Record<string, unknown>)) {
    assertStorableOptionValue(entryValue, entryId, kind, `${path}.${key}`);
  }
}

/**
 * Pull the storable fields out of the options bag carried on a live
 * agent/tool `SingleStepEntry`: `retries` / `metadata` for both kinds, plus
 * the JSON-safe agent execution passthrough set
 * ({@link SERIALIZED_AGENT_PASSTHROUGH_OPTION_KEYS}) that `runAgentEntry`
 * forwards verbatim to the agent run. Every other option — closures, live
 * instances, per-run identity, trust signals, and any key this walk doesn't
 * recognize — must hard-crash here rather than silently vanish through
 * storage.
 */
function pickSerializableStepOptions(
  options: any,
  entryId: string,
  kind: 'agent' | 'tool',
): SerializedStepOptions | undefined {
  if (!options || typeof options !== 'object') return undefined;

  // Closure-valued options don't round-trip. Fail loudly at serialize time so
  // the workflow author immediately learns their step won't persist rather
  // than discovering it in production when the callback silently no-ops.
  const forbidden: Array<{ key: string; hint: string }> = [
    { key: 'onFinish', hint: 'callback closure' },
    { key: 'onChunk', hint: 'callback closure' },
    { key: 'onError', hint: 'callback closure' },
    { key: 'onStepFinish', hint: 'callback closure' },
    { key: 'onAbort', hint: 'callback closure' },
    { key: 'toolChoice', hint: 'may be a function' },
    { key: 'requireToolApproval', hint: 'may be a function' },
  ];
  for (const { key, hint } of forbidden) {
    if (typeof options[key] === 'function') {
      throw new Error(
        `${kind === 'agent' ? 'Agent' : 'Tool'} step "${entryId}" cannot be stored: option "${key}" is a ${hint} that does not round-trip. Remove it or move that logic outside the persisted workflow.`,
      );
    }
  }
  // Scorer configs don't survive storage in ANY form — `SerializedStepOptions`
  // has no scorers field, so a rehydrated step would silently run unscored.
  // Fail loudly for static configs exactly like for closures.
  if (options.scorers !== undefined) {
    throw new Error(
      `${kind === 'agent' ? 'Agent' : 'Tool'} step "${entryId}" cannot be stored: option "scorers" does not round-trip through storage (scorer configs are not serialized). Remove it or attach scoring outside the persisted workflow.`,
    );
  }

  const passthroughKeys = new Set<string>(SERIALIZED_AGENT_PASSTHROUGH_OPTION_KEYS);
  const out: SerializedStepOptions = {};
  if (typeof options.retries === 'number') out.retries = options.retries;
  if (options.metadata && typeof options.metadata === 'object') {
    assertStorableOptionValue(options.metadata, entryId, kind, 'metadata');
    out.metadata = options.metadata as Record<string, any>;
  }
  for (const key of Object.keys(options)) {
    const value = options[key];
    if (value === undefined) continue;
    // Handled above (retries/metadata) or elsewhere (structuredOutput is
    // captured as the entry's `outputSchema`; the callback/scorers rejections
    // already fired for function values).
    if (key === 'retries' || key === 'metadata' || key === 'structuredOutput') continue;
    if (kind === 'agent' && passthroughKeys.has(key)) {
      assertStorableOptionValue(value, entryId, kind, key);
      (out as Record<string, unknown>)[key] = value;
      continue;
    }
    const hint = NON_STORABLE_STEP_OPTION_HINTS[key];
    throw new Error(
      `${kind === 'agent' ? 'Agent' : 'Tool'} step "${entryId}" cannot be stored: option "${key}" does not round-trip through storage${
        hint
          ? ` (${hint})`
          : ' (it is not in the serialized subset, so a rehydrated step would silently run without it)'
      }. Remove it or move that logic outside the persisted workflow.`,
    );
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * If the agent-step options carry `structuredOutput.schema`, that schema IS
 * the step's output shape (see `createStepFromAgent`). Emit it as JSON Schema
 * so rehydration can wire the same structured output back in.
 */
function extractStructuredOutputJsonSchema(options: any, entryId: string): Record<string, any> | undefined {
  const raw = options?.structuredOutput?.schema;
  if (raw === undefined || raw === null) return undefined;
  const admitted = getAdmittedJsonSchema(raw);
  if (admitted !== undefined) return admitted;
  try {
    // `.agent()`'s typed overload requires a StandardSchemaWithJSON, but the
    // any-form accepts a raw Zod schema. Normalize either shape here so the
    // storage form is consistent.
    const standard = toStandardSchema(raw);
    return standardSchemaToJSONSchema(standard) as Record<string, any>;
  } catch (e) {
    throw new Error(
      `Agent step "${entryId}" cannot be stored: structuredOutput.schema is not convertible to JSON Schema (${(e as Error).message}).`,
    );
  }
}
