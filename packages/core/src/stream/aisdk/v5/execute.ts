import { isProxy } from 'node:util/types';
import { getModelCapabilities as getAnthropicModelCapabilities } from '@ai-sdk/anthropic-v6/internal';
import {
  injectJsonInstructionIntoMessages as injectJsonInstructionIntoMessagesV3,
  isAbortError,
} from '@ai-sdk/provider-utils-v6';
import type { LanguageModelV2CallOptions, LanguageModelV2Prompt } from '@ai-sdk/provider-v5';
import { APICallError } from '@internal/ai-sdk-v5';
import type { IdGenerator, ToolChoice, ToolSet } from '@internal/ai-sdk-v5';
import { prepareJsonSchemaForOpenAIStrictMode } from '@mastra/schema-compat';
import type { StructuredOutputOptions, StructuredOutputSourceDecision } from '../../../agent/types';
import { AISDKV5LanguageModel, applyStrictForV2 } from '../../../llm/model/aisdk/v5/model';
import { validateModelTimeoutSettings } from '../../../llm/model/model-settings';
import type { ModelMethodType } from '../../../llm/model/model.loop.types';
import { modelSupportsStructuredOutput, modelSupportsTemperature } from '../../../llm/model/provider-registry';
import { ModelRouterLanguageModel } from '../../../llm/model/router';
import type { MastraLanguageModel, SharedProviderOptions } from '../../../llm/model/shared.types';
import {
  createTimeoutAbortSignal,
  guardStreamUntilMatch,
  guardStreamWithAbort,
  isMastraTimeoutError,
  raceAgainstAbort,
} from '../../../loop/timeout';
import type { LoopOptions } from '../../../loop/types';
import type { ModelInferenceFinishPayload, PreparedModelRequestMetrics } from '../../../observability';
import { createExactJsonMeasurementSnapshot } from '../../../observability/content-free-measurement';
import type { ExactJsonMeasurementSnapshot } from '../../../observability/content-free-measurement';
import type { PublicSchema } from '../../../schema';
import { DEFAULT_MAX_RETRY_AFTER_MS, getRetryAfterMs, waitDelay } from '../../../utils/retry-after';
import {
  getResponseFormat,
  getSemanticJsonSchema,
  getSemanticResponseSchema,
  hasVerifiedObjectOpenness,
} from '../../base/schema';
import type { ChunkType, LanguageModelV2StreamResult, OnResult } from '../../types';
import { attachModelStreamTransport, readModelStreamTransport } from '../../types';
import { prepareToolsAndToolChoice, preparedToolKey } from './compat';
import type { ModelSpecVersion } from './compat';
import { AISDKV5InputStream } from './input';

type JsonPromptInjection = StructuredOutputOptions<unknown>['jsonPromptInjection'];
type ResolvedJsonPromptInjection = Exclude<JsonPromptInjection, 'auto'>;

/**
 * p-retry's own defaults, pinned explicitly so the delay it schedules between
 * model-call attempts can be computed when honoring a provider `Retry-After`.
 */
const RETRY_MIN_TIMEOUT_MS = 1_000;
const RETRY_BACKOFF_FACTOR = 2;

const CONTENT_CHUNK_TYPES = new Set([
  'text-delta',
  'reasoning-delta',
  'tool-call',
  'tool-call-delta',
  'tool-input-delta',
  'tool-result',
  'object',
  'object-result',
  'file',
  'source',
]);

function isContentChunk(chunk: unknown): boolean {
  return (
    typeof chunk === 'object' && chunk !== null && CONTENT_CHUNK_TYPES.has((chunk as { type?: string }).type ?? '')
  );
}

/**
 * Whether a failed model call will be retried. Used by both `onFailedAttempt` and
 * `shouldRetry` so a terminal error never waits out a provider delay it will not use.
 */
function isRetryableModelError(error: unknown): boolean {
  try {
    // A step budget covers the whole attempt, so burning what is left of it on retries
    // against the same model would only delay the fallback to the next one.
    if (isMastraTimeoutError(error)) return false;
    // Guarded together: both predicates walk the thrown value's prototype chain
    // (`instanceof` / `name`), which a hostile value can make throw.
    if (isAbortError(error)) return false;
  } catch {
    // A hostile thrown value must not make observability or retry inspection throw.
  }
  if (APICallError.isInstance(error)) {
    return error.isRetryable;
  }
  return true;
}

/**
 * Whether a JSON schema has an object with free-form keys (e.g. `z.record()`), which OpenAI
 * strict mode cannot express: it requires every object to list fixed keys with
 * `additionalProperties: false`.
 */
export function hasFreeFormObject(schema: unknown): boolean {
  return someSchemaNode(schema, declaresFreeFormKeys);
}

function declaresFreeFormKeys(node: Record<string, unknown>): boolean {
  const additional = node.additionalProperties;
  if (additional === true || (additional && typeof additional === 'object')) return true;
  return Boolean(node.patternProperties || node.propertyNames);
}

/** Whether `predicate` holds for the schema or any of its subschemas. */
function someSchemaNode(schema: unknown, predicate: (node: Record<string, unknown>) => boolean): boolean {
  if (!schema || typeof schema !== 'object' || Array.isArray(schema)) return false;
  const node = schema as Record<string, unknown>;
  if (predicate(node)) return true;

  // Only descend into keywords that hold subschemas. Data keywords (`default`, `const`,
  // `enum`, `examples`) and property names are not schemas and must not trigger a match.
  const subschemas: unknown[] = [];
  for (const key of ['properties', '$defs', 'definitions'] as const) {
    const map = node[key];
    if (map && typeof map === 'object' && !Array.isArray(map)) subschemas.push(...Object.values(map));
  }
  for (const key of ['anyOf', 'oneOf', 'allOf'] as const) {
    const list = node[key];
    if (Array.isArray(list)) subschemas.push(...list);
  }
  const items = node.items;
  if (Array.isArray(items)) subschemas.push(...items);
  else if (items) subschemas.push(items);
  if (Array.isArray(node.prefixItems)) subschemas.push(...node.prefixItems);
  return subschemas.some(subschema => someSchemaNode(subschema, predicate));
}

/**
 * Anthropic structured-output complexity limits. They apply to the combined total across every
 * strict schema in one request (`output_config.format` and `strict: true` tools); a request over
 * either limit is rejected before generation.
 * @see https://platform.claude.com/docs/en/build-with-claude/structured-outputs ("Optional
 * parameters: 24", "Parameters with union types: 16" — `anyOf` or type arrays)
 */
export const ANTHROPIC_STRUCTURED_OUTPUT_LIMITS = { optionalParameters: 24, unionParameters: 16 } as const;

/**
 * Counts the parameters Anthropic's structured-output limits are measured in: properties not listed
 * in their object's `required`, and subschemas that use `anyOf`/`oneOf` (the Anthropic provider
 * sends `oneOf` as `anyOf`) or a type array. Walks the same subschema keywords as
 * {@link hasFreeFormObject}.
 */
export function countStructuredOutputParameters(schema: unknown): { optional: number; union: number } {
  const totals = { optional: 0, union: 0 };
  const visit = (value: unknown) => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return;
    const node = value as Record<string, unknown>;
    if (Array.isArray(node.anyOf) || Array.isArray(node.oneOf) || (Array.isArray(node.type) && node.type.length > 1)) {
      totals.union++;
    }
    const properties = node.properties;
    if (properties && typeof properties === 'object' && !Array.isArray(properties)) {
      const required = new Set(Array.isArray(node.required) ? node.required : []);
      for (const key of Object.keys(properties)) if (!required.has(key)) totals.optional++;
    }
    for (const key of ['properties', '$defs', 'definitions'] as const) {
      const map = node[key];
      if (map && typeof map === 'object' && !Array.isArray(map)) Object.values(map).forEach(visit);
    }
    for (const key of ['anyOf', 'oneOf', 'allOf', 'prefixItems'] as const) {
      const list = node[key];
      if (Array.isArray(list)) list.forEach(visit);
    }
    if (Array.isArray(node.items)) node.items.forEach(visit);
    else visit(node.items);
  };
  visit(schema);
  return totals;
}

/**
 * Whether a model may be served by `@ai-sdk/anthropic` (or behave like it). Matched by provider id or,
 * because `createAnthropic({ name })` renames the provider, by the same Claude model-id test the
 * pinned adapter uses (`isKnownModel || modelId.includes('claude-')`). Over-matching only ever sends
 * a schema through the prompt; missing a Claude model would send it strict.
 */
const isAnthropicModel = (model: { provider: string; modelId: string }) =>
  /^anthropic(\.|$)/.test(model.provider) || model.modelId.includes('claude-');

/** Keywords that only annotate or constrain a value; `@ai-sdk/anthropic` keeps or describes them. */
const STRICT_VALUE_KEYWORDS = new Set([
  '$schema',
  '$id',
  '$comment',
  'title',
  'description',
  'default',
  'examples',
  'deprecated',
  'readOnly',
  'writeOnly',
  'x-date',
  'type',
  'enum',
  'const',
  'format',
  'pattern',
  'minLength',
  'maxLength',
  'minimum',
  'maximum',
  'exclusiveMinimum',
  'exclusiveMaximum',
  'multipleOf',
  'minItems',
  'maxItems',
  'uniqueItems',
]);
/** Structural keywords whose subschemas are checked in turn. */
const STRICT_STRUCTURE_KEYWORDS = new Set([
  'properties',
  'required',
  'additionalProperties',
  'items',
  'anyOf',
  'oneOf',
  '$defs',
  'definitions',
]);

/**
 * Whether Anthropic's strict output format provably carries a schema node and everything under it.
 * True only for recognised shapes, so any keyword or shape not listed here means "not carriable":
 * - every keyword is a value keyword ({@link STRICT_VALUE_KEYWORDS}) or a structural one
 *   ({@link STRICT_STRUCTURE_KEYWORDS});
 * - the node has a `type`, `enum`, `const`, `anyOf` or `oneOf` (an empty schema accepts
 *   anything, which strict output cannot express); `$ref` is never carriable (Anthropic rejects
 *   recursive and external references, and Zod only emits references for recursion);
 * - an object node is typed `object`, closed (`additionalProperties: false`) and every property
 *   schema is carriable; a typeless node with `properties` / `required` / `additionalProperties` is not;
 * - an array node has `items`, and it is one carriable schema (no tuples, `additionalItems`,
 *   `prefixItems`, or an array without `items`, which admits any value);
 * - every `anyOf` / `oneOf` branch and `$defs` / `definitions` entry is carriable.
 * So `allOf` (intersections), `patternProperties`, `propertyNames`, `unevaluated*`, `not`,
 * conditionals and open objects (records, catchall, passthrough, loose objects, or JSON Schema
 * objects without `additionalProperties: false`) are not carriable.
 */
function isStrictOutputCarriable(schema: unknown): boolean {
  if (!schema || typeof schema !== 'object' || Array.isArray(schema)) return false;
  const node = schema as Record<string, unknown>;
  for (const key of Object.keys(node)) {
    if (!STRICT_VALUE_KEYWORDS.has(key) && !STRICT_STRUCTURE_KEYWORDS.has(key)) return false;
  }
  if (!['type', 'enum', 'const', 'anyOf', 'oneOf'].some(key => key in node)) return false;
  // Anthropic rejects complex values inside enums; `const` is held to the same rule.
  if ('enum' in node && (!Array.isArray(node.enum) || !node.enum.every(isPrimitiveJsonValue))) return false;
  if ('const' in node && !isPrimitiveJsonValue(node.const)) return false;

  const types = Array.isArray(node.type) ? node.type : [node.type];
  // Enum and const values must have one of the node's declared types (a contradiction can't be met).
  if ('type' in node) {
    const values = [...(Array.isArray(node.enum) ? node.enum : []), ...('const' in node ? [node.const] : [])];
    if (
      !values.every(
        value => types.includes(jsonValueType(value)) || (types.includes('number') && typeof value === 'number'),
      )
    ) {
      return false;
    }
  }
  if ('properties' in node || 'required' in node || 'additionalProperties' in node || types.includes('object')) {
    if (!types.includes('object') || node.additionalProperties !== false) return false;
    // `@ai-sdk/anthropic` drops `additionalProperties` from an object without properties, so only an
    // object that lists its properties stays closed on the wire.
    const properties = node.properties;
    if (!properties || typeof properties !== 'object' || Array.isArray(properties)) return false;
    if (Object.keys(properties).length === 0) return false;
    if (!Object.values(properties).every(isStrictOutputCarriable)) return false;
  }
  // An array admits any value unless one carriable `items` schema constrains it.
  if (types.includes('array') || 'items' in node) {
    if (!('items' in node) || Array.isArray(node.items) || !isStrictOutputCarriable(node.items)) return false;
  }
  for (const key of ['anyOf', 'oneOf'] as const) {
    if (key in node && (!Array.isArray(node[key]) || !(node[key] as unknown[]).every(isStrictOutputCarriable))) {
      return false;
    }
  }
  for (const key of ['$defs', 'definitions'] as const) {
    const map = node[key];
    if (map === undefined) continue;
    if (!map || typeof map !== 'object' || Array.isArray(map)) return false;
    if (!Object.values(map).every(isStrictOutputCarriable)) return false;
  }
  return true;
}

function isPrimitiveJsonValue(value: unknown): boolean {
  return value === null || ['string', 'number', 'boolean'].includes(typeof value);
}

/** The JSON Schema type of a primitive value (`integer` for whole numbers). */
function jsonValueType(value: unknown): string {
  if (value === null) return 'null';
  if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'number';
  return typeof value;
}

/**
 * The single PF-7309 decision: whether Anthropic's strict native output format provably carries the
 * caller's response schema. It is made on {@link getSemanticResponseSchema} (each object as open as
 * the caller's schema library makes it), never on a normalized or compat-converted schema, because
 * those converters close objects. Strict is allowed only when the schema is carriable
 * ({@link isStrictOutputCarriable}) and within {@link ANTHROPIC_STRUCTURED_OUTPUT_LIMITS};
 * anything else goes through the prompt.
 */
export function anthropicStrictOutputCarries(schema: unknown): boolean {
  // Anthropic documents object roots only; a root union is not provably accepted. Arrays and enums
  // already arrive wrapped in an object by the output-format wrapping.
  if ((schema as { type?: unknown } | undefined)?.type !== 'object') return false;
  if (!isStrictOutputCarriable(schema)) return false;
  const { optional, union } = countStructuredOutputParameters(schema);
  return (
    optional <= ANTHROPIC_STRUCTURED_OUTPUT_LIMITS.optionalParameters &&
    union <= ANTHROPIC_STRUCTURED_OUTPUT_LIMITS.unionParameters
  );
}

/**
 * Records the {@link StructuredOutputSourceDecision} for a caller's schema: the semantic response
 * schema (what prompt injection sends) and whether strict output carries it. Strict output is only
 * provable for a schema library whose object openness Mastra reads soundly
 * ({@link hasVerifiedObjectOpenness}); any other library's schema goes through the prompt.
 */
export function recordStructuredOutputSourceDecision(schema: unknown): StructuredOutputSourceDecision | undefined {
  if (!schema) return undefined;
  const semantic = getSemanticResponseSchema(schema as PublicSchema);
  const carries = hasVerifiedObjectOpenness(schema as PublicSchema) && anthropicStrictOutputCarries(semantic);
  return {
    schema: semantic,
    strictOutputCannotCarry: !carries,
    fingerprint: structuredOutputSchemaFingerprint(schema),
  };
}

/**
 * A canonical fingerprint of the schema a decision was recorded for: the stable JSON of its
 * semantic JSON Schema. A durable run's serialized schema has the same fingerprint as the caller's
 * schema it was serialized from, so a reload keeps the recorded decision (including a fallback for
 * a library whose openness only the original schema could tell) instead of re-reading the JSON.
 */
export function structuredOutputSchemaFingerprint(schema: unknown): string {
  // The root `$schema` is a dialect marker, which the JSON Schema wrapper adds on a reload.
  const { $schema: _dialect, ...semantic } = getSemanticJsonSchema(schema as PublicSchema);
  return stableJson(semantic);
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    const entries = Object.keys(value)
      .sort()
      .map(key => `${JSON.stringify(key)}:${stableJson((value as Record<string, unknown>)[key])}`);
    return `{${entries.join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

/** The step's tool selection, whose strict tools count toward Anthropic's request-wide limits. */
type StrictToolSource = { tools?: unknown; activeTools?: readonly string[]; toolChoice?: unknown };
type DecisionModel = { provider: string; modelId: string; specificationVersion?: string };

/**
 * The input schemas of the tools the candidate native request would send as `strict: true`: the
 * same {@link prepareToolsAndToolChoice} call that builds the request (the step's `toolChoice` and
 * `activeTools`, native-format stripping, the model's spec version), then, for a V2 model, the
 * {@link applyStrictForV2} step that removes per-tool `strict` before dispatch.
 */
function strictToolInputSchemas({ tools, activeTools, toolChoice }: StrictToolSource, model: DecisionModel): unknown[] {
  if (!tools || typeof tools !== 'object') return [];
  if (!Object.values(tools).some(tool => (tool as { strict?: unknown } | undefined)?.strict === true)) return [];
  type PrepareArgs = Parameters<typeof prepareToolsAndToolChoice>[0];
  const targetVersion: ModelSpecVersion =
    model.specificationVersion === 'v4' ? 'v4' : model.specificationVersion === 'v3' ? 'v3' : 'v2';
  const prepared = prepareToolsAndToolChoice({
    tools: tools as PrepareArgs['tools'],
    toolChoice: toolChoice as PrepareArgs['toolChoice'],
    activeTools: activeTools as PrepareArgs['activeTools'],
    targetVersion,
    stripToolsWhenNone: true,
  });
  // Only a model Mastra dispatches through AISDKV5LanguageModel loses per-tool `strict`
  // (applyStrictForV2 in its doStream/doGenerate). Every other model, including a router model whose
  // adapter version is only known after it resolves the gateway model at dispatch, keeps `strict`
  // in this count.
  const sent =
    model instanceof AISDKV5LanguageModel
      ? (applyStrictForV2({ prompt: [], tools: prepared.tools } as unknown as LanguageModelV2CallOptions).tools ?? [])
      : (prepared.tools ?? []);
  return sent
    .filter(tool => (tool as { strict?: unknown }).strict === true)
    .map(tool => (tool as { inputSchema?: unknown }).inputSchema);
}

/**
 * Whether one Anthropic request stays within {@link ANTHROPIC_STRUCTURED_OUTPUT_LIMITS} with the
 * response schema sent natively. The limits are request-wide: the response schema counts as the
 * larger of its recorded source and its compat-converted wire form (the Anthropic compat layer can
 * turn a root union of objects into one object with more optional properties), and every active
 * `strict: true` tool's input schema is added. Checked per request, because tools vary per step;
 * it only ever moves a carriable schema to the prompt.
 */
function anthropicRequestWithinLimits(
  schema: unknown,
  recordedSchema: unknown,
  model: DecisionModel,
  tools: StrictToolSource,
): boolean {
  const wire = getResponseFormat(schema as Parameters<typeof getResponseFormat>[0], {
    model: { provider: model.provider, modelId: model.modelId, supportsStructuredOutputs: true },
  });
  const source = countStructuredOutputParameters(recordedSchema);
  const converted = countStructuredOutputParameters(wire.type === 'json' ? wire.schema : undefined);
  let optional = Math.max(source.optional, converted.optional);
  let union = Math.max(source.union, converted.union);
  for (const inputSchema of strictToolInputSchemas(tools, model)) {
    const counts = countStructuredOutputParameters(inputSchema);
    optional += counts.optional;
    union += counts.union;
  }
  return (
    optional <= ANTHROPIC_STRUCTURED_OUTPUT_LIMITS.optionalParameters &&
    union <= ANTHROPIC_STRUCTURED_OUTPUT_LIMITS.unionParameters
  );
}

/**
 * The decision for structured-output options entering Mastra (the Agent API boundary, durable
 * preparation, a structuring pass): one already recorded upstream is kept, otherwise it is recorded
 * from the options' schema.
 */
export function structuredOutputSourceDecision(structuredOutput: {
  schema?: unknown;
  sourceDecision?: StructuredOutputSourceDecision;
}): StructuredOutputSourceDecision | undefined {
  return structuredOutput.sourceDecision ?? recordStructuredOutputSourceDecision(structuredOutput.schema);
}

function readStrictJsonSchemaOption(options: unknown): boolean | undefined {
  if (!options || typeof options !== 'object') return undefined;
  const strictJsonSchema = (options as { strictJsonSchema?: unknown }).strictJsonSchema;
  return typeof strictJsonSchema === 'boolean' ? strictJsonSchema : undefined;
}

/**
 * Whether the model sends structured output as an OpenAI strict json_schema, which
 * requires the schema to be prepared (all properties required, no unsupported keywords).
 *
 * OpenAI providers are detected by provider id. Other provider instances (for example
 * `@ai-sdk/openai-compatible` models created with `supportsStructuredOutputs: true`) are
 * detected by capability, honoring a `strictJsonSchema` override in their provider options.
 * Model-router models always advertise structured output support, so they keep deciding by
 * provider id.
 */
// Native @ai-sdk/openai ids are 'openai.chat', 'openai.responses', ...; 'openai-compatible.*' reads its own provider options.
const isNativeOpenAIProvider = (provider: string) => /^openai(\.|$)/.test(provider);

export function usesOpenAIStrictJsonSchema(
  model: Pick<MastraLanguageModel, 'provider'> & { supportsStructuredOutputs?: unknown },
  providerOptions?: SharedProviderOptions,
): boolean {
  if (isNativeOpenAIProvider(model.provider)) return true;
  if (model instanceof ModelRouterLanguageModel || model.supportsStructuredOutputs !== true) return false;

  const providerKey = model.provider.split('.')[0]!;
  const camelCaseProviderKey = providerKey
    .split(/[-_]/)
    .map((part, i) => (i === 0 ? part : part.charAt(0).toUpperCase() + part.slice(1)))
    .join('');
  const options = providerOptions as Record<string, unknown> | undefined;
  // Highest precedence first, matching @ai-sdk/openai-compatible (the only version that sends `strict`).
  for (const key of new Set([camelCaseProviderKey, providerKey, 'openaiCompatible', 'openai-compatible'])) {
    const strictJsonSchema = readStrictJsonSchemaOption(options?.[key]);
    if (strictJsonSchema !== undefined) return strictJsonSchema;
  }

  // @ai-sdk/openai-compatible sends strict json_schema by default.
  return true;
}

export function resolveJsonPromptInjection(
  value: JsonPromptInjection,
  capability: boolean | undefined,
): ResolvedJsonPromptInjection {
  if (value !== 'auto') return value;
  return capability === true ? undefined : 'inline';
}

/**
 * Whether a step sends a native structured-output schema (`responseFormat`) to the model.
 * Tools are dropped under `toolChoice: 'none'` only in this case (#14459); when the schema is
 * injected into the prompt instead, tools are kept so providers retain tool history (#25908).
 */
export function sendsNativeResponseFormat(
  structuredOutput:
    | {
        schema?: unknown;
        model?: unknown;
        jsonPromptInjection?: JsonPromptInjection;
        sourceDecision?: StructuredOutputSourceDecision;
      }
    | undefined,
  model: DecisionModel,
  providerOptions?: SharedProviderOptions,
  tools: StrictToolSource = {},
): boolean {
  if (!structuredOutput?.schema || structuredOutput.model) return false;
  return !resolveDirectStructuredOutputInjection(structuredOutput, model, providerOptions, tools).mode;
}

function jsonSchemaOf(responseFormat: ReturnType<typeof getResponseFormat> | undefined): unknown {
  return responseFormat?.type === 'json' ? responseFormat.schema : undefined;
}

/**
 * An explicit `providerOptions.anthropic.structuredOutputMode` of `'outputFormat'` or `'jsonTool'`
 * chooses how `@ai-sdk/anthropic` carries the schema, so Mastra keeps the native response format for
 * it. `'auto'` is the provider's default and leaves the fallback in place. The provider also merges a
 * custom provider-name key over `anthropic`; the fallback only applies to `anthropic` / `anthropic.*`
 * providers, whose provider-options key is `anthropic`.
 */
function choosesAnthropicStructuredOutputMode(
  providerOptions: SharedProviderOptions | undefined,
  model: DecisionModel,
): boolean {
  // The adapter's merge (`@ai-sdk/anthropic` 3.0.133): `Object.assign({}, anthropic, ownKey)`, where
  // `ownKey` is its provider-name key unless that is `anthropic`. A key present in `ownKey` wins
  // even when its value is `undefined`.
  const options = providerOptions as Record<string, object | undefined> | undefined;
  const providerKey = model.provider.split('.')[0]!;
  const merged: { structuredOutputMode?: unknown } = Object.assign(
    {},
    options?.anthropic ?? {},
    providerKey !== 'anthropic' ? (options?.[providerKey] ?? {}) : {},
  );
  const mode = merged.structuredOutputMode;
  if (mode === 'outputFormat') return true;
  if (mode !== 'jsonTool') return false;
  // The pinned adapter sends a JSON-schema response natively instead of as a JSON tool when the model
  // rejects forced tool use and supports structured output (`@ai-sdk/anthropic` 3.0.133,
  // `doStream`/`doGenerate` request args); then the decision and the request budget apply.
  const { rejectsForcedToolUse, supportsStructuredOutput } = getAnthropicModelCapabilities(model.modelId);
  return !(rejectsForcedToolUse && supportsStructuredOutput);
}

/**
 * Resolves how a direct (no structuring model) structured-output step carries its schema: through
 * the prompt (`'system'` / `'inline'`) or as a native response format (`mode: undefined`).
 *
 * Anthropic's native output format is strict, so a schema it cannot carry falls back to inline
 * prompt injection, the same mode `'auto'` picks for models without native support. Mastra validates
 * the generated text against the schema either way. The decision is the recorded
 * `structuredOutput.sourceDecision` ({@link recordStructuredOutputSourceDecision}); only a caller
 * that reaches `execute` without one (a direct `execute` call) has it recorded here, from the schema
 * it passed.
 *
 * Precedence on Anthropic: `jsonPromptInjection: false` keeps the native format; an explicit
 * `structuredOutputMode` of `'outputFormat'` or `'jsonTool'` keeps it too, and also wins over
 * `jsonPromptInjection: 'auto'` (which otherwise resolves to inline injection for models without
 * registered native support). Whenever Anthropic gets the schema through `'auto'` or the fallback,
 * the prompt carries the source schema, so records stay open. Explicit `true` / `'system'` /
 * `'inline'` still inject the wire schema as before.
 */
function resolveDirectStructuredOutputInjection(
  structuredOutput: {
    schema?: unknown;
    jsonPromptInjection?: JsonPromptInjection;
    sourceDecision?: StructuredOutputSourceDecision;
  },
  model: DecisionModel,
  providerOptions: SharedProviderOptions | undefined,
  tools: StrictToolSource,
): { mode: 'system' | 'inline' | undefined; schema?: unknown } {
  const { jsonPromptInjection } = structuredOutput;
  if (jsonPromptInjection === false) return { mode: undefined };
  const anthropic = isAnthropicModel(model);
  const providerChoosesMode = anthropic && choosesAnthropicStructuredOutputMode(providerOptions, model);
  if (jsonPromptInjection === 'auto' && providerChoosesMode) return { mode: undefined };

  const decision = () =>
    structuredOutput.sourceDecision ?? recordStructuredOutputSourceDecision(structuredOutput.schema);
  const modelRoute = `${model.provider.split('.')[0]}/${model.modelId}`;
  const resolved = resolveJsonPromptInjection(
    jsonPromptInjection,
    jsonPromptInjection === 'auto' ? modelSupportsStructuredOutput(modelRoute) : undefined,
  );
  if (resolved) {
    const mode = resolved === true ? 'system' : resolved;
    return anthropic && jsonPromptInjection === 'auto' ? { mode, schema: decision()?.schema } : { mode };
  }
  if (!anthropic || providerChoosesMode) return { mode: undefined };
  const recorded = decision();
  if (!recorded) return { mode: undefined };
  if (recorded.strictOutputCannotCarry) return { mode: 'inline', schema: recorded.schema };
  return anthropicRequestWithinLimits(structuredOutput.schema, recorded.schema, model, tools)
    ? { mode: undefined }
    : { mode: 'inline', schema: recorded.schema };
}

type InjectJsonInstructionArgs = Parameters<typeof injectJsonInstructionIntoMessagesV3>[0];

/**
 * Typed V2 wrapper for the `@ai-sdk/provider-utils-v6` helper, which only reads and rewrites the
 * leading system message's string content — a shape shared by V2 and V3 prompts.
 */
function injectJsonInstructionIntoMessages(
  args: Omit<InjectJsonInstructionArgs, 'messages'> & { messages: LanguageModelV2Prompt },
): LanguageModelV2Prompt {
  return injectJsonInstructionIntoMessagesV3({
    ...args,
    messages: args.messages as unknown as InjectJsonInstructionArgs['messages'],
  }) as unknown as LanguageModelV2Prompt;
}

/**
 * Caller-supplied `structuredOutput.instructions` replace the generated schema dump only when
 * they carry actual text. Empty / whitespace-only values fall back to the generated instruction.
 */
function hasCompactInstructions(instructions: string | undefined): instructions is string {
  return typeof instructions === 'string' && instructions.trim().length > 0;
}

function buildJsonInstruction(schema: unknown, instructions?: string) {
  if (hasCompactInstructions(instructions)) {
    return instructions;
  }
  return `Return your response as JSON matching this schema:\n\n${JSON.stringify(schema)}\n\nReturn only valid JSON. Do not include markdown or explanatory text.`;
}

function injectJsonInstructionIntoLatestUserMessage({
  messages,
  schema,
  instructions,
}: {
  messages: LanguageModelV2Prompt;
  schema: unknown;
  instructions?: string;
}): LanguageModelV2Prompt {
  const instruction = buildJsonInstruction(schema, instructions);
  const prompt = messages.map(message => ({
    ...message,
    content: Array.isArray(message.content) ? [...message.content] : message.content,
  })) as LanguageModelV2Prompt;

  for (let i = prompt.length - 1; i >= 0; i--) {
    const message = prompt[i];
    if (message?.role !== 'user') {
      continue;
    }

    message.content = Array.isArray(message.content)
      ? [...message.content, { type: 'text', text: instruction }]
      : [
          { type: 'text', text: String(message.content ?? '') },
          { type: 'text', text: instruction },
        ];
    return prompt;
  }

  return [...prompt, { role: 'user', content: [{ type: 'text', text: instruction }] }] as LanguageModelV2Prompt;
}

function omit<T extends object, K extends keyof T>(obj: T, keys: K[]): Omit<T, K> {
  const newObj = { ...obj };
  for (const key of keys) {
    delete newObj[key];
  }
  return newObj;
}

type ProviderMessageRole = 'system' | 'user' | 'assistant' | 'tool' | 'other';

const measurementEncoder = new TextEncoder();

type OwnProperty = { state: 'absent' } | { state: 'data'; value: unknown } | { state: 'unsupported' };

function isDescriptorSafeObject(value: unknown): value is object {
  if (typeof value !== 'object' || value === null) return false;
  try {
    return !isProxy(value);
  } catch {
    return false;
  }
}

function ownProperty(value: unknown, key: string): OwnProperty {
  if (!isDescriptorSafeObject(value)) return { state: 'unsupported' };
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor === undefined) return { state: 'absent' };
    return 'value' in descriptor ? { state: 'data', value: descriptor.value } : { state: 'unsupported' };
  } catch {
    return { state: 'unsupported' };
  }
}

function providerMessageRole(message: unknown): ProviderMessageRole | undefined {
  if (!isDescriptorSafeObject(message)) return 'other';
  try {
    if (Array.isArray(message)) return 'other';
  } catch {
    return undefined;
  }
  const roleProperty = ownProperty(message, 'role');
  if (roleProperty.state === 'unsupported') return undefined;
  const role = roleProperty.state === 'data' ? roleProperty.value : undefined;
  return role === 'system' || role === 'user' || role === 'assistant' || role === 'tool' ? role : 'other';
}

function arrayLength(value: unknown): number | undefined {
  if (!isDescriptorSafeObject(value)) return undefined;
  try {
    if (!Array.isArray(value)) return undefined;
    const descriptor = Object.getOwnPropertyDescriptor(value, 'length');
    const length = descriptor && 'value' in descriptor ? descriptor.value : undefined;
    return Number.isSafeInteger(length) && length >= 0 ? length : undefined;
  } catch {
    return undefined;
  }
}

/** Serialize only values already detached by createExactJsonMeasurementSnapshot. */
function detachedJsonUtf8ByteLength(value: unknown): number | undefined {
  try {
    const serialized = JSON.stringify(value);
    return serialized === undefined ? undefined : measurementEncoder.encode(serialized).byteLength;
  } catch {
    return undefined;
  }
}

function providerMessageMeasurements(prompt: unknown, measureBytes: boolean) {
  const roles: Record<ProviderMessageRole, { count: number | undefined; bytes: number | undefined }> = {
    system: { count: 0, bytes: measureBytes ? 0 : undefined },
    user: { count: 0, bytes: measureBytes ? 0 : undefined },
    assistant: { count: 0, bytes: measureBytes ? 0 : undefined },
    tool: { count: 0, bytes: measureBytes ? 0 : undefined },
    other: { count: 0, bytes: measureBytes ? 0 : undefined },
  };
  const length = arrayLength(prompt);
  if (length === undefined) {
    for (const role of Object.values(roles)) role.count = undefined;
    return { count: undefined, roles, totalBytes: undefined };
  }
  let totalBytes: number | undefined = measureBytes ? 2 + Math.max(0, length - 1) : undefined;
  for (let index = 0; index < length; index += 1) {
    const item = ownProperty(prompt, String(index));
    const message = item.state === 'data' ? item.value : undefined;
    const role = providerMessageRole(message);
    if (item.state !== 'data' || role === undefined) {
      for (const metrics of Object.values(roles)) metrics.count = undefined;
      totalBytes = undefined;
      continue;
    }
    const roleMetrics = roles[role];
    if (roleMetrics.count !== undefined) roleMetrics.count += 1;
    if (!measureBytes) continue;
    const measured = detachedJsonUtf8ByteLength(message);
    if (measured === undefined) {
      roleMetrics.bytes = undefined;
      totalBytes = undefined;
      continue;
    }
    if (roleMetrics.bytes !== undefined) roleMetrics.bytes += measured;
    if (totalBytes !== undefined) totalBytes += measured;
  }
  return { count: length, roles, totalBytes };
}

const measuredProviderReasoningEfforts = new Set(['none', 'low', 'medium', 'high', 'xhigh', 'max']);

function plainRecord(value: unknown): Record<string, unknown> | undefined {
  if (!isDescriptorSafeObject(value)) return undefined;
  try {
    if (Array.isArray(value)) return undefined;
    const prototype = Reflect.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null ? (value as Record<string, unknown>) : undefined;
  } catch {
    return undefined;
  }
}

function preparedProviderReasoning(providerOptions: unknown) {
  const options = plainRecord(providerOptions);
  if (!options) return { providerReasoningEffortState: 'provider_default' as const };
  const candidates: unknown[] = [];
  let unsupported = false;
  for (const [providerKey, effortPath] of [
    ['azure', ['reasoningEffort']],
    ['openai', ['reasoningEffort']],
    ['openrouter', ['reasoning', 'effort']],
  ] as const) {
    const providerProperty = ownProperty(options, providerKey);
    if (providerProperty.state === 'absent') continue;
    if (providerProperty.state === 'unsupported') {
      unsupported = true;
      continue;
    }
    let current = plainRecord(providerProperty.value);
    if (current === undefined) {
      unsupported = true;
      continue;
    }
    for (const [index, key] of effortPath.entries()) {
      const property = ownProperty(current, key);
      if (property.state === 'absent') {
        current = undefined;
        break;
      }
      if (property.state === 'unsupported') {
        unsupported = true;
        current = undefined;
        break;
      }
      if (index === effortPath.length - 1) {
        if (property.value !== undefined) candidates.push(property.value);
      } else {
        current = plainRecord(property.value);
        if (current === undefined) {
          unsupported = true;
          break;
        }
      }
    }
  }
  if (unsupported) return { providerReasoningEffortState: 'unknown' as const };
  const effort = candidates[0];
  if (candidates.length !== 1 || typeof effort !== 'string' || !measuredProviderReasoningEfforts.has(effort)) {
    return candidates.length === 0
      ? { providerReasoningEffortState: 'provider_default' as const }
      : { providerReasoningEffortState: 'unknown' as const };
  }
  return {
    providerReasoningEffortState: 'measured' as const,
    providerReasoningEffort: effort as 'none' | 'low' | 'medium' | 'high' | 'xhigh' | 'max',
  };
}

type PreparedRequestMeasurement = Omit<
  PreparedModelRequestMetrics,
  'providerAttempt' | 'providerPreparationMs' | 'providerDispatchTimestampMs'
>;

function measuredSnapshotRecord(measurement: ExactJsonMeasurementSnapshot): Record<string, unknown> | undefined {
  return measurement.state === 'measured' ? plainRecord(measurement.snapshot) : undefined;
}

function preparedRequestMeasurement({
  providerRequest,
  providerOptions,
  prompt,
  responseSchema,
  responseSchemaInline,
}: {
  providerRequest: Record<string, unknown>;
  providerOptions: unknown;
  prompt: LanguageModelV2Prompt;
  responseSchema: unknown;
  responseSchemaInline: boolean;
}): PreparedRequestMeasurement {
  const measurementStartedAtMs = Date.now();
  // Reasoning effort is a small, fixed-path diagnostic independent of whether
  // the full provider envelope exceeds the exact-snapshot bound. This reads
  // only descriptor-safe, non-Proxy data properties and returns primitives.
  const providerReasoning = preparedProviderReasoning(providerOptions);
  // AbortSignal is a live cancellation handle, not provider request content.
  // Exclude it before detaching the exact JSON-safe envelope; every value used
  // after adapter dispatch then comes from this immutable snapshot or a
  // content-free primitive captured alongside it.
  const providerRequestMeasurement = createExactJsonMeasurementSnapshot(omit(providerRequest, ['abortSignal']));
  const providerRequestSnapshot = measuredSnapshotRecord(providerRequestMeasurement);
  const detachedPromptProperty = providerRequestSnapshot ? ownProperty(providerRequestSnapshot, 'prompt') : undefined;
  const detachedPrompt = detachedPromptProperty?.state === 'data' ? detachedPromptProperty.value : undefined;
  // Counts remain available when exact byte measurement is bounded out, but
  // only through descriptor reads guarded against Proxy traps. No content,
  // accessors, iterators, or serialization hooks are invoked.
  const fallbackMessageMeasurements = providerMessageMeasurements(prompt, false);
  const messageMeasurements = providerRequestSnapshot
    ? providerMessageMeasurements(detachedPrompt, true)
    : fallbackMessageMeasurements;
  const system = messageMeasurements.roles.system;
  const user = messageMeasurements.roles.user;
  const assistant = messageMeasurements.roles.assistant;
  const tool = messageMeasurements.roles.tool;
  const other = messageMeasurements.roles.other;
  const providerMessageBytes = messageMeasurements.totalBytes;
  const providerRequestBytes =
    providerRequestMeasurement.state === 'measured' ? providerRequestMeasurement.utf8ByteLength : undefined;
  const detachedToolsProperty = providerRequestSnapshot ? ownProperty(providerRequestSnapshot, 'tools') : undefined;
  const detachedTools = detachedToolsProperty?.state === 'data' ? detachedToolsProperty.value : undefined;
  const detachedProviderToolCount =
    detachedToolsProperty?.state === 'absent' ||
    (detachedToolsProperty?.state === 'data' && detachedToolsProperty.value === undefined)
      ? 0
      : detachedToolsProperty?.state === 'data'
        ? arrayLength(detachedToolsProperty.value)
        : undefined;
  const providerToolsProperty = ownProperty(providerRequest, 'tools');
  const fallbackProviderToolCount =
    providerToolsProperty.state === 'absent' ||
    (providerToolsProperty.state === 'data' && providerToolsProperty.value === undefined)
      ? 0
      : providerToolsProperty.state === 'data'
        ? arrayLength(providerToolsProperty.value)
        : undefined;
  const measuredProviderToolCount = providerRequestSnapshot ? detachedProviderToolCount : fallbackProviderToolCount;
  const providerToolCount = measuredProviderToolCount;
  const providerToolSchemaBytes =
    providerRequestSnapshot && providerToolCount !== undefined && providerToolCount > 0
      ? detachedJsonUtf8ByteLength(detachedTools)
      : undefined;
  const providerToolSchemaState =
    measuredProviderToolCount === undefined
      ? ('unknown' as const)
      : providerToolCount === 0
        ? ('not_applicable' as const)
        : providerToolSchemaBytes === undefined
          ? ('unknown' as const)
          : ('measured' as const);
  const responseSchemaMeasurement =
    responseSchema === undefined ? undefined : createExactJsonMeasurementSnapshot(responseSchema);
  const providerResponseSchemaBytes =
    responseSchemaMeasurement?.state === 'measured' ? responseSchemaMeasurement.utf8ByteLength : undefined;
  const providerResponseSchemaState =
    responseSchema === undefined
      ? ('not_applicable' as const)
      : providerResponseSchemaBytes === undefined
        ? ('unknown' as const)
        : responseSchemaInline
          ? ('inline_in_prompt' as const)
          : ('measured' as const);
  const measurementState =
    providerMessageBytes === undefined ||
    providerRequestBytes === undefined ||
    system.bytes === undefined ||
    user.bytes === undefined ||
    assistant.bytes === undefined ||
    tool.bytes === undefined ||
    other.bytes === undefined ||
    providerToolSchemaState === 'unknown' ||
    providerResponseSchemaState === 'unknown'
      ? ('unknown' as const)
      : ('measured' as const);
  const providerMeasurementMs = Math.max(0, Date.now() - measurementStartedAtMs);

  return {
    measurementState,
    providerBreakdownState: 'serialized_components_non_additive',
    ...(messageMeasurements.count === undefined ? {} : { providerMessageCount: messageMeasurements.count }),
    ...(providerMessageBytes === undefined ? {} : { providerMessageBytes }),
    ...(system.count === undefined ? {} : { providerSystemMessageCount: system.count }),
    ...(system.bytes === undefined ? {} : { providerSystemMessageBytes: system.bytes }),
    ...(user.count === undefined ? {} : { providerUserMessageCount: user.count }),
    ...(user.bytes === undefined ? {} : { providerUserMessageBytes: user.bytes }),
    ...(assistant.count === undefined ? {} : { providerAssistantMessageCount: assistant.count }),
    ...(assistant.bytes === undefined ? {} : { providerAssistantMessageBytes: assistant.bytes }),
    ...(tool.count === undefined ? {} : { providerToolMessageCount: tool.count }),
    ...(tool.bytes === undefined ? {} : { providerToolMessageBytes: tool.bytes }),
    ...(other.count === undefined ? {} : { providerOtherMessageCount: other.count }),
    ...(other.bytes === undefined ? {} : { providerOtherMessageBytes: other.bytes }),
    ...(system.bytes === undefined ? {} : { providerInstructionBytes: system.bytes }),
    ...(providerToolCount === undefined ? {} : { providerToolCount }),
    ...(providerToolSchemaBytes === undefined ? {} : { providerToolSchemaBytes }),
    providerToolSchemaState,
    ...(providerResponseSchemaBytes === undefined ? {} : { providerResponseSchemaBytes }),
    providerResponseSchemaState,
    ...providerReasoning,
    ...(providerRequestBytes === undefined ? {} : { providerRequestBytes }),
    providerMeasurementMs,
  };
}

type ExecutionProps<OUTPUT = undefined> = {
  runId: string;
  model: MastraLanguageModel;
  providerOptions?: SharedProviderOptions;
  inputMessages: LanguageModelV2Prompt;
  tools?: ToolSet;
  toolChoice?: ToolChoice<ToolSet>;
  activeTools?: string[];
  options?: {
    abortSignal?: AbortSignal;
  };
  includeRawChunks?: boolean;
  modelSettings?: LoopOptions['modelSettings'];
  onResult: OnResult;
  /** Receives only content-free measurements after the provider request has been dispatched. */
  onPreparedRequest?: (metrics: PreparedModelRequestMetrics) => void;
  /** Starts one tracing boundary immediately before each provider adapter invocation. */
  onProviderAttemptStart?: (providerAttempt: number) => void;
  /**
   * Receives the keys (in `tools`) of the tools the provider request advertises,
   * after tool preparation (none for `toolChoice: 'none'`), immediately before
   * each provider adapter invocation.
   */
  onProviderDispatch?: (advertisedToolKeys: string[]) => void;
  /** Closes only the failed provider-attempt boundary so a retry can open another. */
  onProviderAttemptError?: (input: { error: unknown; providerAttempt: number; aborted: boolean }) => void;
  /** Records the provider's first semantic chunk before downstream buffering. */
  onProviderFirstContent?: () => void;
  /** Records provider chunk timing before downstream buffering or processor backpressure. */
  onProviderChunk?: (chunk: ChunkType, observedAt: Date) => void;
  /** Records provider finish before downstream buffering or processor backpressure. */
  onProviderFinish?: (
    payload: ModelInferenceFinishPayload,
    endTime: Date,
    response: { responseId?: string; responseModel?: string },
  ) => void;
  /** Zero-based logical attempt offset for callers that own retries outside this adapter loop. */
  providerAttemptOffset?: number;
  structuredOutput?: StructuredOutputOptions<OUTPUT>;
  /**
  Additional HTTP headers to be sent with the request.
  Only applicable for HTTP-based providers.
  */
  headers?: Record<string, string | undefined>;
  shouldThrowError?: boolean;
  methodType: ModelMethodType;
  generateId?: IdGenerator;
};

export function execute<OUTPUT = undefined>({
  runId,
  model,
  providerOptions,
  inputMessages,
  tools,
  toolChoice,
  activeTools,
  options,
  onResult,
  onPreparedRequest,
  onProviderAttemptStart,
  onProviderDispatch,
  onProviderAttemptError,
  onProviderFirstContent,
  onProviderChunk,
  onProviderFinish,
  providerAttemptOffset = 0,
  includeRawChunks,
  modelSettings,
  structuredOutput,
  headers,
  shouldThrowError,
  methodType,
  generateId,
}: ExecutionProps<OUTPUT>) {
  const preparationStartedAtMs = Date.now();
  const v5 = new AISDKV5InputStream({
    component: 'LLM',
    name: model.modelId,
    generateId,
    onProviderFirstContent,
    onProviderChunk,
    onProviderFinish,
  });

  // Determine target version based on model's specificationVersion
  // V3 (AI SDK v6) and V4 (AI SDK v7) models need 'provider' type, V2 models need 'provider-defined'
  const targetVersion: ModelSpecVersion =
    model.specificationVersion === 'v4' ? 'v4' : model.specificationVersion === 'v3' ? 'v3' : 'v2';

  const structuredOutputMode = structuredOutput?.schema
    ? structuredOutput?.model
      ? 'processor'
      : 'direct'
    : undefined;

  const toolsAndToolChoice = prepareToolsAndToolChoice({
    tools,
    toolChoice,
    activeTools,
    targetVersion,
    stripToolsWhenNone: sendsNativeResponseFormat(structuredOutput, model, providerOptions, {
      tools,
      activeTools,
      toolChoice,
    }),
  });

  const responseFormat = structuredOutput?.schema
    ? getResponseFormat(structuredOutput?.schema, {
        model: {
          provider: model.provider,
          modelId: model.modelId,
          supportsStructuredOutputs: true,
        },
      })
    : undefined;

  let prompt = inputMessages;
  const jsonPromptInjection = structuredOutput?.jsonPromptInjection;
  const modelRoute = `${model.provider.split('.')[0]}/${model.modelId}`;
  const directInjection =
    structuredOutputMode === 'direct' && structuredOutput
      ? resolveDirectStructuredOutputInjection(structuredOutput, model, providerOptions, {
          tools,
          activeTools,
          toolChoice,
        })
      : undefined;
  const resolvedJsonPromptInjection = directInjection
    ? directInjection.mode
    : resolveJsonPromptInjection(
        jsonPromptInjection,
        jsonPromptInjection === 'auto' ? modelSupportsStructuredOutput(modelRoute) : undefined,
      );
  const injectionMode = resolvedJsonPromptInjection === true ? 'system' : resolvedJsonPromptInjection;
  // The Anthropic fallback injects the schema from before the provider compat layer; every other
  // injection keeps the wire schema it injected before.
  const injectedSchema = directInjection?.schema ?? jsonSchemaOf(responseFormat);

  // For direct mode (no model provided for structuring agent), inject JSON schema instruction if opting out of native response format with jsonPromptInjection
  if (structuredOutputMode === 'direct' && responseFormat?.type === 'json' && injectionMode) {
    const compactInstructions = hasCompactInstructions(structuredOutput?.instructions)
      ? structuredOutput.instructions
      : undefined;
    prompt =
      injectionMode === 'inline'
        ? injectJsonInstructionIntoLatestUserMessage({
            messages: inputMessages,
            schema: injectedSchema,
            instructions: compactInstructions,
          })
        : injectJsonInstructionIntoMessages({
            messages: inputMessages,
            // Compact instructions replace the schema dump entirely. Passing them in the
            // `schemaSuffix` slot (with no schema) suppresses the AI SDK's default generic
            // suffix, which would otherwise be appended when `schema` is nullish.
            ...(compactInstructions
              ? { schema: undefined, schemaPrefix: undefined, schemaSuffix: compactInstructions }
              : { schema: injectedSchema as typeof responseFormat.schema }),
          });
  }

  // For processor mode without agent reuse, inject a custom prompt to inform the main agent
  // about the structured output schema that the structuring agent will use.
  // An explicit `jsonPromptInjection: false` opts out of this hint.
  if (
    structuredOutputMode === 'processor' &&
    responseFormat?.type === 'json' &&
    responseFormat?.schema &&
    !structuredOutput?.useAgent &&
    jsonPromptInjection !== false
  ) {
    prompt = injectJsonInstructionIntoMessages({
      messages: inputMessages,
      schema: responseFormat.schema,
      schemaPrefix: `Your response will be processed by another agent to extract structured data. Please ensure your response contains comprehensive information for all the following fields that will be extracted:\n`,
      schemaSuffix: `\n\nYou don't need to format your response as JSON unless the user asks you to. Just ensure your natural language response includes relevant information for each field in the schema above.`,
    });
  }

  /**
   * Enable OpenAI's strict JSON schema mode to ensure schema compliance.
   * Without this, OpenAI may omit required fields or violate type constraints.
   * @see https://platform.openai.com/docs/guides/structured-outputs#structured-outputs-vs-json-mode
   * @see https://ai-sdk.dev/docs/ai-sdk-core/generating-structured-data#accessing-reasoning
   */
  // Strict json_schema (OpenAI, and OpenAI-compatible providers with native structured output)
  // requires all properties to be required, additionalProperties: false, and no unsupported keywords.
  const isOpenAIProvider = isNativeOpenAIProvider(model.provider);
  // OpenAI rejects free-form objects in strict mode, so send those schemas non-strict instead of
  // rewriting them into objects that can hold no keys. Mastra still validates the result against the schema.
  const sendNonStrict =
    isOpenAIProvider &&
    providerOptions?.openai?.strictJsonSchema !== true &&
    hasFreeFormObject(responseFormat?.type === 'json' ? responseFormat.schema : undefined);
  const isStrictJsonSchemaMode =
    responseFormat?.type === 'json' &&
    !injectionMode &&
    !sendNonStrict &&
    usesOpenAIStrictJsonSchema(model, providerOptions);

  if (isStrictJsonSchemaMode && responseFormat?.schema) {
    responseFormat.schema = prepareJsonSchemaForOpenAIStrictMode(responseFormat.schema);
  }

  // Only OpenAI providers read `providerOptions.openai`; OpenAI-compatible providers send strict by default
  // and read their own provider key.
  const providerOptionsToUse: SharedProviderOptions | undefined =
    (isStrictJsonSchemaMode || sendNonStrict) && isOpenAIProvider
      ? {
          ...(providerOptions ?? {}),
          openai: {
            strictJsonSchema: isStrictJsonSchemaMode,
            ...(providerOptions?.openai ?? {}),
          },
        }
      : providerOptions;

  const providerResponseFormat = structuredOutputMode === 'direct' && !injectionMode ? responseFormat : undefined;
  const responseSchema = responseFormat?.type === 'json' ? responseFormat.schema : undefined;
  const responseSchemaInline =
    responseSchema !== undefined &&
    (injectionMode !== undefined || (structuredOutputMode === 'processor' && !structuredOutput?.useAgent));
  const responseSchemaForMeasurement =
    providerResponseFormat?.type === 'json'
      ? providerResponseFormat.schema
      : responseSchemaInline
        ? injectionMode !== undefined
          ? injectedSchema
          : responseSchema
        : undefined;
  const preparationBeforeProviderCallbackMs = Math.max(0, Date.now() - preparationStartedAtMs);

  const stream = v5.initialize({
    runId,
    onResult,
    createStream: async () => {
      const preparationResumedAtMs = Date.now();
      try {
        const timeout = validateModelTimeoutSettings(modelSettings?.timeout);
        let filteredModelSettings = omit(modelSettings || {}, ['maxRetries', 'headers', 'timeout']);

        // Capability-gated stripping of sampling params for models that reject them
        // (e.g. Claude Sonnet 5, reasoning models). The model router applies this for
        // router-id models, but provider instances passed directly (e.g. @ai-sdk/anthropic,
        // @ai-sdk/amazon-bedrock) never enter the router, so strip here on the shared path
        // too. Only drop on an explicit `false`; leave `true`/`undefined` untouched.
        if (modelSupportsTemperature(modelRoute) === false) {
          filteredModelSettings = omit(filteredModelSettings, ['temperature', 'topP', 'topK']);
        }

        // Bound this single model call by modelSettings.timeout.stepMs, composed with
        // whatever signal the run already carries. The budget stays armed until the
        // returned stream ends, so a provider that stalls mid-stream is caught too.
        const { signal: abortSignal, cleanup: cleanupStepTimeout } = createTimeoutAbortSignal({
          parentSignal: options?.abortSignal,
          timeoutMs: timeout?.stepMs,
          timeoutType: 'step',
        });
        let callAbortSignal = abortSignal;
        let cleanupFirstChunkTimeout = () => {};

        const pRetry = await import('p-retry');
        const retryResult = await pRetry
          .default(
            async attemptNumber => {
              const providerAttempt = providerAttemptOffset + attemptNumber;
              const firstChunkTimeout = createTimeoutAbortSignal({
                parentSignal: abortSignal,
                timeoutMs: methodType === 'stream' ? timeout?.firstChunkMs : undefined,
                timeoutType: 'firstChunk',
              });
              callAbortSignal = firstChunkTimeout.signal;
              cleanupFirstChunkTimeout = firstChunkTimeout.cleanup;
              // Starts after p-retry has completed any backoff, so retry delay is
              // never reported as provider request preparation.
              const attemptPreparationStartedAtMs = Date.now();
              const fn = (methodType === 'stream' ? model.doStream : model.doGenerate).bind(model);
              // Preserve the pre-observability retry contract: every provider
              // attempt receives a fresh top-level request object. Some adapters
              // annotate their input synchronously, and reusing one object here
              // would let a failed attempt contaminate the next retry.
              const providerRequest = {
                ...toolsAndToolChoice,
                prompt,
                providerOptions: providerOptionsToUse,
                abortSignal: callAbortSignal,
                includeRawChunks,
                responseFormat: providerResponseFormat,
                ...filteredModelSettings,
                headers,
              };
              let requestMeasurement: PreparedRequestMeasurement | undefined;
              if (onPreparedRequest) {
                try {
                  // Detach all measurement inputs before the adapter takes
                  // ownership of providerRequest. Adapters may mutate it
                  // synchronously or retain it beyond this invocation.
                  requestMeasurement = preparedRequestMeasurement({
                    providerRequest,
                    providerOptions: providerOptionsToUse,
                    prompt,
                    responseSchema: responseSchemaForMeasurement,
                    responseSchemaInline,
                  });
                } catch {
                  // Measurement must never prevent or delay a provider failure.
                }
              }
              try {
                onProviderAttemptStart?.(providerAttempt);
              } catch {
                // Tracing must never prevent the provider call.
              }
              try {
                // Fork PF-5349: the ceiling is the set of tools the request let the model call.
                // Upstream #26232 keeps the definitions under `toolChoice: 'none'`, but none of
                // them is callable, so such a request bounds a later recovery step to no tools.
                onProviderDispatch?.(
                  (toolsAndToolChoice.toolChoice as { type?: unknown } | undefined)?.type === 'none'
                    ? []
                    : (toolsAndToolChoice.tools ?? []).map(tool => preparedToolKey(tool) ?? tool.name),
                );
              } catch {
                // Observers must never prevent the provider call.
              }
              // Cast needed: V2 and V3 call options are structurally compatible but typed differently
              // (e.g., tool types differ: V2 uses 'provider-defined', V3 uses 'provider')
              const providerDispatchTimestampMs = Date.now();
              try {
                let streamResultPromise: unknown;
                try {
                  // Invoke the adapter before notifying observers so they cannot add
                  // to provider dispatch latency. Post-dispatch reporting consumes
                  // only the detached, content-free measurement captured before the
                  // adapter received the live request object.
                  streamResultPromise = (fn as Function)(providerRequest);
                } finally {
                  if (onPreparedRequest && requestMeasurement) {
                    try {
                      onPreparedRequest({
                        ...requestMeasurement,
                        providerAttempt,
                        providerDispatchTimestampMs,
                        // Each retry receives a fresh request object, but only the
                        // first attempt owns shared prompt/tool preparation. Every
                        // attempt reports its own request construction and detached
                        // measurement work without including p-retry backoff.
                        providerPreparationMs:
                          Math.max(0, providerDispatchTimestampMs - attemptPreparationStartedAtMs) +
                          (attemptNumber === 1
                            ? preparationBeforeProviderCallbackMs +
                              Math.max(0, attemptPreparationStartedAtMs - preparationResumedAtMs)
                            : 0),
                      });
                    } catch {
                      // Measurement and observers must never prevent the provider call.
                    }
                  }
                }
                // Raced rather than merely signalled: a provider that ignores `abortSignal`
                // would otherwise hang straight past its budget. The race is armed on the
                // already-dispatched promise, in the same synchronous turn as the dispatch,
                // so the budget still bounds the provider call while
                // `providerDispatchTimestampMs` stays the exact provider request boundary.
                // Promise.resolve keeps the pre-race tolerance for an adapter that returns a
                // non-thenable; it is identity for a native promise, so it costs no tick.
                const streamResult = await raceAgainstAbort(Promise.resolve(streamResultPromise), callAbortSignal);

                // We have to cast this because doStream is missing the warnings property in its return type even though it exists
                return streamResult as unknown as LanguageModelV2StreamResult;
              } catch (error) {
                // `abortSignal` is now the composed step budget, not the caller's signal, so
                // `aborted` alone would relabel every timeout as a clean cancellation. A
                // Mastra timeout is a failure (see MastraTimeoutError in loop/timeout.ts) and
                // must stay on the inference-error boundary. The signal reason is checked
                // first because our own budget sets it, which settles the classification even
                // when the provider's AbortError wins the race against the budget rejection.
                let timedOut = false;
                try {
                  // `instanceof` walks the value's prototype chain, so it is guarded exactly
                  // like isAbortError below.
                  timedOut = isMastraTimeoutError(callAbortSignal?.reason) || isMastraTimeoutError(error);
                } catch {
                  // A hostile value must not stop the attempt boundary from being reported.
                }
                let aborted = !timedOut && callAbortSignal?.aborted === true;
                if (!aborted && !timedOut) {
                  try {
                    aborted = isAbortError(error);
                  } catch {
                    // Retry/error behavior remains unchanged for hostile thrown values.
                  }
                }
                try {
                  onProviderAttemptError?.({ error, providerAttempt, aborted });
                } catch {
                  // Tracing must never change retry or terminal error behavior.
                }
                cleanupFirstChunkTimeout();
                throw error;
              }
            },
            {
              retries: modelSettings?.maxRetries ?? 2,
              signal: abortSignal,
              // Pinned to p-retry's own defaults so the backoff it schedules stays
              // unchanged while remaining computable in onFailedAttempt below.
              minTimeout: RETRY_MIN_TIMEOUT_MS,
              factor: RETRY_BACKOFF_FACTOR,
              async onFailedAttempt(context) {
                // Runs before shouldRetry, so bail on anything that will not be retried:
                // a terminal error must not wait out a provider delay it will never use.
                if (context.retriesLeft <= 0 || !isRetryableModelError(context.error)) return;

                const retryAfterMs = getRetryAfterMs(context.error);
                if (retryAfterMs === undefined) return;

                // p-retry applies its own exponential delay after this hook resolves, so
                // wait only the remainder. Total wait lands on
                // max(backoff, min(Retry-After, cap)), the same rule
                // StreamErrorRetryProcessor applies, keeping a hostile or very large
                // Retry-After from wedging the run.
                const boundedRetryAfterMs = Math.min(retryAfterMs, DEFAULT_MAX_RETRY_AFTER_MS);
                const scheduledBackoffMs = RETRY_MIN_TIMEOUT_MS * RETRY_BACKOFF_FACTOR ** (context.attemptNumber - 1);
                await waitDelay(boundedRetryAfterMs - scheduledBackoffMs, abortSignal);
              },
              shouldRetry(context) {
                return isRetryableModelError(context.error);
              },
            },
          )
          .catch(error => {
            cleanupFirstChunkTimeout();
            cleanupStepTimeout();
            throw error;
          });

        if (!retryResult?.stream) {
          cleanupFirstChunkTimeout();
          cleanupStepTimeout();
          return retryResult;
        }

        const firstChunkGuardedStream = guardStreamUntilMatch(
          retryResult.stream,
          callAbortSignal,
          isContentChunk,
          cleanupFirstChunkTimeout,
        );
        const guardedResult = {
          ...retryResult,
          stream: guardStreamWithAbort(firstChunkGuardedStream, abortSignal, cleanupStepTimeout),
        } as unknown as LanguageModelV2StreamResult;
        // The router attaches its stream transport as a non-enumerable symbol,
        // which object spread drops. Re-attach it so transport handles survive
        // the step-timeout wrapper.
        attachModelStreamTransport(guardedResult, readModelStreamTransport(retryResult));
        return guardedResult;
      } catch (error) {
        if (shouldThrowError) {
          throw error;
        }

        return {
          stream: new ReadableStream({
            start: async controller => {
              controller.enqueue({
                type: 'error',
                error,
              });
              controller.close();
            },
          }),
          warnings: [],
          request: {},
          rawResponse: {},
        };
      }
    },
  });

  return stream;
}
