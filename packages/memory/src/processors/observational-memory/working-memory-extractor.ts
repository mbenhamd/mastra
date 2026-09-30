import { parseMemoryRequestContext } from '@mastra/core/memory';
import { toStandardSchema } from '@mastra/core/schema';
import type { PublicSchema } from '@mastra/core/schema';
import { standardSchemaToJSONSchema } from '@mastra/schema-compat/schema';
import { z } from 'zod';

import { resolveNullableBranch, stripNullsFromOptional } from '../../tools/working-memory';
import { Extractor } from './extractor';
import type { ExtractorRuntimeContext } from './extractor';

/**
 * The structured-output schema for this extractor stays generic because every structured extractor shares one
 * response object: a strict working-memory schema there would make one invalid document fail every sibling
 * extractor. The configured schema is enforced here instead, with its own validator, before anything is stored.
 * Like the working memory tool, nulls in optional fields are treated as "not provided" rather than as invalid.
 */
async function validateAgainstConfiguredSchema(
  schema: PublicSchema,
  value: unknown,
): Promise<{ value: unknown; jsonSchema: Record<string, unknown> }> {
  const standardSchema = toStandardSchema(schema);
  const jsonSchema = standardSchemaToJSONSchema(standardSchema, { io: 'input' }) as Record<string, unknown>;
  const result = await standardSchema['~standard'].validate(stripNullsFromOptional(value, jsonSchema));
  if (!result.issues) {
    return { value: result.value, jsonSchema };
  }

  const details = result.issues
    .map(issue => {
      const path = issue.path?.map(segment => String(typeof segment === 'object' ? segment.key : segment)).join('.');
      return path ? `${path}: ${issue.message}` : issue.message;
    })
    .join('; ');
  throw new Error(`Working memory update does not match the configured schema, so it was not saved: ${details}`);
}

async function getWorkingMemoryDetails(context: ExtractorRuntimeContext): Promise<{
  template?: string;
  current?: string | null;
  usesSchema: boolean;
  configuredSchema?: unknown;
}> {
  const memory = context.memory!;
  const memoryConfig = parseMemoryRequestContext(context.requestContext)?.memoryConfig;
  const config = memory.getMergedThreadConfig(memoryConfig ?? {});
  const workingMemory = config.workingMemory;
  if (!workingMemory?.enabled) {
    return { usesSchema: false };
  }

  const [template, current] = await Promise.all([
    memory.getWorkingMemoryTemplate({ memoryConfig }),
    context.threadId
      ? memory.getWorkingMemory({
          threadId: context.threadId,
          resourceId: context.resourceId,
          memoryConfig,
        })
      : Promise.resolve(null),
  ]);

  return {
    template: typeof template?.content === 'string' ? template.content : JSON.stringify(template?.content),
    current,
    usesSchema: Boolean(workingMemory.schema),
    configuredSchema: workingMemory.schema,
  };
}

function isZodLikeSchema(value: unknown): value is z.ZodType<Record<string, unknown>> {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof (value as { safeParse?: unknown }).safeParse === 'function' &&
    typeof (value as { nullable?: unknown }).nullable === 'function'
  );
}

function isFactlessValue(value: unknown): boolean {
  return (
    value === null ||
    value === undefined ||
    (typeof value === 'string' && !value.trim()) ||
    (Array.isArray(value) && value.length === 0)
  );
}

/**
 * Drop factless members of schema-REQUIRED declared properties, recursively.
 * Required-but-nullable schema fields make provider constrained decoding emit
 * every key, so a null, blank string, or empty array there means "no fact";
 * the stored document should only carry the keys with facts. Optional
 * properties already had their nulls stripped before validation, and
 * undeclared (record) entries keep every value the schema allowed, including
 * null.
 */
function pruneFactlessRequired(value: unknown, rawSchema: Record<string, unknown>): unknown {
  const schema = resolveNullableBranch(value, rawSchema);

  if (Array.isArray(value)) {
    const itemSchema = (schema.items as Record<string, unknown>) ?? {};
    return value.map(item => pruneFactlessRequired(item, itemSchema));
  }

  if (typeof value !== 'object' || value === null) {
    return value;
  }

  const properties = (schema.properties as Record<string, Record<string, unknown>>) ?? {};
  const required = (schema.required as string[]) ?? [];
  const result: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (!Object.hasOwn(properties, key)) {
      result[key] = entry;
      continue;
    }
    const pruned = pruneFactlessRequired(entry, properties[key]!);
    if (required.includes(key) && isFactlessValue(pruned)) {
      continue;
    }
    result[key] = pruned;
  }
  return result;
}

function buildWorkingMemoryInstructions(details: Awaited<ReturnType<typeof getWorkingMemoryDetails>>): string {
  if (details.usesSchema) {
    return [
      'Update working memory with durable facts from the observations you made.',
      'Return the full updated JSON object when working memory should change.',
      'Fill every field for which the observations or the current working memory contain a durable fact; carry existing values forward unless contradicted. Use null or empty arrays only where nothing applies.',
      'Return null when no working memory update is needed.',
      details.template ? `Working memory JSON schema:\n${details.template}` : undefined,
      details.current ? `Current working memory JSON:\n${details.current}` : undefined,
    ]
      .filter(Boolean)
      .join('\n\n');
  }

  return [
    'Update working memory with durable facts from the observations you made.',
    'Return the full updated Markdown working memory. Preserve useful existing content and add or revise only what changed.',
    // Emission must be unconditional: optional sections get skipped often
    // enough by real observer models that durable facts are silently lost.
    // Re-emitting an unchanged document is an idempotent write.
    'You MUST always include this section in your output. If nothing durable changed, return the current working memory verbatim.',
    details.template ? `Working memory template:\n${details.template}` : undefined,
    details.current ? `Current working memory:\n${details.current}` : undefined,
  ]
    .filter(Boolean)
    .join('\n\n');
}

export class WorkingMemoryExtractor extends Extractor<string | Record<string, unknown> | null> {
  constructor() {
    super({
      name: 'Working Memory',
      includePreviousExtraction: false,
      metadataKeyPath: false,
      retryStructuredExtractionOnEmptyObject: true,
      instructions: async context => buildWorkingMemoryInstructions(await getWorkingMemoryDetails(context)),
      schema: async context => {
        const details = await getWorkingMemoryDetails(context);
        if (!details.usesSchema) {
          return undefined;
        }
        // Prefer the CONFIGURED working-memory schema over a generic record:
        // providers with schema-constrained decoding (Gemini structured
        // output) only emit properties the schema declares, so a
        // properties-less record schema decodes as {} and durable facts are
        // silently dropped. Null stays the no-update sentinel.
        // All structured extractors share one response object, so an invalid
        // working-memory document must not fail the shared parse and drop
        // sibling extractors: `catch` keeps the configured JSON Schema for
        // decoding but passes the raw document through, and `onExtracted`
        // enforces the configured schema before anything is stored.
        if (isZodLikeSchema(details.configuredSchema)) {
          // `ctx` is undefined only when zod probes the catch value for JSON Schema `default`;
          // returning undefined there keeps the emitted decoding schema free of a default.
          return details.configuredSchema
            .nullable()
            .catch(ctx => (ctx === undefined ? undefined : ctx.input) as Record<string, unknown> | null);
        }
        return z.union([z.record(z.string(), z.unknown()), z.null()]);
      },
      onExtracted: async ({ current, memory, threadId, resourceId, requestContext, observationalMemoryRecordId }) => {
        const memoryConfig = parseMemoryRequestContext(requestContext)?.memoryConfig;
        const config = memory!.getMergedThreadConfig(memoryConfig ?? {});
        const configuredSchema = config.workingMemory?.schema;

        let document: unknown = current;
        let configuredJsonSchema: Record<string, unknown> | undefined;
        if (configuredSchema) {
          if (current === null) {
            return undefined;
          }
          ({ value: document, jsonSchema: configuredJsonSchema } = await validateAgainstConfiguredSchema(
            configuredSchema,
            current,
          ));
        }

        let workingMemory: string;
        if (configuredJsonSchema && typeof document === 'object' && document !== null) {
          // Required-but-nullable schema fields force constrained decoding to
          // emit every key; persist only the keys that carry facts, and never
          // overwrite the stored document with a factless one.
          const pruned = pruneFactlessRequired(document, configuredJsonSchema);
          if (
            typeof pruned === 'object' &&
            pruned !== null &&
            !Array.isArray(pruned) &&
            Object.keys(pruned).length === 0
          ) {
            return undefined;
          }
          workingMemory = JSON.stringify(pruned);
        } else {
          workingMemory = typeof document === 'string' ? document : (JSON.stringify(document) ?? '');
        }
        if (!workingMemory.trim()) {
          return undefined;
        }

        await memory!.updateWorkingMemory({
          threadId,
          resourceId,
          workingMemory,
          memoryConfig,
          observationalMemoryRecordId,
        });

        return document as Record<string, unknown> | string;
      },
    });
  }
}
