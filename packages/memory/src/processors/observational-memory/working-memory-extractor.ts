import { parseMemoryRequestContext } from '@mastra/core/memory';
import { z } from 'zod';

import { Extractor } from './extractor';
import type { ExtractorRuntimeContext } from './extractor';

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

/**
 * Strip null, empty-string, empty-array, and empty-object members recursively.
 * Schema-required-but-nullable fields make provider constrained decoding emit
 * every key; the stored document should only carry the keys with facts.
 */
function pruneEmptyDeep(value: unknown): unknown {
  if (value === null || value === undefined) return undefined;
  if (typeof value === 'string') return value.trim() ? value : undefined;
  if (Array.isArray(value)) {
    const pruned = value.map(pruneEmptyDeep).filter(entry => entry !== undefined);
    return pruned.length > 0 ? pruned : undefined;
  }
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .map(([key, entry]) => [key, pruneEmptyDeep(entry)] as const)
      .filter(([, entry]) => entry !== undefined);
    return entries.length > 0 ? Object.fromEntries(entries) : undefined;
  }
  return value;
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
        if (isZodLikeSchema(details.configuredSchema)) {
          return details.configuredSchema.nullable() as z.ZodType<Record<string, unknown> | null>;
        }
        return z.union([z.record(z.string(), z.unknown()), z.null()]);
      },
      onExtracted: async ({ current, memory, threadId, resourceId, requestContext, observationalMemoryRecordId }) => {
        const memoryConfig = parseMemoryRequestContext(requestContext)?.memoryConfig;
        const config = memory!.getMergedThreadConfig(memoryConfig ?? {});
        const isSchemaWorkingMemory = Boolean(config.workingMemory?.schema);

        if (isSchemaWorkingMemory && current === null) {
          return undefined;
        }

        let workingMemory: string;
        if (isSchemaWorkingMemory && typeof current === 'object') {
          // Required-but-nullable schema fields force constrained decoding to
          // emit every key; persist only the keys that carry facts, and never
          // overwrite the stored document with a factless one.
          const pruned = pruneEmptyDeep(current);
          if (pruned === undefined) {
            return undefined;
          }
          workingMemory = JSON.stringify(pruned);
        } else {
          workingMemory = typeof current === 'string' ? current : (JSON.stringify(current) ?? '');
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

        return current;
      },
    });
  }
}
