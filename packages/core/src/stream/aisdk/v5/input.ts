import type { LanguageModelV2StreamPart } from '@ai-sdk/provider-v5';
import type { IdGenerator } from '@internal/ai-sdk-v5';
import { generateId as defaultGenerateId } from '@internal/ai-sdk-v5';
import { resolveResponseModelId } from '../../../llm/model/server-side-fallback';
import type { RegisteredLogger } from '../../../logger';
import { createExactJsonMeasurementSnapshot } from '../../../observability/content-free-measurement';
import { safeEnqueue, MastraModelInput } from '../../base';
import type { ChunkType } from '../../types';
import { convertFullStreamChunkToMastra } from './transform';
import type { StreamPart } from './transform';

type ProviderFinishPayload = Extract<ChunkType, { type: 'finish' }>['payload'];
type ProviderResponseIdentity = { responseId?: string; responseModel?: string };

function detachedJsonRecord(value: unknown): Record<string, unknown> | undefined {
  const measurement = createExactJsonMeasurementSnapshot(value);
  if (measurement.state !== 'measured') return undefined;
  try {
    const detached = JSON.parse(JSON.stringify(measurement.snapshot));
    return detached && typeof detached === 'object' && !Array.isArray(detached)
      ? (detached as Record<string, unknown>)
      : undefined;
  } catch {
    return undefined;
  }
}

function ownStringDataProperty(value: unknown, key: string): string | undefined {
  if (!value || typeof value !== 'object') return undefined;
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor && 'value' in descriptor && typeof descriptor.value === 'string' ? descriptor.value : undefined;
  } catch {
    return undefined;
  }
}

function providerFinishEvidence(payload: ProviderFinishPayload): ProviderFinishPayload {
  const usage = detachedJsonRecord(payload.output.usage) ?? {};
  const providerMetadata = detachedJsonRecord(payload.metadata?.providerMetadata ?? payload.providerMetadata);
  // Capture the finish-payload model here, while the payload is still pre-processor and so
  // still trustworthy. Providers that never emit a `response-metadata` chunk report the served
  // model only on this payload (upstream #21154), and tracing reads it as producer identity.
  // Downstream the same field can be written by an output processor, so a value recovered later
  // cannot be attributed to the producer - which is why the tracing merge refuses the step-side
  // `modelId`. Reading it now is what keeps the legitimate case working.
  const finishModelId = ownStringDataProperty(payload.metadata, 'modelId');
  return {
    stepResult: {
      reason: typeof payload.stepResult.reason === 'string' ? payload.stepResult.reason : 'unknown',
    },
    output: { usage },
    metadata: {
      ...(providerMetadata ? { providerMetadata } : {}),
      ...(finishModelId === undefined ? {} : { modelId: finishModelId }),
    },
  } as ProviderFinishPayload;
}

/**
 * Checks if an ID is a simple numeric string (e.g., "0", "1", "2").
 * Anthropic and Google providers use these indices which reset per LLM call,
 * while OpenAI uses UUIDs that are already unique.
 */
function isNumericId(id: string): boolean {
  return /^\d+$/.test(id);
}

export class AISDKV5InputStream extends MastraModelInput {
  #generateId: IdGenerator;
  #onProviderFirstContent?: () => void;
  #onProviderChunk?: (chunk: ChunkType, observedAt: Date) => void;
  #onProviderFinish?: (payload: ProviderFinishPayload, endTime: Date, response: ProviderResponseIdentity) => void;
  #providerContentObserved = false;
  #providerResponseId?: string;
  #providerResponseModel?: string;

  constructor({
    component,
    name,
    generateId,
    onProviderFirstContent,
    onProviderChunk,
    onProviderFinish,
  }: {
    component: RegisteredLogger;
    name: string;
    generateId?: IdGenerator;
    onProviderFirstContent?: () => void;
    onProviderChunk?: (chunk: ChunkType, observedAt: Date) => void;
    onProviderFinish?: (payload: ProviderFinishPayload, endTime: Date, response: ProviderResponseIdentity) => void;
  }) {
    super({ component, name });
    this.#generateId = generateId ?? defaultGenerateId;
    this.#onProviderFirstContent = onProviderFirstContent;
    this.#onProviderChunk = onProviderChunk;
    this.#onProviderFinish = onProviderFinish;
  }

  async transform({
    runId,
    stream,
    controller,
  }: {
    runId: string;
    stream: ReadableStream<LanguageModelV2StreamPart>;
    controller: ReadableStreamDefaultController<ChunkType>;
  }) {
    // Map numeric IDs to unique IDs for uniqueness across steps.
    // Workaround for @ai-sdk/anthropic and @ai-sdk/google duplicate IDs bug:
    // These providers use numeric indices ("0", "1", etc.) that reset per LLM call.
    // See: https://github.com/mastra-ai/mastra/issues/9909
    const idMap = new Map<string, string>();

    for await (const chunk of stream) {
      const rawChunk = chunk as StreamPart;
      const providerObservedAt = new Date();

      // Clear ID map on new step so each step gets fresh UUIDs
      if ((rawChunk as { type: string }).type === 'stream-start') {
        idMap.clear();
        this.#providerResponseId = undefined;
        this.#providerResponseModel = undefined;
      }

      const transformedChunk = convertFullStreamChunkToMastra(rawChunk, { runId });

      if (transformedChunk) {
        try {
          this.#onProviderChunk?.(transformedChunk, new Date(providerObservedAt.getTime()));
        } catch {
          // Observability must never affect provider stream conversion.
        }
        if (transformedChunk.type === 'response-metadata') {
          const { id, modelId } = transformedChunk.payload;
          this.#providerResponseId = typeof id === 'string' ? id : undefined;
          this.#providerResponseModel = typeof modelId === 'string' ? modelId : undefined;
        }
        if (transformedChunk.type === 'finish') {
          try {
            const finishPayload = providerFinishEvidence(transformedChunk.payload);
            this.#onProviderFinish?.(finishPayload, new Date(providerObservedAt.getTime()), {
              responseId: this.#providerResponseId,
              responseModel: resolveResponseModelId(
                finishPayload.metadata?.providerMetadata,
                this.#providerResponseModel,
              ),
            });
          } catch {
            // Observability must never affect provider stream conversion.
          }
        }
        if (
          !this.#providerContentObserved &&
          (transformedChunk.type === 'text-delta' ||
            transformedChunk.type === 'tool-call-delta' ||
            transformedChunk.type === 'tool-call' ||
            transformedChunk.type === 'reasoning-delta' ||
            transformedChunk.type === 'object' ||
            transformedChunk.type === 'object-result')
        ) {
          this.#providerContentObserved = true;
          try {
            this.#onProviderFirstContent?.();
          } catch {
            // Observability must never affect provider stream conversion.
          }
        }
        // Replace numeric IDs with unique IDs for text chunks
        if (
          (transformedChunk.type === 'text-start' ||
            transformedChunk.type === 'text-delta' ||
            transformedChunk.type === 'text-end') &&
          transformedChunk.payload?.id &&
          isNumericId(transformedChunk.payload.id)
        ) {
          const originalId = transformedChunk.payload.id;
          if (!idMap.has(originalId)) {
            idMap.set(originalId, this.#generateId());
          }
          transformedChunk.payload.id = idMap.get(originalId)!;
        }

        safeEnqueue(controller, transformedChunk);
      }
    }
  }
}
