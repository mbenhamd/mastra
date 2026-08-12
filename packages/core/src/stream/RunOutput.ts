import EventEmitter from 'node:events';
import { ReadableStream, WritableStream } from 'node:stream/web';
import type { ReadableStreamGetReaderOptions, ReadableWritablePair, StreamPipeOptions } from 'node:stream/web';
import type { LanguageModelUsage } from '@internal/ai-sdk-v5';
import type { WorkflowResult, WorkflowRunStatus } from '../workflows';
import { DelayedPromise } from './aisdk/v5/compat';
import type { MastraBaseStream } from './base/base';
import { consumeStream } from './base/consume-stream';
import { ChunkFrom } from './types';
import type { StepTripwireData, WorkflowStreamEvent } from './types';

type AggregatedLanguageModelUsage = Required<LanguageModelUsage> & {
  cacheCreationInputTokens: number;
};

function createUsageCount(): AggregatedLanguageModelUsage {
  return {
    inputTokens: 0,
    outputTokens: 0,
    totalTokens: 0,
    cachedInputTokens: 0,
    cacheCreationInputTokens: 0,
    reasoningTokens: 0,
  };
}

function createDelayedPromises<TResult>() {
  return {
    usage: new DelayedPromise<LanguageModelUsage>(),
    result: new DelayedPromise<TResult>(),
  };
}

function getErrorMessage(error: unknown): string | undefined {
  if (error instanceof Error) return error.message;
  if (typeof error !== 'object' || error === null) return undefined;
  const message = (error as { message?: unknown }).message;
  return typeof message === 'string' ? message : undefined;
}

export class WorkflowRunOutput<
  TResult extends WorkflowResult<any, any, any, any> = WorkflowResult<any, any, any, any>,
> implements MastraBaseStream<WorkflowStreamEvent> {
  #status: WorkflowRunStatus = 'running';
  #tripwireData: StepTripwireData | undefined;
  #usageCount = createUsageCount();
  #consumptionStarted = false;
  #baseStream!: ReadableStream<WorkflowStreamEvent>;
  #emitter = new EventEmitter();
  #bufferedChunks: WorkflowStreamEvent[] = [];

  #streamFinished = false;
  #terminalDelivered = false;
  #streamGeneration = 0;

  #streamError: Error | undefined;

  #finalWorkflowResult: unknown;

  #delayedPromises = createDelayedPromises<TResult>();

  /**
   * Unique identifier for this workflow run
   */
  public runId: string;
  /**
   * Unique identifier for this workflow
   */
  public workflowId: string;

  constructor({
    runId,
    workflowId,
    stream,
  }: {
    runId: string;
    workflowId: string;
    stream: ReadableStream<WorkflowStreamEvent>;
  }) {
    this.runId = runId;
    this.workflowId = workflowId;
    this.#attachStream(stream);
  }

  #attachStream(stream: ReadableStream<WorkflowStreamEvent>) {
    const generation = ++this.#streamGeneration;

    this.#baseStream = stream;
    this.#streamFinished = false;
    this.#consumptionStarted = false;
    this.#terminalDelivered = false;
    this.#status = 'running';
    this.#tripwireData = undefined;
    this.#usageCount = createUsageCount();
    this.#streamError = undefined;
    this.#bufferedChunks = [];
    this.#emitter = new EventEmitter();
    this.#delayedPromises = createDelayedPromises<TResult>();

    stream
      .pipeTo(
        new WritableStream({
          start: () => {
            if (generation !== this.#streamGeneration) return;

            this.#emitChunk({
              type: 'workflow-start',
              runId: this.runId,
              from: ChunkFrom.WORKFLOW,
              payload: {
                workflowId: this.workflowId,
              },
            } as WorkflowStreamEvent);
          },
          write: chunk => {
            if (generation !== this.#streamGeneration) return;
            this.#acceptChunk(chunk);
          },
          close: () => {
            this.#finishStream(generation);
          },
        }),
      )
      .catch(reason => {
        const error = reason instanceof Error ? reason : new Error(String(reason));
        this.#finishStream(generation, error);
      });
  }

  #emitChunk(chunk: WorkflowStreamEvent) {
    this.#bufferedChunks.push(chunk);
    this.#emitter.emit('chunk', chunk);
  }

  #acceptChunk(chunk: WorkflowStreamEvent) {
    if (this.#terminalDelivered) return;

    if (chunk.type === 'workflow-finish') {
      // Some workflow engines publish a minimal `{ runId }` finish marker to
      // end their watch transport. It is not the public terminal payload and
      // must not suppress the canonical terminal synthesized on stream close.
      const terminal = this.#normalizeTerminal(chunk);
      if (!terminal) return;

      this.#terminalDelivered = true;
      this.#status = terminal.payload.workflowStatus;
      this.#emitChunk(terminal);
      return;
    }

    if (chunk.type !== 'workflow-step-finish') {
      this.#emitChunk(chunk);
    }

    if (chunk.type === 'workflow-step-output') {
      if ('output' in chunk.payload && chunk.payload) {
        const output = chunk.payload.output;
        if (output?.type === 'finish') {
          if (output.payload && 'usage' in output.payload && output.payload.usage) {
            this.#updateUsageCount(output.payload.usage);
          } else if (output.payload && 'output' in output.payload && output.payload.output) {
            const outputPayload = output.payload.output;
            if ('usage' in outputPayload && outputPayload.usage) {
              this.#updateUsageCount(outputPayload.usage);
            }
          }
        }
      }
    } else if (chunk.type === 'workflow-canceled') {
      this.#status = 'canceled';
    } else if (chunk.type === 'workflow-step-suspended') {
      this.#status = 'suspended';
    } else if (chunk.type === 'workflow-step-result' && chunk.payload.status === 'failed') {
      if (chunk.payload.tripwire) {
        this.#status = 'tripwire';
        this.#tripwireData = chunk.payload.tripwire;
      } else {
        this.#status = 'failed';
      }
    } else if (chunk.type === 'workflow-paused') {
      this.#status = 'paused';
    }
  }

  #normalizeTerminal(chunk: Extract<WorkflowStreamEvent, { type: 'workflow-finish' }>) {
    const payload = chunk.payload as unknown as Record<string, unknown>;
    const status = payload.workflowStatus ?? payload.status;
    if (typeof status !== 'string') return undefined;

    const error = payload.error;
    const metadata =
      typeof payload.metadata === 'object' && payload.metadata !== null && !Array.isArray(payload.metadata)
        ? { ...(payload.metadata as Record<string, unknown>) }
        : {};
    if (error !== undefined) {
      metadata.error ??= error;
      metadata.errorMessage ??= getErrorMessage(error);
    }

    const output =
      typeof payload.output === 'object' && payload.output !== null && !Array.isArray(payload.output)
        ? (payload.output as Record<string, unknown>)
        : {};
    const terminalUsage =
      typeof output.usage === 'object' && output.usage !== null && !Array.isArray(output.usage)
        ? output.usage
        : undefined;
    const hasObservedUsage =
      (this.#usageCount.inputTokens ?? 0) > 0 ||
      (this.#usageCount.outputTokens ?? 0) > 0 ||
      (this.#usageCount.totalTokens ?? 0) > 0;
    if (!hasObservedUsage && terminalUsage) {
      this.#updateUsageCount(
        terminalUsage as {
          inputTokens?: `${number}` | number;
          outputTokens?: `${number}` | number;
          totalTokens?: `${number}` | number;
          reasoningTokens?: `${number}` | number;
          cachedInputTokens?: `${number}` | number;
          cacheCreationInputTokens?: `${number}` | number;
        },
      );
    }
    const usage = this.#usageCount;

    return {
      ...chunk,
      payload: {
        ...payload,
        workflowStatus: status as WorkflowRunStatus,
        output: {
          ...output,
          usage,
        },
        metadata,
      },
    } as Extract<WorkflowStreamEvent, { type: 'workflow-finish' }>;
  }

  #finishStream(generation: number, error?: Error) {
    if (generation !== this.#streamGeneration || this.#streamFinished) return;

    if (error && !this.#terminalDelivered) {
      if (this.#delayedPromises.result.status.type === 'pending') {
        this.#status = 'failed';
        this.#streamError = error;
        this.#delayedPromises.result.reject(error);
      }
    } else if (this.#status === 'running') {
      this.#status = 'success';
    }

    if (!this.#terminalDelivered) {
      this.#terminalDelivered = true;
      this.#emitChunk({
        type: 'workflow-finish',
        runId: this.runId,
        from: ChunkFrom.WORKFLOW,
        payload: {
          workflowStatus: this.#status,
          metadata: this.#streamError
            ? {
                error: this.#streamError,
                errorMessage: this.#streamError.message,
              }
            : {},
          output: {
            usage: this.#usageCount,
          },
          ...(this.#status === 'success' && this.#finalWorkflowResult !== undefined
            ? { finalWorkflowResult: this.#finalWorkflowResult }
            : {}),
          ...(this.#status === 'tripwire' && this.#tripwireData ? { tripwire: this.#tripwireData } : {}),
        },
      } as WorkflowStreamEvent);
    }

    this.#delayedPromises.usage.resolve(this.#usageCount);

    Object.entries(this.#delayedPromises).forEach(([key, promise]) => {
      if (promise.status.type === 'pending') {
        promise.reject(new Error(`promise '${key}' was not resolved or rejected when stream finished`));
      }
    });

    this.#streamFinished = true;
    this.#emitter.emit('finish');
  }

  #getDelayedPromise<T>(promise: DelayedPromise<T>): Promise<T> {
    if (!this.#consumptionStarted) {
      void this.consumeStream();
    }
    return promise.promise;
  }

  #updateUsageCount(
    usage:
      | {
          inputTokens?: `${number}` | number;
          outputTokens?: `${number}` | number;
          totalTokens?: `${number}` | number;
          reasoningTokens?: `${number}` | number;
          cachedInputTokens?: `${number}` | number;
          cacheCreationInputTokens?: `${number}` | number;
        }
      | {
          promptTokens?: `${number}` | number;
          completionTokens?: `${number}` | number;
          totalTokens?: `${number}` | number;
          reasoningTokens?: `${number}` | number;
          cachedInputTokens?: `${number}` | number;
          cacheCreationInputTokens?: `${number}` | number;
        },
  ) {
    let totalUsage = {
      inputTokens: this.#usageCount.inputTokens ?? 0,
      outputTokens: this.#usageCount.outputTokens ?? 0,
      totalTokens: this.#usageCount.totalTokens ?? 0,
      reasoningTokens: this.#usageCount.reasoningTokens ?? 0,
      cachedInputTokens: this.#usageCount.cachedInputTokens ?? 0,
      cacheCreationInputTokens: this.#usageCount.cacheCreationInputTokens ?? 0,
    };
    if ('inputTokens' in usage) {
      totalUsage.inputTokens += parseInt(usage?.inputTokens?.toString() ?? '0', 10);
      totalUsage.outputTokens += parseInt(usage?.outputTokens?.toString() ?? '0', 10);
      // we need to handle both formats because you can use a V1 model inside a stream workflow
    } else if ('promptTokens' in usage) {
      totalUsage.inputTokens += parseInt(usage?.promptTokens?.toString() ?? '0', 10);
      totalUsage.outputTokens += parseInt(usage?.completionTokens?.toString() ?? '0', 10);
    }
    totalUsage.totalTokens += parseInt(usage?.totalTokens?.toString() ?? '0', 10);

    totalUsage.reasoningTokens += parseInt(usage?.reasoningTokens?.toString() ?? '0', 10);
    totalUsage.cachedInputTokens += parseInt(usage?.cachedInputTokens?.toString() ?? '0', 10);
    totalUsage.cacheCreationInputTokens += parseInt(usage?.cacheCreationInputTokens?.toString() ?? '0', 10);
    this.#usageCount = totalUsage;
  }

  /**
   * Finalize the run when the underlying stream pipeline rejects.
   *
   * When `pipeTo` rejects, the WritableStream's `close()` never runs, so without
   * this the terminal `workflow-finish` event would never fire, the delayed
   * `result`/`usage` promises would never settle, and every `fullStream` consumer
   * would hang forever. Mirror the `close()` path but mark the run as failed.
   */
  #finalizeWithError(reason: unknown) {
    // A clean close already finalized the run; nothing to do.
    if (this.#streamFinished) {
      return;
    }

    const error = reason instanceof Error ? reason : new Error(String(reason));
    this.#streamError = error;
    // The run terminated because the stream pipeline rejected, so it failed —
    // overwrite any earlier non-terminal status (paused/suspended/canceled/tripwire)
    // so downstream consumers always see a failed terminal status.
    this.#status = 'failed';

    // Emit a terminal finish so fullStream consumers stop waiting and close.
    this.#emitter.emit('chunk', {
      type: 'workflow-finish',
      runId: this.runId,
      from: ChunkFrom.WORKFLOW,
      payload: {
        workflowStatus: this.#status,
        metadata: {
          error: this.#streamError,
          errorMessage: this.#streamError.message,
        },
        output: {
          usage: this.#usageCount,
        },
      },
    });

    // Reject any still-pending delayed promises so result/usage callers see the
    // error instead of awaiting forever.
    Object.entries(this.#delayedPromises).forEach(([_key, promise]) => {
      if (promise.status.type === 'pending') {
        promise.reject(error);
      }
    });

    this.#streamFinished = true;
    this.#emitter.emit('finish');

    console.error('[WorkflowRunOutput] workflow stream pipeline error', error);
  }

  /**
   * @internal
   */
  updateResults(results: TResult) {
    if (results.status === 'success') {
      this.#finalWorkflowResult = results.result;
    }
    if (this.#delayedPromises.result.status.type === 'pending') {
      this.#delayedPromises.result.resolve(results);
      if (!this.#terminalDelivered) {
        this.#status = results.status;
      }
    }
  }

  /**
   * @internal
   */
  rejectResults(error: Error) {
    if (this.#delayedPromises.result.status.type === 'pending') {
      this.#delayedPromises.result.reject(error);
      this.#status = 'failed';
      this.#streamError = error;
    }
  }

  /**
   * @internal
   */
  resume(stream: ReadableStream<WorkflowStreamEvent>) {
    this.#attachStream(stream);
  }

  async consumeStream(options?: Parameters<typeof consumeStream>[0]): Promise<void> {
    if (this.#consumptionStarted) {
      return;
    }

    this.#consumptionStarted = true;

    try {
      await consumeStream({
        stream: this.#baseStream as globalThis.ReadableStream,
        onError: options?.onError,
      });
    } catch (error) {
      options?.onError?.(error);
    }
  }

  get fullStream(): ReadableStream<WorkflowStreamEvent> {
    const self = this;
    // Holds this subscriber's own detach function once start() registers its
    // listeners. cancel() must only remove this subscriber's handlers — the
    // emitter is shared across every concurrent fullStream/observe consumer of
    // the same run, so removeAllListeners() here would silently kill every
    // other subscriber (see #19743).
    let detach: (() => void) | undefined;
    return new ReadableStream<WorkflowStreamEvent>({
      start(controller) {
        // Replay existing buffered chunks
        self.#bufferedChunks.forEach(chunk => {
          controller.enqueue(chunk);
        });

        // If stream already finished, close immediately
        if (self.#streamFinished) {
          controller.close();
          return;
        }

        // Listen for new chunks and stream finish
        const chunkHandler = (chunk: WorkflowStreamEvent) => {
          controller.enqueue(chunk);
        };

        const finishHandler = () => {
          self.#emitter.off('chunk', chunkHandler);
          self.#emitter.off('finish', finishHandler);
          controller.close();
        };

        self.#emitter.on('chunk', chunkHandler);
        self.#emitter.on('finish', finishHandler);

        detach = () => {
          self.#emitter.off('chunk', chunkHandler);
          self.#emitter.off('finish', finishHandler);
        };
      },

      pull(_controller) {
        // Only start consumption when someone is actively reading the stream
        if (!self.#consumptionStarted) {
          void self.consumeStream();
        }
      },

      cancel() {
        // Only detach this subscriber's own listeners — never the whole emitter.
        detach?.();
      },
    });
  }

  get status() {
    return this.#status;
  }

  get result() {
    return this.#getDelayedPromise(this.#delayedPromises.result);
  }

  get usage() {
    return this.#getDelayedPromise(this.#delayedPromises.usage);
  }

  /**
   * @deprecated Use `fullStream.locked` instead
   */
  get locked(): boolean {
    console.warn('WorkflowRunOutput.locked is deprecated. Use fullStream.locked instead.');
    return this.fullStream.locked;
  }

  /**
   * @deprecated Use `fullStream.cancel()` instead
   */
  cancel(reason?: any): Promise<void> {
    console.warn('WorkflowRunOutput.cancel() is deprecated. Use fullStream.cancel() instead.');
    return this.fullStream.cancel(reason);
  }

  /**
   * @deprecated Use `fullStream.getReader()` instead
   */
  getReader(
    options?: ReadableStreamGetReaderOptions,
  ): ReadableStreamDefaultReader<WorkflowStreamEvent> | ReadableStreamBYOBReader {
    console.warn('WorkflowRunOutput.getReader() is deprecated. Use fullStream.getReader() instead.');
    return this.fullStream.getReader(options as any) as any;
  }

  /**
   * @deprecated Use `fullStream.pipeThrough()` instead
   */
  pipeThrough<T>(
    transform: ReadableWritablePair<T, WorkflowStreamEvent>,
    options?: StreamPipeOptions,
  ): ReadableStream<T> {
    console.warn('WorkflowRunOutput.pipeThrough() is deprecated. Use fullStream.pipeThrough() instead.');
    return this.fullStream.pipeThrough(transform as any, options) as ReadableStream<T>;
  }

  /**
   * @deprecated Use `fullStream.pipeTo()` instead
   */
  pipeTo(destination: WritableStream<WorkflowStreamEvent>, options?: StreamPipeOptions): Promise<void> {
    console.warn('WorkflowRunOutput.pipeTo() is deprecated. Use fullStream.pipeTo() instead.');
    return this.fullStream.pipeTo(destination, options);
  }

  /**
   * @deprecated Use `fullStream.tee()` instead
   */
  tee(): [ReadableStream<WorkflowStreamEvent>, ReadableStream<WorkflowStreamEvent>] {
    console.warn('WorkflowRunOutput.tee() is deprecated. Use fullStream.tee() instead.');
    return this.fullStream.tee();
  }

  /**
   * @deprecated Use `fullStream[Symbol.asyncIterator]()` instead
   */
  [Symbol.asyncIterator](): AsyncIterableIterator<WorkflowStreamEvent> {
    console.warn(
      'WorkflowRunOutput[Symbol.asyncIterator]() is deprecated. Use fullStream[Symbol.asyncIterator]() instead.',
    );
    return this.fullStream[Symbol.asyncIterator]();
  }

  /**
   * Helper method to treat this object as a ReadableStream
   * @deprecated Use `fullStream` directly instead
   */
  toReadableStream(): ReadableStream<WorkflowStreamEvent> {
    console.warn('WorkflowRunOutput.toReadableStream() is deprecated. Use fullStream directly instead.');
    return this.fullStream;
  }
}
