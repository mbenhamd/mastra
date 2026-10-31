import { expect, it } from 'vitest';
import type { ChunkType } from '../../../../stream';
import type { Processor } from '../../../../processors';
import { BatchPartsProcessor, StreamErrorRetryProcessor } from '../../../../processors';
import { describeForAllEngines, runLoopScenario, useLoopScenarioAimock } from '../aimock-scenario';

/**
 * Story: a guard processor holds the reply with an inner BatchPartsProcessor that
 * shares the guard's state, and publishes only what its own check approves when the
 * turn finishes. Releasing held parts at the end of the model stream must not hand
 * the inner batch out past the guard's check; discarding a failed stream must still
 * clear it, so a retried call's reply carries nothing stale.
 */
describeForAllEngines('Held parts of a processor composed inside another processor', engine => {
  const getMock = useLoopScenarioAimock();
  const APPROVED = '[ok] ';

  const streamedText = (chunks: ChunkType[] | undefined) =>
    (chunks ?? [])
      .filter(c => c.type === 'text-delta')
      .map(c => (c.payload as { text: string }).text)
      .join('');

  /** Holds text in an inner batch; at the turn's finish, publishes it once, marked, if `approve` allows it. */
  const holdingGuard = (approve: (text: string) => boolean): Processor => {
    const inner = new BatchPartsProcessor({ batchSize: Number.MAX_SAFE_INTEGER, emitOnNonText: true });
    return {
      id: 'holding-guard',
      processOutputStream: async args => {
        if (args.part.type !== 'text-delta' && args.part.type !== 'finish') return args.part;
        const part = await inner.processOutputStream(args);
        if (part === null || part.type !== 'text-delta') return part;
        if (!approve(part.payload.text)) return null;
        return { ...part, payload: { ...part.payload, text: APPROVED + part.payload.text } };
      },
    };
  };

  it('the end-of-stream release does not bypass the composing processor', async () => {
    const { output, chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Answer',
      collectChunks: true,
      outputProcessors: [holdingGuard(() => false)],
      fixtures: llm => {
        llm.on({ endpoint: 'chat' }, { content: 'unverified claim the guard never approves' }, { chunkSize: 5 });
      },
    });

    expect(streamedText(chunks)).toBe('');
    expect(await output.text).toBe('');
  });

  it('the end-of-stream release does not bypass a subclass that overrides processOutputStream', async () => {
    class BlockingBatch extends BatchPartsProcessor {
      constructor() {
        super({ batchSize: Number.MAX_SAFE_INTEGER, emitOnNonText: true });
      }
      // Holds text in the inherited batch and never publishes it.
      override async processOutputStream(args: Parameters<BatchPartsProcessor['processOutputStream']>[0]) {
        if (args.part.type !== 'text-delta') return args.part;
        await super.processOutputStream(args);
        return null;
      }
    }
    const { output, chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Answer',
      collectChunks: true,
      outputProcessors: [new BlockingBatch()],
      fixtures: llm => {
        llm.on({ endpoint: 'chat' }, { content: 'text the subclass blocks' }, { chunkSize: 5 });
      },
    });

    expect(streamedText(chunks)).toBe('');
    expect(await output.text).toBe('');
  });

  it('text the composing processor approves at finish is published exactly once', async () => {
    const REPLY = 'a claim the guard checks and approves';
    const { output, chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Answer',
      collectChunks: true,
      outputProcessors: [holdingGuard(text => text === REPLY)],
      fixtures: llm => {
        llm.on({ endpoint: 'chat' }, { content: REPLY }, { chunkSize: 5 });
      },
    });

    expect(streamedText(chunks), 'held text was released unchecked or published twice').toBe(APPROVED + REPLY);
    expect(await output.text).toBe(APPROVED + REPLY);
  });

  it('a discarded attempt leaves nothing stale in the retried reply', async () => {
    const { output, chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Answer',
      collectChunks: true,
      errorProcessors: [new StreamErrorRetryProcessor({ maxRetries: 2 })],
      outputProcessors: [holdingGuard(() => true)],
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          { content: 'Stale partial answer that gets cut off' },
          { chunkSize: 5, latency: 20, disconnectAfterMs: 150 },
        );
        llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: 'Fresh answer.' });
      },
    });

    expect(streamedText(chunks), 'the dropped attempt text reached the retried reply').toBe(APPROVED + 'Fresh answer.');
    expect(await output.text).toBe(APPROVED + 'Fresh answer.');
  });
});
