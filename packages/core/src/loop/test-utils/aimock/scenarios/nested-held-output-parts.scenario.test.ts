import { expect, it } from 'vitest';
import type { ChunkType } from '../../../../stream';
import type { Processor } from '../../../../processors';
import { BatchPartsProcessor } from '../../../../processors';
import { describeForAllEngines, runLoopScenario, useLoopScenarioAimock } from '../aimock-scenario';

/**
 * Story: a guard processor holds the reply with an inner BatchPartsProcessor that
 * shares the guard's state, and publishes only what its own check approves. Releasing
 * held parts at the end of the model stream must not hand the inner batch out past the
 * guard's check.
 */
describeForAllEngines('Held parts of a processor composed inside another processor', engine => {
  const getMock = useLoopScenarioAimock();
  const REPLY = 'unverified claim the guard never approves';

  it('the end-of-stream release does not bypass the composing processor', async () => {
    const inner = new BatchPartsProcessor({ batchSize: Number.MAX_SAFE_INTEGER, emitOnNonText: true });
    const guard: Processor = {
      id: 'withholding-guard',
      processOutputStream: async args => {
        if (args.part.type !== 'text-delta') return args.part;
        await inner.processOutputStream(args);
        return null;
      },
    };
    const { output, chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Answer',
      collectChunks: true,
      outputProcessors: [guard],
      fixtures: llm => {
        llm.on({ endpoint: 'chat' }, { content: REPLY }, { chunkSize: 5 });
      },
    });

    const streamed = (chunks ?? [])
      .filter((c: ChunkType) => c.type === 'text-delta')
      .map(c => (c.payload as { text: string }).text)
      .join('');
    expect(streamed).toBe('');
    expect(await output.text).toBe('');
  });
});
