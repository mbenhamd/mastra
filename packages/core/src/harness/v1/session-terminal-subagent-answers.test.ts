import { describe, expect, it } from 'vitest';

import { TerminalSubagentAnswers } from './session-terminal-subagent-answers';
import type { HarnessSubagentDirectAnswer } from './terminal-subagent-result';

const answer = (text: string) => ({ text }) as unknown as HarnessSubagentDirectAnswer;

describe('TerminalSubagentAnswers', () => {
  it('hands out a single candidate once, for its own run only', () => {
    const answers = new TerminalSubagentAnswers();
    const a = answer('a');
    answers.offer('run-1', 'part-1', a);
    expect(answers.take('run-2')).toBeUndefined();
    expect(answers.take('run-1')).toEqual({ partId: 'part-1', answer: a });
    expect(answers.take('run-1')).toBeUndefined();
  });

  it('replaces the answer when the same part id is offered again', () => {
    const answers = new TerminalSubagentAnswers();
    const later = answer('later');
    answers.offer('run-1', 'part-1', answer('earlier'));
    answers.offer('run-1', 'part-1', later);
    expect(answers.take('run-1')).toEqual({ partId: 'part-1', answer: later });
  });

  it('removes the candidate when another part id is offered for the same run, and take then returns nothing', () => {
    const answers = new TerminalSubagentAnswers();
    answers.offer('run-1', 'part-1', answer('first'));
    answers.offer('run-1', 'part-2', answer('second'));
    expect(answers.take('run-1')).toBeUndefined();
  });

  it('does not close a run to candidates after a conflict: the next offer is stored', () => {
    const answers = new TerminalSubagentAnswers();
    const third = answer('third');
    answers.offer('run-1', 'part-1', answer('first'));
    answers.offer('run-1', 'part-2', answer('second'));
    answers.offer('run-1', 'part-3', third);
    expect(answers.take('run-1')).toEqual({ partId: 'part-3', answer: third });
  });

  it('keeps runs apart: a conflict or a drop on one run leaves another run untouched', () => {
    const answers = new TerminalSubagentAnswers();
    const b = answer('b');
    answers.offer('run-1', 'part-1', answer('a'));
    answers.offer('run-2', 'part-9', b);
    answers.offer('run-1', 'part-2', answer('conflict'));
    answers.drop('run-3');
    expect(answers.take('run-2')).toEqual({ partId: 'part-9', answer: b });
    answers.offer('run-2', 'part-9', b);
    answers.drop('run-2');
    expect(answers.take('run-2')).toBeUndefined();
  });
});
