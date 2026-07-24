import { describe, expect, it } from 'vitest';

import { buildObservationIndexInput } from '../observation-index-input';

describe('buildObservationIndexInput', () => {
  it('rejects groups without an authorizing observational-memory record', () => {
    expect(
      buildObservationIndexInput({
        group: {
          id: 'group-1',
          range: 'message-1:message-2',
          content: 'Date: 2026-07-23\nObservation',
          provenance: {},
        },
        threadId: 'thread-1',
        resourceId: 'resource-1',
      }),
    ).toBeUndefined();
  });

  it('forwards the exact record and scope used to authorize the index row', () => {
    expect(
      buildObservationIndexInput({
        group: {
          id: 'group-1',
          range: 'message-1:message-2',
          content: 'Date: 2026-07-23\nObservation',
          provenance: { recordId: 'record-1' },
        },
        threadId: undefined,
        resourceId: 'resource-1',
      }),
    ).toEqual({
      text: 'Date: 2026-07-23\nObservation',
      groupId: 'group-1',
      range: 'message-1:message-2',
      recordId: 'record-1',
      threadId: '',
      resourceId: 'resource-1',
    });
  });
});
