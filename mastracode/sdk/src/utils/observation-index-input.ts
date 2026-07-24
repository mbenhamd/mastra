export type ObservationIndexCandidate = {
  id: string;
  range: string;
  content: string;
  provenance: {
    recordId?: string;
  };
};

export type AuthorizedObservationIndexInput = {
  text: string;
  groupId: string;
  range: string;
  recordId: string;
  threadId: string;
  resourceId: string;
};

export function buildObservationIndexInput(options: {
  group: ObservationIndexCandidate;
  threadId: string | undefined;
  resourceId: string;
}): AuthorizedObservationIndexInput | undefined {
  const recordId = options.group.provenance.recordId;
  if (!recordId) {
    return undefined;
  }

  return {
    text: options.group.content,
    groupId: options.group.id,
    range: options.group.range,
    recordId,
    threadId: options.threadId ?? '',
    resourceId: options.resourceId,
  };
}
