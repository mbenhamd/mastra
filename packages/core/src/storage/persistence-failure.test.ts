import { describe, expect, it } from 'vitest';
import { ErrorCategory, ErrorDomain, MastraError } from '../error';
import { getStoragePersistenceFailure } from './persistence-failure';

function storageError(persistenceFailure: unknown, cause?: unknown): MastraError {
  return new MastraError(
    {
      id: 'MASTRA_STORAGE_TEST_FAILED',
      domain: ErrorDomain.STORAGE,
      category: ErrorCategory.THIRD_PARTY,
      details: { persistenceFailure: persistenceFailure as string },
    },
    cause,
  );
}

describe('getStoragePersistenceFailure', () => {
  it('reads the classification through wrapping errors', () => {
    const wrapped = new MastraError(
      { id: 'WORKFLOW_TEST_FAILED', domain: ErrorDomain.MASTRA_WORKFLOW, category: ErrorCategory.SYSTEM },
      new Error('outer', { cause: storageError('commit_unknown') }),
    );

    expect(getStoragePersistenceFailure(wrapped)).toBe('commit_unknown');
  });

  it('ignores values outside the contract and errors without one', () => {
    expect(getStoragePersistenceFailure(storageError('retry_later'))).toBeUndefined();
    expect(getStoragePersistenceFailure(new Error('plain'))).toBeUndefined();
    expect(getStoragePersistenceFailure('transient')).toBeUndefined();
  });
});
