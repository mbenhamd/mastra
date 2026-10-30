import { describe, expectTypeOf, it } from 'vitest';
import type { HarnessStorage } from './base';
import type {
  HarnessTerminalGrantRevocationInput,
  HarnessTerminalGrantRevocationReceipt,
  HarnessTerminalGuardedGrantRevocationReceipt,
} from './terminal-handoff';

declare const storage: HarnessStorage;
declare const input: Omit<HarnessTerminalGrantRevocationInput, 'requireOwnerLeaseLapsed'>;
declare const maybeGuarded: HarnessTerminalGrantRevocationInput;

describe('revokeTerminalGrant receipt types', () => {
  it('answers owner_live only to a revocation that may pass requireOwnerLeaseLapsed', () => {
    expectTypeOf(storage.revokeTerminalGrant(input)).resolves.toEqualTypeOf<HarnessTerminalGrantRevocationReceipt>();
    expectTypeOf(storage.revokeTerminalGrant(input)).resolves.not.toHaveProperty('leaseExpiresAt');
    expectTypeOf(
      storage.revokeTerminalGrant({ ...input, requireOwnerLeaseLapsed: { ownerId: 'owner-1' } }),
    ).resolves.toEqualTypeOf<HarnessTerminalGuardedGrantRevocationReceipt>();
    expectTypeOf(
      storage.revokeTerminalGrant(maybeGuarded),
    ).resolves.toEqualTypeOf<HarnessTerminalGuardedGrantRevocationReceipt>();
    expectTypeOf<HarnessTerminalGrantRevocationReceipt['status']>().toEqualTypeOf<
      'admitted' | 'revoked' | 'duplicate'
    >();
    expectTypeOf<HarnessTerminalGuardedGrantRevocationReceipt['status']>().toEqualTypeOf<
      'admitted' | 'revoked' | 'duplicate' | 'owner_live'
    >();
  });
});
