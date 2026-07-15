import { afterEach } from 'vitest';
import { clearGlobalRunRegistry } from './run-registry';

// prepare() intentionally retains a one-time runtime capability until it is
// consumed or explicitly released. Tests use deterministic UUIDs, so clear
// abandoned preparations between isolated cases.
afterEach(() => {
  clearGlobalRunRegistry();
});
