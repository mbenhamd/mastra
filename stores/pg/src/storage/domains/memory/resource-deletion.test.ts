import { createResourceDeletionTest } from '@internal/storage-test-utils';
import { vi } from 'vitest';

import { PostgresStore } from '../..';
import { TEST_CONFIG } from '../../test-utils';

vi.setConfig({ testTimeout: 60_000, hookTimeout: 60_000 });

createResourceDeletionTest({ storage: new PostgresStore(TEST_CONFIG) });
