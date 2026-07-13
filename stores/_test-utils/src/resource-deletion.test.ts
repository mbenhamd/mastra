import { MockStore } from '@mastra/core/storage';
import { createResourceDeletionTest } from './domains/memory/resource-deletion';

createResourceDeletionTest({ storage: new MockStore() });
