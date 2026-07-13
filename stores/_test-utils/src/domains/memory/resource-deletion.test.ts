import { MockStore } from '@mastra/core/storage';
import { createResourceDeletionTest } from './resource-deletion';

createResourceDeletionTest({ storage: new MockStore() });
