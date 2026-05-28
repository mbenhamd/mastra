### 12.9 Sandbox command policy

```ts
import { LocalSandbox } from '@mastra/core/workspace';

// Proposed v1 sandbox-provider configuration. Current core LocalSandbox must
// expose or be wrapped by this config before this example is copy-pasteable.
const sandbox = new LocalSandbox({
  commandPolicy: 'restricted',
  commands: {
    npm: { description: 'npm CLI' },
    gh: { description: 'GitHub CLI' },
    git: { description: 'Git CLI, available read-only' },
  },
});

// The portable v1 policy is a static command-start allowlist. Provider-specific
// programmable registries, custom executors, and per-command env injection are
// outside the v1 sandbox policy surface. This does not replace product-level
// filesystem/path grants such as MastraCode `/sandbox` and `request_access`;
// those remain workspace/session state and must be enforced by the filesystem
// or workspace adapter.
```
