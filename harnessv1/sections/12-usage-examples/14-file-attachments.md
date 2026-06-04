### 12.14 File attachments

```ts
import fs from 'node:fs/promises';

const session = await harness.session({
  resourceId: 'local-user',
  threadId: { fresh: true },
});

// Inline form — the harness flushes bytes to the attachment store before
// queuing, so this survives a server restart.
const screenshot = await fs.readFile('./screenshot.png');
await session.queue({
  contents: 'What does this UI bug look like?',
  files: [
    { kind: 'inline', name: 'screenshot.png', mimeType: 'image/png', data: screenshot },
  ],
});

// URL form — for assets already hosted somewhere reachable. The URL is
// transient input; before durable admission the harness copies bytes into
// attachment storage and rewrites the operation to a ref.
await session.queue({
  contents: 'Compare this design to the current implementation',
  files: [
    { kind: 'url', name: 'figma-export.png', mimeType: 'image/png', url: 'https://cdn.example.com/asset/abc.png' },
  ],
});

// Pre-upload form — useful for browser drag-drop with progress UI.
const { attachmentId } = await session.uploadAttachment({
  name: 'logs.txt',
  mimeType: 'text/plain',
  data: largeBuffer,
  onProgress: (loaded, total) => console.log(`${(loaded / total * 100).toFixed(1)}%`),
});

await session.queue({
  contents: 'Find the root cause in these logs',
  files: [
    { kind: 'ref', name: 'logs.txt', mimeType: 'text/plain', attachmentId },
  ],
});

// Referenced attachments live until their durable references are gone or session
// delete cleanup removes them. Staged, unreferenced attachments are governed by
// the configured retention/cleanup policy. Close is not an attachment deletion
// boundary.
await session.deleteAttachment({ attachmentId });
```
