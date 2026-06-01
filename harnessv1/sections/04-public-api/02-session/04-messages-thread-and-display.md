### 4.2d Messages, Thread, and Display

```ts
  // Messages
  listMessages(opts?: ListMessagesOptions): Promise<ListPage<HarnessMessage>>;
  // Chronological UX read model. This is reconstructed from existing durable
  // sources and redacted read models; it is not SSE replay, operation
  // settlement, or read-state.
  getActivityTimeline(opts?: ActivityTimelineOptions): Promise<SessionActivityTimeline>;

  // Thread app metadata. This is the only public thread-metadata extension
  // point. It writes `HarnessThread.metadata.app[key]`, never a raw top-level
  // thread metadata key. `key` must use the storage-safe metadata-key grammar
  // (`^[A-Za-z_][A-Za-z0-9_]{0,127}$`) and must not be `__proto__`,
  // `prototype`, `constructor`, or a reserved Harness, Mastra, Memory,
  // channel, or legacy metadata name; invalid or reserved keys reject with
  // `HarnessValidationError`. `value` must be canonical JSON (`JsonValue` —
  // see §4.4 / §6.1); non-JSON or lossy values reject before storage is
  // touched. The harness never reads `metadata.app` for mode/model,
  // permissions, token usage, OM config, channel state, subagent ownership,
  // thread title/list labels, or any other runtime decision. Use typed Session
  // or thread APIs for those fields.
  setThreadSetting(opts: { key: string; value: JsonValue }): Promise<void>;

  // Display state. This public shape is the wire/persistence-safe
  // `HarnessDisplayStateSnapshotV1` from §5.1: plain JSON only, with arrays or
  // records instead of Map/Set and epoch milliseconds instead of Date objects.
  // An implementation may keep a richer in-process render model internally,
  // but public reads, local display-state subscriptions, storage snapshots, and
  // HTTP responses all normalize to the snapshot shape. On hydration this is
  // rebuilt from the persisted display snapshot when usable, plus durable
  // queue/pending/currentRun/thread/message state. It is renderable state only,
  // not durable SSE replay. `subscribeDisplayState(...)` is an in-process
  // convenience; RemoteSession uses `getDisplayState()`, `subscribe(...)` SSE
  // events, and snapshot refetch instead of a separate display subscription API.
  getDisplayState(): Readonly<HarnessDisplayStateSnapshotV1>;
  subscribe(listener: HarnessListener): () => void;
  // Plan-task summary (TM-5). The snapshot carries an OPTIONAL bounded plan-task
  // summary — NOT the full tree (§5.1k):
  //   planTasks?: {
  //     total: number;
  //     byStatus: Partial<Record<HarnessPlanTaskStatus, number>>;
  //     inProgressTaskIds: string[]; // bounded: one per root (§5.1k)
  //     rootCount: number;
  //   }
  // It is absent until the session has observed its plan tree. It is cheap to
  // compute: the session keeps it in memory and refreshes it for FREE on every
  // plan-task mutation from the post-image the mutator already holds (no
  // per-snapshot storage read), and lazily seeds it once (bounded single-page
  // read) for a hydrated session that has pre-existing tasks but has not mutated
  // yet. UIs drive per-task detail off the `papersflow.plan_task.updated` event
  // deltas (§10.3) + the bounded `plan_task_check` read, not this summary.
  subscribeDisplayState(
    listener: (state: HarnessDisplayStateSnapshotV1) => void,
    opts?: { windowMs?: number },
  ): () => void;

```
