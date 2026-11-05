### 4.2c Skill Discovery and Inspection

```ts
  // Skill discovery — applies the full resolution chain (code-registered,
  // then workspace-discovered). See §4.6. The public surface is flat on
  // `Session`, matching `useSkill(...)`; there is no `session.skills.*`
  // namespace and no harness-level skill catalogue. This keeps skill
  // inspection scoped to the workspace/resource that the session can actually
  // invoke against.
  listSkills(): Promise<HarnessSkill[]>;
  getSkill(name: string): Promise<HarnessSkill | undefined>;
  // Drop the cached workspace-discovery result. The next list / get / use call
  // re-runs async workspace discovery. Local-only: workspace discovery requires
  // server-side access to the configured workspace skill source, so
  // `refreshSkills` is absent from `RemoteSession`. See §4.6 and §13.5.
  refreshSkills(): Promise<void>;

  // Concurrency / inspection (read-only). These methods read the owning
  // session's live projection when resident, otherwise the reconciled
  // `SessionRecord` projection after hydration. `isBusy()` and
  // `waitForIdle(...)` use the session-owned idle boundary: no non-terminal
  // `currentRun` (`starting`, `running`, `waiting`, or `resuming`), no
  // canonical pending approval/suspension/question/plan item, an empty
  // `SessionRecord.pendingQueue`, and no in-flight inline or delegated
  // subagent execution owned by this session. They do not wait for upstream channel inbox
  // retry, downstream outbox delivery, or unrelated background diagnostics.
  // `waitForIdle({ timeout })` rejects if the timeout elapses before that
  // boundary is reached and does not mutate session state; the rejection is not
  // operation settlement evidence. Calling it from work that itself keeps the
  // same session busy can wait until that work reaches a terminal or pending
  // idle boundary.
  //
  // `getQueueDepth()` reports `SessionRecord.pendingQueue.length` under the
  // active session owner or from the hydrated record; it never counts the
  // legacy in-memory follow-up buffer.
  //
  // `getCurrentRunId()` and `getCurrentTraceId()` read the live agent state
  // when present, otherwise the reconciled `SessionRecord.currentRun` snapshot
  // (§5.1/§5.7). After a process restart they return `null` for interrupted
  // non-resumable direct messages unless a pending item or agent-layer run
  // proves the run is still active. Cancellation is not a session concern in
  // v1 — see §3.
  isBusy(): boolean;
  // `isTurnActive()` is true for an in-flight turn, an active queue drain, a
  // drained queued item, a non-empty `pendingQueue`, a pending resume that
  // has not been cancelled, or an `abortActiveWork()` / `resetPlanTasks()`
  // still cancelling children (Stop re-aborts the turn once they are
  // cancelled; in that window it can be true while `isBusy()` is false).
  // It ignores in-flight subagent executions, so a detached
  // `task_delegate` child that outlives its parent turn reads
  // `isBusy() === true` but `isTurnActive() === false`. An inline
  // `spawn_subagent` still reads true through the turn that runs it. Hosts
  // gating admission of a new user turn read this; shutdown and `waitForIdle`
  // keep the broader `isBusy()` boundary, and `useSkill` and the synchronous
  // structured-output form still refuse while `isBusy()`. It is local-only
  // for now: `RemoteSession` exposes `isBusy()` but not `isTurnActive()`.
  isTurnActive(): boolean;
  waitForIdle(opts?: { timeout?: number }): Promise<void>;
  getQueueDepth(): number;
  getCurrentRunId(): string | null;
  getCurrentTraceId(): string | null;

```
