import { deepEqual } from '../../../utils/deep-equal';
import type {
  ApplyWorkingMemoryUpdateInput,
  StorageWorkingMemoryTransitionParticipantReceipt,
  WorkingMemoryPathProvenance,
  WorkingMemorySnapshot,
} from '../../types';

const CONTROL_METADATA_KEY = 'workingMemory';
const MAX_JSON_NESTING_DEPTH = 256;
const MAX_PROTECTED_PATHS = 256;
const MAX_POINTER_LENGTH = 1024;
// maxDataBytes deliberately bounds only the stored value. Provenance has an
// independent fixed cardinality, with room for one coarse entry plus every
// protected-path marker, so even a tiny value limit cannot erase owner controls.
const MAX_PROVENANCE_ENTRIES = MAX_PROTECTED_PATHS + 1;
const MAX_PROVENANCE_TIMESTAMP_LENGTH = 64;
const WORKING_MEMORY_INCARNATION_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;
const FORBIDDEN_POINTER_SEGMENTS = new Set(['__proto__', 'prototype', 'constructor']);

type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export class WorkingMemoryRevisionConflictError extends Error {
  constructor() {
    super('Working-memory revision conflict.');
    this.name = 'WorkingMemoryRevisionConflictError';
  }
}

/** A safe, caller-actionable rejection of a revisioned Working Memory request. */
export class WorkingMemoryValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WorkingMemoryValidationError';
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function decodePointerSegment(segment: string): string {
  if (/~(?:[^01]|$)/u.test(segment)) {
    throw new WorkingMemoryValidationError('Working-memory paths must use valid RFC 6901 escaping.');
  }
  const decoded = segment.replaceAll('~1', '/').replaceAll('~0', '~');
  if (FORBIDDEN_POINTER_SEGMENTS.has(decoded)) {
    throw new WorkingMemoryValidationError('Working-memory paths may not address prototype fields.');
  }
  return decoded;
}

function encodePointerSegment(segment: string): string {
  return segment.replaceAll('~', '~0').replaceAll('/', '~1');
}

function pointerSegments(pointer: string): string[] {
  if (typeof pointer !== 'string') {
    throw new WorkingMemoryValidationError('Working-memory paths must be strings.');
  }
  if (pointer === '') return [];
  if (!pointer.startsWith('/') || pointer.length > MAX_POINTER_LENGTH) {
    throw new WorkingMemoryValidationError('Working-memory paths must be bounded RFC 6901 JSON pointers.');
  }
  return pointer.slice(1).split('/').map(decodePointerSegment);
}

function parseArrayIndex(segment: string): number | undefined {
  if (!/^(?:0|[1-9]\d*)$/u.test(segment)) return undefined;
  const index = Number(segment);
  return Number.isSafeInteger(index) ? index : undefined;
}

export function normalizeWorkingMemoryPaths(paths: readonly string[] | undefined): string[] {
  if (!paths) return [];
  if (paths.length > MAX_PROTECTED_PATHS) {
    throw new WorkingMemoryValidationError(`Working memory supports at most ${MAX_PROTECTED_PATHS} protected paths.`);
  }

  const normalized = [
    ...new Set(
      paths.map(path => {
        pointerSegments(path);
        return path;
      }),
    ),
  ].sort();

  return normalized.filter((path, index) => {
    if (path === '') return true;
    return !normalized.slice(0, index).some(parent => parent === '' || path.startsWith(`${parent}/`));
  });
}

function assertSupportedJsonValue(value: JsonValue): void {
  const pending: Array<{ value: JsonValue; depth: number }> = [{ value, depth: 0 }];
  while (pending.length > 0) {
    const { value: current, depth } = pending.pop()!;
    if (typeof current === 'number') {
      if (!Number.isFinite(current) || (Number.isInteger(current) && !Number.isSafeInteger(current))) {
        throw new WorkingMemoryValidationError(
          'Working-memory JSON numbers must be finite safe integers or finite decimals.',
        );
      }
      continue;
    }
    if (!Array.isArray(current) && !isRecord(current)) continue;

    const childDepth = depth + 1;
    if (childDepth > MAX_JSON_NESTING_DEPTH) {
      throw new WorkingMemoryValidationError(
        `Working-memory JSON values may contain at most ${MAX_JSON_NESTING_DEPTH} nested arrays or objects.`,
      );
    }
    const children = Array.isArray(current) ? current : (Object.values(current) as JsonValue[]);
    for (const child of children) pending.push({ value: child, depth: childDepth });
  }
}

function parseJsonValue(value: string | null): JsonValue | undefined {
  if (value === null) return null;
  let parsed: JsonValue;
  try {
    parsed = JSON.parse(value) as JsonValue;
  } catch {
    return undefined;
  }
  assertSupportedJsonValue(parsed);
  return parsed;
}

function cloneJson<T extends JsonValue>(value: T): T {
  return structuredClone(value);
}

function getPointer(root: JsonValue, segments: readonly string[]): { exists: boolean; value?: JsonValue } {
  let current: JsonValue = root;
  for (const segment of segments) {
    if (Array.isArray(current)) {
      const index = parseArrayIndex(segment);
      if (index === undefined || index >= current.length) return { exists: false };
      current = current[index]!;
      continue;
    }
    if (!isRecord(current) || !Object.prototype.hasOwnProperty.call(current, segment)) {
      return { exists: false };
    }
    current = current[segment] as JsonValue;
  }
  return { exists: true, value: current };
}

function sameContainerType(left: JsonValue, right: JsonValue): boolean {
  return (Array.isArray(left) && Array.isArray(right)) || (isRecord(left) && isRecord(right));
}

function emptyContainerLike(value: JsonValue): JsonValue[] | Record<string, JsonValue> {
  if (Array.isArray(value)) return [];
  if (isRecord(value)) return {};
  throw new WorkingMemoryValidationError('Protected working-memory paths must traverse JSON containers.');
}

function setPointer(
  root: JsonValue,
  segments: readonly string[],
  value: JsonValue,
  templateRoot: JsonValue,
): JsonValue {
  if (segments.length === 0) return cloneJson(value);
  if (!sameContainerType(root, templateRoot)) {
    root = emptyContainerLike(templateRoot);
  }

  let current = root as JsonValue[] | Record<string, JsonValue>;
  let template = templateRoot as JsonValue[] | Record<string, JsonValue>;
  for (let index = 0; index < segments.length - 1; index += 1) {
    const segment = segments[index]!;
    const arrayIndex = Array.isArray(template) ? parseArrayIndex(segment) : undefined;
    if (Array.isArray(template) && arrayIndex === undefined) {
      throw new WorkingMemoryValidationError('Working-memory array paths must use numeric indexes.');
    }
    const templateNext = Array.isArray(template) ? template[arrayIndex!] : template[segment];
    if (templateNext === undefined) {
      throw new WorkingMemoryValidationError('Protected working-memory paths must exist in the stored value.');
    }
    const existing = Array.isArray(current) ? current[arrayIndex!] : current[segment];
    const next: JsonValue[] | Record<string, JsonValue> =
      existing !== undefined && sameContainerType(existing, templateNext)
        ? (existing as JsonValue[] | Record<string, JsonValue>)
        : emptyContainerLike(templateNext);
    if (Array.isArray(current)) {
      current[arrayIndex!] = next;
    } else {
      current[segment] = next;
    }
    current = next;
    template = templateNext as JsonValue[] | Record<string, JsonValue>;
  }

  const leaf = segments.at(-1)!;
  if (Array.isArray(current)) {
    const arrayIndex = parseArrayIndex(leaf);
    if (arrayIndex === undefined) {
      throw new WorkingMemoryValidationError('Working-memory array paths must use numeric indexes.');
    }
    current[arrayIndex] = cloneJson(value);
  } else {
    current[leaf] = cloneJson(value);
  }
  return root;
}

function preserveProtectedPaths(
  currentValue: string | null,
  proposedValue: string | null,
  protectedPaths: readonly string[],
): string | null {
  if (protectedPaths.length === 0) return proposedValue;
  if (protectedPaths.includes('')) return currentValue;

  const current = parseJsonValue(currentValue);
  const proposed = parseJsonValue(proposedValue);
  if (current === undefined || proposed === undefined) {
    throw new WorkingMemoryValidationError('Path-protected working memory must contain valid JSON.');
  }

  let merged: JsonValue | undefined;
  for (const pointer of protectedPaths) {
    const segments = pointerSegments(pointer);
    const prior = getPointer(current, segments);
    if (!prior.exists) {
      throw new WorkingMemoryValidationError('Protected working-memory paths must exist in the stored value.');
    }
    const proposedAtPointer = getPointer(proposed, segments);
    if (proposedAtPointer.exists && deepEqual(proposedAtPointer.value, prior.value)) continue;
    if (merged === undefined) merged = cloneJson(proposed);
    merged = setPointer(merged, segments, prior.value!, current);
  }
  return merged === undefined ? proposedValue : JSON.stringify(merged);
}

function valuesEqual(left: JsonValue | undefined, right: JsonValue | undefined): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function isValidProvenanceTimestamp(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length <= MAX_PROVENANCE_TIMESTAMP_LENGTH &&
    value.trim() !== '' &&
    Number.isFinite(Date.parse(value))
  );
}

function changedJsonPointers(before: string | null, after: string | null): string[] {
  const beforeJson = parseJsonValue(before);
  const afterJson = parseJsonValue(after);
  // The storage contract distinguishes no value (`null`) from the literal JSON
  // text `"null"`, even though both parse to the same JavaScript value.
  if (before === null || after === null) return before === after ? [] : [''];
  if (beforeJson === undefined || afterJson === undefined) return before === after ? [] : [''];

  const changed: string[] = [];
  let exceededEntryLimit = false;
  const recordChange = (path: string) => {
    if (changed.length >= MAX_PROVENANCE_ENTRIES) {
      exceededEntryLimit = true;
      return;
    }
    changed.push(path);
  };
  const visit = (left: JsonValue | undefined, right: JsonValue | undefined, path: string) => {
    if (exceededEntryLimit || valuesEqual(left, right)) return;
    if (
      left === undefined ||
      right === undefined ||
      left === null ||
      right === null ||
      typeof left !== 'object' ||
      typeof right !== 'object' ||
      Array.isArray(left) ||
      Array.isArray(right)
    ) {
      recordChange(path);
      return;
    }
    const keys = new Set([...Object.keys(left), ...Object.keys(right)]);
    // Values may legitimately contain names that are unsafe to address as
    // mutable JavaScript object paths. Track the containing object as one
    // coarse provenance unit instead of emitting a control pointer that the
    // fail-closed reader would later reject.
    if ([...keys].some(key => FORBIDDEN_POINTER_SEGMENTS.has(key))) {
      recordChange(path);
      return;
    }
    // Stored JSON keys are not control input and may be longer than a bounded
    // JSON pointer permits. Collapse their changes to the nearest valid
    // ancestor so a value we just wrote always remains readable.
    if ([...keys].some(key => `${path}/${encodePointerSegment(key)}`.length > MAX_POINTER_LENGTH)) {
      recordChange(path);
      return;
    }
    if (keys.size === 0) recordChange(path);
    for (const key of [...keys].sort()) {
      visit(left[key], right[key], `${path}/${encodePointerSegment(key)}`);
      if (exceededEntryLimit) break;
    }
  };
  visit(beforeJson, afterJson, '');
  // A whole-value update can be represented safely at the root. Owner markers
  // for protected descendants are restored after this coarse entry is applied.
  return exceededEntryLimit ? [''] : changed;
}

function parseProvenance(value: unknown, currentRevision: number): Record<string, WorkingMemoryPathProvenance> {
  if (!isRecord(value)) {
    throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
  }
  const entries = Object.entries(value);
  if (entries.length > MAX_PROVENANCE_ENTRIES) {
    throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
  }
  const provenance: Record<string, WorkingMemoryPathProvenance> = {};
  for (const [path, entry] of entries) {
    if (!isRecord(entry)) {
      throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
    }
    if (
      (entry.source !== 'owner' && entry.source !== 'observer') ||
      typeof entry.revision !== 'number' ||
      !Number.isSafeInteger(entry.revision) ||
      entry.revision < 0 ||
      entry.revision > currentRevision ||
      !isValidProvenanceTimestamp(entry.updatedAt)
    )
      throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
    try {
      pointerSegments(path);
      provenance[path] = {
        source: entry.source,
        revision: entry.revision,
        updatedAt: entry.updatedAt,
      };
    } catch {
      throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
    }
  }
  return provenance;
}

export function readWorkingMemorySnapshot(
  value: string | null | undefined,
  metadata: Record<string, unknown> | undefined,
): WorkingMemorySnapshot {
  const storedMastra = metadata?.mastra;
  if (storedMastra !== undefined && !isRecord(storedMastra)) {
    throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
  }
  const mastra = storedMastra ?? {};
  const storedControl = mastra[CONTROL_METADATA_KEY];
  if (storedControl !== undefined && !isRecord(storedControl)) {
    throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
  }
  const control = storedControl ?? {};
  if (
    storedControl !== undefined &&
    (typeof control.revision !== 'number' ||
      !Number.isSafeInteger(control.revision) ||
      control.revision < 0 ||
      !Array.isArray(control.protectedPaths) ||
      control.protectedPaths.some(path => typeof path !== 'string') ||
      !isRecord(control.provenance))
  ) {
    throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
  }
  const revision = storedControl === undefined ? 0 : (control.revision as number);
  const protectedPaths = normalizeWorkingMemoryPaths(
    storedControl === undefined ? [] : (control.protectedPaths as string[]),
  );
  const parsedValue = parseJsonValue(value ?? null);
  if (protectedPaths.length > 0 && !protectedPaths.includes('')) {
    if (
      parsedValue === undefined ||
      (!isRecord(parsedValue) && !Array.isArray(parsedValue)) ||
      protectedPaths.some(path => !getPointer(parsedValue, pointerSegments(path)).exists)
    ) {
      throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
    }
  }
  return {
    value: value ?? null,
    revision,
    protectedPaths,
    provenance: storedControl === undefined ? {} : parseProvenance(control.provenance, revision),
  };
}

/** Whether metadata carries the native revision/protection control record. */
export function hasWorkingMemorySnapshotControls(metadata: Record<string, unknown> | undefined): boolean {
  const mastra = isRecord(metadata?.mastra) ? metadata.mastra : undefined;
  return mastra !== undefined && Object.prototype.hasOwnProperty.call(mastra, CONTROL_METADATA_KEY);
}

function assertWorkingMemoryIncarnationValue(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !WORKING_MEMORY_INCARNATION_PATTERN.test(value)) {
    throw new WorkingMemoryValidationError('Stored working-memory incarnation is invalid.');
  }
}

/** @internal Read the opaque identity of the governed control lifetime. */
export function readWorkingMemoryIncarnation(metadata: Record<string, unknown> | undefined): string | null {
  if (!hasWorkingMemorySnapshotControls(metadata)) return null;
  const mastra = metadata!.mastra as Record<string, unknown>;
  const control = mastra[CONTROL_METADATA_KEY];
  if (!isRecord(control)) {
    throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
  }
  assertWorkingMemoryIncarnationValue(control.incarnation);
  return control.incarnation;
}

/** @internal Reject a stale or malformed governed-control incarnation. */
export function assertWorkingMemoryIncarnation(
  currentIncarnation: string | null,
  expectedIncarnation: string | null,
): void {
  if (currentIncarnation !== null) assertWorkingMemoryIncarnationValue(currentIncarnation);
  if (expectedIncarnation !== null) assertWorkingMemoryIncarnationValue(expectedIncarnation);
  if (currentIncarnation !== expectedIncarnation) throw new WorkingMemoryRevisionConflictError();
}

/** @internal Validate one atomically prepared scope-transition participant. */
export function assertWorkingMemoryTransitionParticipant(
  currentSnapshot: WorkingMemorySnapshot,
  currentIncarnation: string | null,
  expected: StorageWorkingMemoryTransitionParticipantReceipt,
): void {
  assertWorkingMemoryIncarnation(currentIncarnation, expected.workingMemoryIncarnation);
  assertWorkingMemorySnapshotRevision(currentSnapshot, expected.snapshot.revision);
  if (
    currentIncarnation === null &&
    expected.workingMemoryIncarnation === null &&
    currentSnapshot.value !== expected.snapshot.value
  ) {
    throw new WorkingMemoryRevisionConflictError();
  }
}

/**
 * @internal Issue a fresh identity whenever caller-supplied governed metadata
 * starts a new control lifetime at a coordinate that currently has no controls.
 */
export function reincarnateWorkingMemorySnapshotMetadata(
  metadata: Record<string, unknown> | undefined,
): Record<string, unknown> | undefined {
  if (!hasWorkingMemorySnapshotControls(metadata)) return metadata;
  const mastra = metadata!.mastra as Record<string, unknown>;
  const control = mastra[CONTROL_METADATA_KEY];
  if (!isRecord(control)) return metadata;
  return {
    ...metadata,
    mastra: {
      ...mastra,
      [CONTROL_METADATA_KEY]: { ...control, incarnation: crypto.randomUUID() },
    },
  };
}

/** Prevent generic whole-row saves from moving a governed snapshot to a different owner. */
export function assertGovernedThreadResourceUnchanged({
  currentResourceId,
  currentMetadata,
  proposedResourceId,
}: {
  currentResourceId: string;
  currentMetadata: Record<string, unknown> | undefined;
  proposedResourceId: string;
}): void {
  if (currentResourceId !== proposedResourceId && hasWorkingMemorySnapshotControls(currentMetadata)) {
    throw new WorkingMemoryValidationError(
      'Threads with revisioned working memory cannot be reassigned to another resource by saveThread.',
    );
  }
}

/** Assert that a governed thread-to-resource transition cannot retain a duplicate snapshot. */
export function assertThreadWorkingMemoryRemoved(metadata: Record<string, unknown> | undefined): void {
  if (
    Object.prototype.hasOwnProperty.call(metadata ?? {}, 'workingMemory') ||
    hasWorkingMemorySnapshotControls(metadata)
  ) {
    throw new WorkingMemoryValidationError(
      'Thread-to-resource working-memory transitions require sanitized thread metadata.',
    );
  }
}

/** Remove thread-local Working Memory fields after their governance has been handled explicitly. */
export function stripThreadWorkingMemoryMetadata(
  metadata: Record<string, unknown> | undefined,
): Record<string, unknown> | undefined {
  if (metadata === undefined) return undefined;

  const stripped = { ...metadata };
  delete stripped.workingMemory;
  if (isRecord(stripped.mastra)) {
    const mastra = { ...stripped.mastra };
    delete mastra[CONTROL_METADATA_KEY];
    if (Object.keys(mastra).length === 0) delete stripped.mastra;
    else stripped.mastra = mastra;
  }
  return stripped;
}

/** Merge a partial thread update before removing its governed Working Memory fields. */
export function mergeThreadMetadataForWorkingMemoryTransition(
  current: Record<string, unknown> | undefined,
  update: Record<string, unknown> | undefined,
): Record<string, unknown> | undefined {
  if (update === undefined) return stripThreadWorkingMemoryMetadata(current);

  const merged = { ...current, ...update };
  if (isRecord(update.mastra)) {
    const currentMastra = isRecord(current?.mastra) ? current.mastra : {};
    merged.mastra = { ...currentMastra, ...update.mastra };
  }
  return stripThreadWorkingMemoryMetadata(merged);
}

/**
 * Fail closed when changing scope without an explicit migration value would
 * hide a governed thread snapshot.
 */
export function assertThreadWorkingMemoryIsUngoverned(metadata: Record<string, unknown> | undefined): void {
  const value = typeof metadata?.workingMemory === 'string' ? metadata.workingMemory : null;
  readWorkingMemorySnapshot(value, metadata);
  if (hasWorkingMemorySnapshotControls(metadata)) {
    throw new WorkingMemoryValidationError(
      'Governed thread working memory requires an explicit workingMemory value before switching to resource scope.',
    );
  }
}

function storedControlEquals(left: unknown, right: unknown): boolean {
  type CompareFrame = { kind: 'compare'; left: unknown; right: unknown };
  type CompleteFrame = { kind: 'complete'; left: object; right: object };
  type ComparisonFrame = CompareFrame | CompleteFrame;
  type PairState = 'visiting' | 'complete';

  const pending: ComparisonFrame[] = [{ kind: 'compare', left, right }];
  const pairStates = new WeakMap<object, WeakMap<object, PairState>>();
  const setPairState = (leftObject: object, rightObject: object, state: PairState) => {
    let rightStates = pairStates.get(leftObject);
    if (!rightStates) {
      rightStates = new WeakMap<object, PairState>();
      pairStates.set(leftObject, rightStates);
    }
    rightStates.set(rightObject, state);
  };

  while (pending.length > 0) {
    const frame = pending.pop()!;
    if (frame.kind === 'complete') {
      setPairState(frame.left, frame.right, 'complete');
      continue;
    }

    const { left: currentLeft, right: currentRight } = frame;
    if (currentLeft === currentRight) continue;
    if (
      currentLeft === null ||
      currentRight === null ||
      typeof currentLeft !== 'object' ||
      typeof currentRight !== 'object'
    ) {
      return false;
    }

    const pairState = pairStates.get(currentLeft)?.get(currentRight);
    // Separate cyclic control graphs are not safe repetitions of the stored
    // value. Fail closed so the public guard emits its validation error.
    if (pairState === 'visiting') return false;
    if (pairState === 'complete') continue;

    if (Array.isArray(currentLeft) || Array.isArray(currentRight)) {
      if (!Array.isArray(currentLeft) || !Array.isArray(currentRight) || currentLeft.length !== currentRight.length) {
        return false;
      }
      setPairState(currentLeft, currentRight, 'visiting');
      pending.push({ kind: 'complete', left: currentLeft, right: currentRight });
      for (let index = currentLeft.length - 1; index >= 0; index -= 1) {
        pending.push({ kind: 'compare', left: currentLeft[index], right: currentRight[index] });
      }
      continue;
    }

    const leftRecord = currentLeft as Record<string, unknown>;
    const rightRecord = currentRight as Record<string, unknown>;
    const leftKeys = Object.keys(leftRecord);
    if (
      leftKeys.length !== Object.keys(rightRecord).length ||
      leftKeys.some(key => !Object.prototype.hasOwnProperty.call(rightRecord, key))
    ) {
      return false;
    }
    setPairState(currentLeft, currentRight, 'visiting');
    pending.push({ kind: 'complete', left: currentLeft, right: currentRight });
    for (let index = leftKeys.length - 1; index >= 0; index -= 1) {
      const key = leftKeys[index]!;
      pending.push({ kind: 'compare', left: leftRecord[key], right: rightRecord[key] });
    }
  }

  return true;
}

/**
 * Reject generic storage writes that try to change a native revisioned Working
 * Memory value or its control record. Generic writers may omit those fields or
 * repeat the exact stored values (for example from a stale whole-row payload),
 * but mutations must go through applyWorkingMemoryUpdate so revision checks and
 * owner protections remain atomic.
 */
export function assertWorkingMemorySnapshotUnchanged({
  currentValue,
  currentMetadata,
  proposedValue,
  proposedValueProvided,
  proposedMetadata,
}: {
  currentValue: string | null | undefined;
  currentMetadata: Record<string, unknown> | undefined;
  proposedValue: unknown;
  proposedValueProvided: boolean;
  proposedMetadata: Record<string, unknown> | undefined;
}): void {
  if (!hasWorkingMemorySnapshotControls(currentMetadata)) return;

  const current = readWorkingMemorySnapshot(currentValue, currentMetadata);
  if (proposedValueProvided && proposedValue !== current.value) {
    throw new WorkingMemoryValidationError('Revisioned working memory must be changed with applyWorkingMemoryUpdate.');
  }

  if (!proposedMetadata || !Object.prototype.hasOwnProperty.call(proposedMetadata, 'mastra')) return;
  if (!isRecord(proposedMetadata.mastra)) {
    throw new WorkingMemoryValidationError(
      'Revisioned working memory controls must be changed with applyWorkingMemoryUpdate.',
    );
  }
  if (!Object.prototype.hasOwnProperty.call(proposedMetadata.mastra, CONTROL_METADATA_KEY)) return;

  const currentMastra = currentMetadata!.mastra as Record<string, unknown>;
  if (!storedControlEquals(proposedMetadata.mastra[CONTROL_METADATA_KEY], currentMastra[CONTROL_METADATA_KEY])) {
    throw new WorkingMemoryValidationError(
      'Revisioned working memory controls must be changed with applyWorkingMemoryUpdate.',
    );
  }
}

/** Restore the stored native control record after an allowed generic metadata write. */
export function preserveWorkingMemorySnapshotControls(
  currentMetadata: Record<string, unknown> | undefined,
  nextMetadata: Record<string, unknown>,
): Record<string, unknown> {
  if (!hasWorkingMemorySnapshotControls(currentMetadata)) return nextMetadata;
  const currentMastra = currentMetadata!.mastra as Record<string, unknown>;
  const nextMastra = isRecord(nextMetadata.mastra) ? nextMetadata.mastra : {};
  return {
    ...nextMetadata,
    mastra: {
      ...nextMastra,
      [CONTROL_METADATA_KEY]: structuredClone(currentMastra[CONTROL_METADATA_KEY]),
    },
  };
}

/**
 * Remove observer-derived values while retaining every path the owner has
 * protected. Callers must persist the returned snapshot atomically with the OM
 * retraction that invalidated the derived values.
 */
export function retractObserverWorkingMemorySnapshot(current: WorkingMemorySnapshot): WorkingMemorySnapshot {
  let value = current.value;
  if (current.value !== null && !current.protectedPaths.includes('')) {
    value = null;
  }
  if (current.value !== null && current.protectedPaths.length > 0 && !current.protectedPaths.includes('')) {
    const parsed = parseJsonValue(current.value);
    if (parsed === undefined) {
      // A malformed protected value fails closed: retraction must not destroy
      // data that an owner explicitly asked the observer to preserve.
      value = current.value;
    } else {
      let preserved: JsonValue = Array.isArray(parsed) ? [] : {};
      for (const pointer of current.protectedPaths) {
        const segments = pointerSegments(pointer);
        const existing = getPointer(parsed, segments);
        if (existing.exists) preserved = setPointer(preserved, segments, existing.value!, parsed);
      }
      value = deepEqual(parsed, preserved) ? current.value : JSON.stringify(preserved);
    }
  }

  const protectedPaths = current.protectedPaths;
  const provenance = Object.fromEntries(
    Object.entries(current.provenance).filter(
      ([path, entry]) =>
        entry.source === 'owner' &&
        protectedPaths.some(
          protectedPath =>
            protectedPath === '' ||
            path === protectedPath ||
            path.startsWith(`${protectedPath}/`) ||
            protectedPath.startsWith(`${path}/`),
        ),
    ),
  );
  if (value === current.value && JSON.stringify(provenance) === JSON.stringify(current.provenance)) return current;
  return {
    value,
    revision: current.revision + 1,
    protectedPaths,
    provenance,
  };
}

export function writeWorkingMemorySnapshotMetadata(
  metadata: Record<string, unknown> | undefined,
  snapshot: WorkingMemorySnapshot,
): Record<string, unknown> {
  const current = metadata ?? {};
  const mastra = isRecord(current.mastra) ? current.mastra : {};
  const currentControl = mastra[CONTROL_METADATA_KEY];
  let incarnation: string;
  if (isRecord(currentControl) && currentControl.incarnation !== undefined) {
    assertWorkingMemoryIncarnationValue(currentControl.incarnation);
    incarnation = currentControl.incarnation;
  } else {
    incarnation = crypto.randomUUID();
  }
  // Keep this public serialization helper fail-closed even when a caller did
  // not obtain its snapshot from applyWorkingMemorySnapshotUpdate.
  const validated = readWorkingMemorySnapshot(snapshot.value, {
    mastra: {
      [CONTROL_METADATA_KEY]: {
        revision: snapshot.revision,
        protectedPaths: snapshot.protectedPaths,
        provenance: snapshot.provenance,
      },
    },
  });
  return {
    ...current,
    mastra: {
      ...mastra,
      [CONTROL_METADATA_KEY]: {
        incarnation,
        revision: validated.revision,
        protectedPaths: [...validated.protectedPaths],
        provenance: Object.fromEntries(
          Object.entries(validated.provenance).map(([path, entry]) => [path, { ...entry }]),
        ),
      },
    },
  };
}

/** Fail closed unless a stored snapshot still has the caller's observed revision. */
export function assertWorkingMemorySnapshotRevision(
  current: Pick<WorkingMemorySnapshot, 'revision'>,
  expectedRevision: number,
): void {
  if (!Number.isSafeInteger(expectedRevision) || expectedRevision < 0) {
    throw new WorkingMemoryValidationError('Working-memory expectedRevision must be a non-negative safe integer.');
  }
  if (current.revision !== expectedRevision) {
    throw new WorkingMemoryRevisionConflictError();
  }
}

export function applyWorkingMemorySnapshotUpdate(
  current: WorkingMemorySnapshot,
  input: Pick<
    ApplyWorkingMemoryUpdateInput,
    'value' | 'expectedRevision' | 'source' | 'maxDataBytes' | 'protectPaths' | 'unprotectPaths'
  >,
  updatedAt = new Date().toISOString(),
): WorkingMemorySnapshot {
  assertWorkingMemorySnapshotRevision(current, input.expectedRevision);
  if (input.source !== 'observer' && input.source !== 'owner') {
    throw new WorkingMemoryValidationError('Working-memory source must be owner or observer.');
  }
  if (!isValidProvenanceTimestamp(updatedAt)) {
    throw new WorkingMemoryValidationError('Working-memory provenance timestamp is invalid.');
  }
  if (input.value !== null && typeof input.value !== 'string') {
    throw new WorkingMemoryValidationError('Working-memory value must be a string or null.');
  }
  if (input.maxDataBytes !== undefined && (!Number.isSafeInteger(input.maxDataBytes) || input.maxDataBytes <= 0)) {
    throw new WorkingMemoryValidationError('Working-memory maxDataBytes must be a positive safe integer.');
  }
  if (input.source === 'observer' && (input.protectPaths?.length || input.unprotectPaths?.length)) {
    throw new WorkingMemoryValidationError('Observer working-memory updates cannot change protected paths.');
  }

  const protectPaths = normalizeWorkingMemoryPaths(input.protectPaths);
  const unprotectPaths = normalizeWorkingMemoryPaths(input.unprotectPaths);
  if (protectPaths.some(path => unprotectPaths.includes(path))) {
    throw new WorkingMemoryValidationError(
      'A working-memory path cannot be protected and unprotected in the same update.',
    );
  }

  const nextProtected = new Set(current.protectedPaths);
  for (const path of unprotectPaths) nextProtected.delete(path);
  for (const path of protectPaths) nextProtected.add(path);
  const protectedPaths = normalizeWorkingMemoryPaths([...nextProtected]);
  const value =
    input.source === 'observer'
      ? preserveProtectedPaths(current.value, input.value, current.protectedPaths)
      : input.value;
  if (protectedPaths.length > 0 && !protectedPaths.includes('')) {
    const parsed = parseJsonValue(value);
    if (parsed === undefined || (!isRecord(parsed) && !Array.isArray(parsed))) {
      throw new WorkingMemoryValidationError('Path-protected working memory must contain a JSON object or array.');
    }
    if (protectedPaths.some(path => !getPointer(parsed, pointerSegments(path)).exists)) {
      throw new WorkingMemoryValidationError('Protected working-memory paths must exist in the stored value.');
    }
  }
  if (
    value !== null &&
    input.maxDataBytes !== undefined &&
    new TextEncoder().encode(value).byteLength > input.maxDataBytes
  ) {
    throw new WorkingMemoryValidationError('Working-memory value exceeds the configured UTF-8 byte limit.');
  }

  const controlChanged = JSON.stringify(protectedPaths) !== JSON.stringify(current.protectedPaths);
  const changedPaths = changedJsonPointers(current.value, value);
  if (!controlChanged && value === current.value) return current;

  const revision = current.revision + 1;
  const parsedValue = parseJsonValue(value);
  const buildProvenance = (paths: readonly string[]) => {
    const provenance = { ...current.provenance };
    for (const path of paths) {
      for (const existingPath of Object.keys(provenance)) {
        if (existingPath === path || path === '' || existingPath.startsWith(`${path}/`)) {
          delete provenance[existingPath];
        }
      }
      if (path === '' || parsedValue === undefined || getPointer(parsedValue, pointerSegments(path)).exists) {
        provenance[path] = { source: input.source, revision, updatedAt };
      }
    }
    if (input.source === 'observer') {
      // Arrays and over-budget objects are tracked as coarse pointers. Restore
      // every explicit owner marker after those entries are updated.
      for (const protectedPath of current.protectedPaths) {
        const priorOwnerEntry =
          current.provenance[protectedPath] ??
          Object.entries(current.provenance).find(
            ([path, entry]) =>
              entry.source === 'owner' &&
              (path === '' ||
                protectedPath === '' ||
                path.startsWith(`${protectedPath}/`) ||
                protectedPath.startsWith(`${path}/`)),
          )?.[1];
        if (priorOwnerEntry?.source === 'owner') provenance[protectedPath] = priorOwnerEntry;
      }
    }
    if (input.source === 'owner') {
      const pathsToMark = paths.includes('') ? protectedPaths : protectPaths;
      for (const path of pathsToMark) {
        provenance[path] = { source: 'owner', revision, updatedAt };
      }
    }
    return provenance;
  };

  let provenance = buildProvenance(changedPaths);
  if (Object.keys(provenance).length > MAX_PROVENANCE_ENTRIES) {
    provenance = buildProvenance(['']);
  }
  if (Object.keys(provenance).length > MAX_PROVENANCE_ENTRIES) {
    throw new WorkingMemoryValidationError('Working-memory provenance exceeds the supported path limit.');
  }

  return { value, revision, protectedPaths, provenance };
}
