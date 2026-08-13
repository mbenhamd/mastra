import type { ApplyWorkingMemoryUpdateInput, WorkingMemoryPathProvenance, WorkingMemorySnapshot } from '../../types';

const CONTROL_METADATA_KEY = 'workingMemory';
const MAX_PROTECTED_PATHS = 256;
const MAX_POINTER_LENGTH = 1024;
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

function parseJsonValue(value: string | null): JsonValue | undefined {
  if (value === null) return null;
  try {
    return JSON.parse(value) as JsonValue;
  } catch {
    return undefined;
  }
}

function cloneJson<T extends JsonValue>(value: T): T {
  return structuredClone(value);
}

function getPointer(root: JsonValue, segments: readonly string[]): { exists: boolean; value?: JsonValue } {
  let current: JsonValue = root;
  for (const segment of segments) {
    if (Array.isArray(current)) {
      if (!/^\d+$/u.test(segment)) return { exists: false };
      const index = Number(segment);
      if (!Number.isSafeInteger(index) || index >= current.length) return { exists: false };
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

function setPointer(root: JsonValue, segments: readonly string[], value: JsonValue): JsonValue {
  if (segments.length === 0) return cloneJson(value);
  if (!isRecord(root) && !Array.isArray(root)) {
    root = /^\d+$/u.test(segments[0]!) ? [] : {};
  }

  let current = root as JsonValue[] | Record<string, JsonValue>;
  for (let index = 0; index < segments.length - 1; index += 1) {
    const segment = segments[index]!;
    const nextSegment = segments[index + 1]!;
    const existing = Array.isArray(current)
      ? /^\d+$/u.test(segment)
        ? current[Number(segment)]
        : undefined
      : current[segment];
    const next = isRecord(existing) || Array.isArray(existing) ? existing : /^\d+$/u.test(nextSegment) ? [] : {};
    if (Array.isArray(current)) {
      if (!/^\d+$/u.test(segment)) {
        throw new WorkingMemoryValidationError('Working-memory array paths must use numeric indexes.');
      }
      current[Number(segment)] = next;
    } else {
      current[segment] = next;
    }
    current = next;
  }

  const leaf = segments.at(-1)!;
  if (Array.isArray(current)) {
    if (!/^\d+$/u.test(leaf)) {
      throw new WorkingMemoryValidationError('Working-memory array paths must use numeric indexes.');
    }
    current[Number(leaf)] = cloneJson(value);
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

  let merged = cloneJson(proposed);
  for (const pointer of protectedPaths) {
    const segments = pointerSegments(pointer);
    const prior = getPointer(current, segments);
    if (!prior.exists) {
      throw new WorkingMemoryValidationError('Protected working-memory paths must exist in the stored value.');
    }
    merged = setPointer(merged, segments, prior.value!);
  }
  return JSON.stringify(merged);
}

function valuesEqual(left: JsonValue | undefined, right: JsonValue | undefined): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function changedJsonPointers(before: string | null, after: string | null): string[] {
  const beforeJson = parseJsonValue(before);
  const afterJson = parseJsonValue(after);
  if (beforeJson === undefined || afterJson === undefined) return before === after ? [] : [''];

  const changed: string[] = [];
  const visit = (left: JsonValue | undefined, right: JsonValue | undefined, path: string) => {
    if (valuesEqual(left, right)) return;
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
      changed.push(path);
      return;
    }
    const keys = new Set([...Object.keys(left), ...Object.keys(right)]);
    // Values may legitimately contain names that are unsafe to address as
    // mutable JavaScript object paths. Track the containing object as one
    // coarse provenance unit instead of emitting a control pointer that the
    // fail-closed reader would later reject.
    if ([...keys].some(key => FORBIDDEN_POINTER_SEGMENTS.has(key))) {
      changed.push(path);
      return;
    }
    if (keys.size === 0) changed.push(path);
    for (const key of [...keys].sort()) {
      visit(left[key], right[key], `${path}/${encodePointerSegment(key)}`);
    }
  };
  visit(beforeJson, afterJson, '');
  return changed;
}

function parseProvenance(value: unknown, currentRevision: number): Record<string, WorkingMemoryPathProvenance> {
  if (!isRecord(value)) {
    throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
  }
  const provenance: Record<string, WorkingMemoryPathProvenance> = {};
  for (const [path, entry] of Object.entries(value)) {
    if (!isRecord(entry)) {
      throw new WorkingMemoryValidationError('Stored working-memory controls are invalid.');
    }
    if (
      (entry.source !== 'owner' && entry.source !== 'observer') ||
      typeof entry.revision !== 'number' ||
      !Number.isSafeInteger(entry.revision) ||
      entry.revision < 0 ||
      entry.revision > currentRevision ||
      typeof entry.updatedAt !== 'string' ||
      entry.updatedAt.trim() === '' ||
      !Number.isFinite(Date.parse(entry.updatedAt))
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
  if (protectedPaths.length > 0 && !protectedPaths.includes('')) {
    const parsed = parseJsonValue(value ?? null);
    if (
      parsed === undefined ||
      (!isRecord(parsed) && !Array.isArray(parsed)) ||
      protectedPaths.some(path => !getPointer(parsed, pointerSegments(path)).exists)
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
      const firstSegments = pointerSegments(current.protectedPaths[0]!);
      let preserved: JsonValue = /^\d+$/u.test(firstSegments[0] ?? '') ? [] : {};
      for (const pointer of current.protectedPaths) {
        const segments = pointerSegments(pointer);
        const existing = getPointer(parsed, segments);
        if (existing.exists) preserved = setPointer(preserved, segments, existing.value!);
      }
      value = JSON.stringify(preserved);
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
  return {
    ...current,
    mastra: {
      ...mastra,
      [CONTROL_METADATA_KEY]: {
        revision: snapshot.revision,
        protectedPaths: snapshot.protectedPaths,
        provenance: snapshot.provenance,
      },
    },
  };
}

export function applyWorkingMemorySnapshotUpdate(
  current: WorkingMemorySnapshot,
  input: Pick<
    ApplyWorkingMemoryUpdateInput,
    'value' | 'expectedRevision' | 'source' | 'maxDataBytes' | 'protectPaths' | 'unprotectPaths'
  >,
  updatedAt = new Date().toISOString(),
): WorkingMemorySnapshot {
  if (!Number.isSafeInteger(input.expectedRevision) || input.expectedRevision < 0) {
    throw new WorkingMemoryValidationError('Working-memory expectedRevision must be a non-negative safe integer.');
  }
  if (current.revision !== input.expectedRevision) {
    throw new WorkingMemoryRevisionConflictError();
  }
  if (input.source !== 'observer' && input.source !== 'owner') {
    throw new WorkingMemoryValidationError('Working-memory source must be owner or observer.');
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
  if (!controlChanged && changedPaths.length === 0) return current;

  const revision = current.revision + 1;
  const provenance = { ...current.provenance };
  for (const path of changedPaths) {
    for (const existingPath of Object.keys(provenance)) {
      if (existingPath === path || path === '' || existingPath.startsWith(`${path}/`)) {
        delete provenance[existingPath];
      }
    }
    provenance[path] = { source: input.source, revision, updatedAt };
  }
  if (input.source === 'observer') {
    // Arrays are tracked as one changed pointer so that insertions/removals do
    // not assign misleading per-index provenance. Restore the explicit owner
    // marker for each protected path after that coarse entry is updated.
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
    for (const path of protectPaths) {
      provenance[path] = { source: 'owner', revision, updatedAt };
    }
  }

  return { value, revision, protectedPaths, provenance };
}
