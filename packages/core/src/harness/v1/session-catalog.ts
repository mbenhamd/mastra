import { HarnessValidationError } from './errors';
import type {
  HarnessActionCatalogEntry,
  HarnessActionCatalogSourceKind,
  HarnessSkill,
  HarnessSkillActionMetadata,
} from './types';

export type ActionCatalogSkillDescriptor = Pick<
  HarnessSkill,
  'name' | 'description' | 'filePath' | 'category' | 'action' | 'metadata'
>;

export type ActionCatalogMcpCacheEntry = {
  entries: HarnessActionCatalogEntry[];
  expiresAt?: number;
  successful?: boolean;
};

export type ActionCatalogMcpTimedOutWork = {
  work: Promise<HarnessActionCatalogEntry[]>;
  retryAfter: number;
  retryCount: number;
};

export const ACTION_CATALOG_DEFAULT_LIMIT = 100;

export const ACTION_CATALOG_MAX_LIMIT = 500;

const ACTION_CATALOG_MCP_LIST_TIMEOUT_MS = 2_000;

export const ACTION_CATALOG_MCP_SUCCESS_CACHE_MS = 5_000;

export const ACTION_CATALOG_MCP_FAILURE_CACHE_MS = 30_000;

export const ACTION_CATALOG_MCP_MAX_TIMEOUT_RETRIES = 1;

export const ACTION_CATALOG_SOURCE_KINDS: readonly HarnessActionCatalogSourceKind[] = [
  'skill',
  'mcp-server',
  'mcp-tool',
];

export const ACTION_CATALOG_SOURCE_ORDER: Record<HarnessActionCatalogSourceKind, number> = {
  skill: 0,
  'mcp-server': 1,
  'mcp-tool': 2,
};

export const SUPPORTED_SKILL_ARG_SCHEMA_KEYS = new Set([
  'required',
  'properties',
  'type',
  'enum',
  'items',
  'additionalProperties',
]);

export const RESERVED_MCP_SERVER_KEYS = new Set([...Object.getOwnPropertyNames(Object.prototype), '__proto__']);

export function cloneMcpCatalogValue(value: unknown): unknown | undefined {
  if (value === undefined) return undefined;
  try {
    return structuredClone(value);
  } catch {
    try {
      return JSON.parse(JSON.stringify(value)) as unknown;
    } catch {
      return undefined;
    }
  }
}

function isPlainMcpCatalogObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  if (Object.prototype.toString.call(value) !== '[object Object]') return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

export function cloneMcpCatalogRecord(value: unknown): Record<string, unknown> | undefined {
  const cloned = cloneMcpCatalogValue(value);
  return isPlainMcpCatalogObject(cloned) ? cloned : undefined;
}

export function cloneMcpCatalogRecordArray(value: unknown): readonly Record<string, unknown>[] | undefined {
  const cloned = cloneMcpCatalogValue(value);
  if (!Array.isArray(cloned) || cloned.some(item => !isPlainMcpCatalogObject(item))) {
    return undefined;
  }
  return cloned as Record<string, unknown>[];
}

export function cloneMcpSchemaLike(value: unknown): unknown | undefined {
  if (value && typeof value === 'object' && 'jsonSchema' in value) {
    return cloneMcpCatalogValue((value as { jsonSchema?: unknown }).jsonSchema);
  }
  return cloneMcpCatalogValue(value);
}

export function encodeActionCatalogIdPart(value: string): string {
  return encodeURIComponent(value);
}

export function compareActionCatalogIds(a: string, b: string): number {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

export function cloneActionCatalogEntry(entry: HarnessActionCatalogEntry): HarnessActionCatalogEntry {
  const cloned = cloneMcpCatalogValue(entry);
  if (cloned === undefined) {
    throw new HarnessValidationError(
      'actions.list()',
      `could not clone action catalog entry ${JSON.stringify(entry.id)}`,
    );
  }
  return cloned as HarnessActionCatalogEntry;
}

function cloneActionCatalogStringArray(value: unknown): readonly string[] | undefined {
  if (!Array.isArray(value) || value.some(item => typeof item !== 'string' || item.length === 0)) {
    return undefined;
  }
  return [...value];
}

function cloneActionCatalogShortcuts(value: unknown): HarnessSkillActionMetadata['shortcuts'] | undefined {
  if (!Array.isArray(value)) return undefined;
  const shortcuts = value.map(item => {
    if (!isPlainMcpCatalogObject(item) || typeof item.id !== 'string' || item.id.length === 0) return undefined;
    const keys = cloneActionCatalogStringArray(item.keys);
    if (item.keys !== undefined && !keys) return undefined;
    if (item.label !== undefined && typeof item.label !== 'string') return undefined;
    return {
      id: item.id,
      ...(item.label ? { label: item.label } : {}),
      ...(keys ? { keys } : {}),
    };
  });
  return shortcuts.some(shortcut => !shortcut) ? undefined : (shortcuts as HarnessSkillActionMetadata['shortcuts']);
}

function cloneActionCatalogPermissions(value: unknown): HarnessSkillActionMetadata['permissions'] | undefined {
  if (!isPlainMcpCatalogObject(value)) return undefined;
  const tools = cloneActionCatalogStringArray(value.tools);
  const fileScopes = cloneActionCatalogStringArray(value.fileScopes);
  const networkScopes = cloneActionCatalogStringArray(value.networkScopes);
  const mcpScopes = cloneActionCatalogStringArray(value.mcpScopes);
  if (
    (value.tools !== undefined && !tools) ||
    (value.fileScopes !== undefined && !fileScopes) ||
    (value.networkScopes !== undefined && !networkScopes) ||
    (value.mcpScopes !== undefined && !mcpScopes)
  ) {
    return undefined;
  }
  return {
    ...(tools ? { tools } : {}),
    ...(fileScopes ? { fileScopes } : {}),
    ...(networkScopes ? { networkScopes } : {}),
    ...(mcpScopes ? { mcpScopes } : {}),
  };
}

export function cloneActionMetadataLike(value: unknown): HarnessSkillActionMetadata | undefined {
  if (!isPlainMcpCatalogObject(value)) return undefined;
  if (value.displayName !== undefined && typeof value.displayName !== 'string') return undefined;
  if (value.icon !== undefined && typeof value.icon !== 'string') return undefined;
  const shortcuts = cloneActionCatalogShortcuts(value.shortcuts);
  const inputSchema = cloneMcpCatalogRecord(value.inputSchema);
  const outputSchema = cloneMcpCatalogRecord(value.outputSchema);
  const artifactTypes = cloneActionCatalogStringArray(value.artifactTypes);
  const permissions = cloneActionCatalogPermissions(value.permissions);
  if (
    (value.shortcuts !== undefined && !shortcuts) ||
    (value.inputSchema !== undefined && !inputSchema) ||
    (value.outputSchema !== undefined && !outputSchema) ||
    (value.artifactTypes !== undefined && !artifactTypes) ||
    (value.permissions !== undefined && !permissions)
  ) {
    return undefined;
  }
  return {
    ...(value.displayName ? { displayName: value.displayName } : {}),
    ...(value.icon ? { icon: value.icon } : {}),
    ...(shortcuts ? { shortcuts } : {}),
    ...(inputSchema ? { inputSchema } : {}),
    ...(outputSchema ? { outputSchema } : {}),
    ...(artifactTypes ? { artifactTypes } : {}),
    ...(permissions ? { permissions } : {}),
  };
}

export class ActionCatalogMcpListTimeoutError extends Error {
  constructor() {
    super(`MCP tool catalog did not resolve within ${ACTION_CATALOG_MCP_LIST_TIMEOUT_MS}ms`);
    this.name = 'ActionCatalogMcpListTimeoutError';
  }
}

export function startActionCatalogMcpListWithTimeout<T>(
  run: (abortSignal: AbortSignal) => Promise<T>,
  onTimeout?: (work: Promise<T>) => void,
): {
  pending: Promise<T>;
  work: Promise<T>;
  didTimeout: () => boolean;
} {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let timedOut = false;
  const work = run(controller.signal);
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      const timeoutError = new ActionCatalogMcpListTimeoutError();
      timedOut = true;
      onTimeout?.(work);
      controller.abort(timeoutError);
      reject(timeoutError);
    }, ACTION_CATALOG_MCP_LIST_TIMEOUT_MS);
  });
  const pending = Promise.race([work, timeout]).finally(() => {
    if (timer !== undefined) clearTimeout(timer);
  });
  return {
    pending,
    work,
    didTimeout: () => timedOut,
  };
}
