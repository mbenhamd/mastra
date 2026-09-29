export const PG_UNSAFE_JSON_UNICODE_ESCAPE_PATTERN = String.raw`(?<!\\)((?:\\\\)*)(?:(\\u[Dd][89AaBb][0-9A-Fa-f]{2}\\u[Dd][CcDdEeFf][0-9A-Fa-f]{2})|\\u(?:0000|[Dd][89A-Fa-f][0-9A-Fa-f]{2}))`;
const PG_UNSAFE_JSON_UNICODE_ESCAPE_RE = new RegExp(PG_UNSAFE_JSON_UNICODE_ESCAPE_PATTERN, 'g');

/**
 * Sanitizes JSON string for PostgreSQL jsonb:
 * - Removes problematic Unicode sequences:
 *   - \u0000 (null character) - causes error 22P05 "unsupported Unicode escape sequence"
 *   - \uD800-\uDFFF (unpaired surrogates) - causes "Unicode low surrogate must follow a high surrogate"
 * - Preserves escaped-backslash pairs and valid high+low surrogate pairs.
 * - Escapes any remaining invalid JSON escape sequences (e.g. \v, \k, \-)
 */
export function sanitizeJsonForPg(jsonString: string): string {
  return (
    jsonString
      // Preserve each complete escaped-backslash pair. For an odd run, remove
      // only the final unsafe escape; valid high+low surrogate pairs survive.
      .replace(PG_UNSAFE_JSON_UNICODE_ESCAPE_RE, '$1$2')
      // Fix any remaining invalid JSON escape sequences safely without rewriting
      // already-escaped backslashes. Running this AFTER surrogate removal ensures that
      // characters newly exposed by the removal (e.g. a hyphen left after \\ud800-\\udfff)
      // are also caught and escaped.
      .replace(/(^|[^\\])(\\(?!["\\/bfnrtu]))/g, '$1\\\\')
  );
}

const repair = (value: string): string =>
  value
    .replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, '\uFFFD')
    .replaceAll('\0', '');

function repairJson(value: unknown): unknown {
  if (typeof value === 'string') return repair(value);
  if (Array.isArray(value)) return value.map(repairJson);
  if (value && typeof value === 'object') {
    const repaired: Record<string, unknown> = Object.create(null);
    for (const [key, entry] of Object.entries(value)) {
      const repairedKey = repair(key);
      if (Object.hasOwn(repaired, repairedKey)) {
        throw new Error(`JSON keys collide after PostgreSQL normalization: ${repairedKey}`);
      }
      repaired[repairedKey] = repairJson(entry);
    }
    return repaired;
  }
  return value;
}

/** Serialize JSON values without emitting Unicode sequences PostgreSQL rejects. */
export function toPgJson(value: unknown): string {
  const json = JSON.stringify(value);
  if (json === undefined || !/\\u(?:0000|[dD][89a-fA-F][0-9a-fA-F]{2})/.test(json)) return json;
  return JSON.stringify(repairJson(JSON.parse(json)));
}
