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
