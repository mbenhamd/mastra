/**
 * PF-4446: pure route-pattern compiler shared by the fetch compiler.
 *
 * Zero runtime imports by design: pattern compilation is provable inside
 * fail-closed test gates, and its output doubles as `Bun.serve({ routes })`
 * table keys (`:param` segments are valid Bun route syntax as-is).
 */
export type FetchRoutePattern = {
  /** The original `:param` pattern. Bun-compatible as-is. */
  pattern: string;
  match: (pathname: string) => Record<string, string> | null;
};

export function compileFetchRoutePattern(path: string): FetchRoutePattern {
  const names: string[] = [];
  const source = path
    .split('/')
    .map(segment => {
      if (segment.startsWith(':')) {
        names.push(segment.slice(1));
        return '([^/]+)';
      }
      return segment.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    })
    .join('/');
  const expression = new RegExp(`^${source}$`);
  return {
    pattern: path,
    match: (pathname: string) => {
      const hit = expression.exec(pathname);
      if (!hit) return null;
      const params: Record<string, string> = {};
      names.forEach((name, index) => {
        params[name] = decodeURIComponent(hit[index + 1] ?? '');
      });
      return params;
    },
  };
}

/**
 * Canonicalize a `:param` pattern so shape-identical patterns collapse to
 * one `Bun.serve({ routes })` table key (`/agents/:agentId` and
 * `/agents/:id` both become `/agents/:p0`). Static segments pass through
 * untouched. Compiled handlers parse params from the URL themselves, so
 * the canonical names never leak into handler behavior — they only steer
 * Bun's dispatch to the same entry a linear scan would pick.
 */
export function canonicalizeBunRouteKey(path: string): string {
  let index = 0;
  return path
    .split('/')
    .map(segment => (segment.startsWith(':') ? `:p${index++}` : segment))
    .join('/');
}

/** True when the pattern has no `:param` segments. */
export function isStaticRoutePattern(path: string): boolean {
  return !path.split('/').some(segment => segment.startsWith(':'));
}
