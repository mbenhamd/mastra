/**
 * Incremental decoder for the `summary` string of a streaming
 * `report_subagent_outcome` tool input (PF-5620).
 *
 * A subagent's answer is the `summary` argument of its framework-owned outcome
 * report, so the provider streams it as tool-input JSON, not as assistant
 * text. This decoder consumes those input deltas in order, across arbitrary
 * chunk boundaries, and returns only the decoded characters of the top-level
 * `summary` string value. JSON escapes are decoded exactly as `JSON.parse`
 * decodes them. A UTF-16 high surrogate is held back until its low half
 * arrives, so no delta ends in half a code point.
 *
 * Only a key of the top-level object counts: keys of nested objects and string
 * values that merely contain the text "summary" are skipped. Only the first
 * top-level `summary` is streamed. Emission stops for good once the decoded
 * summary would exceed `maxBytes` UTF-8 bytes (such a summary is never a
 * direct answer), or once the input stops being a JSON object.
 */

type DecoderState =
  | 'start' // before the top-level `{`
  | 'between' // outside any string, inside the document
  | 'string' // inside a string literal
  | 'escape' // just after a backslash inside a string
  | 'unicode' // inside a `\uXXXX` escape
  | 'done'; // nothing more is streamed

const SIMPLE_ESCAPES: Record<string, string> = {
  '"': '"',
  '\\': '\\',
  '/': '/',
  b: '\b',
  f: '\f',
  n: '\n',
  r: '\r',
  t: '\t',
};

export class SubagentReportSummaryDecoder {
  #state: DecoderState = 'start';
  /** Nesting depth outside strings: 1 inside the top-level object. */
  #depth = 0;
  /** At depth 1, whether the next string is a key. */
  #expectKey = false;
  /** At depth 1, whether the next value belongs to the `summary` key. */
  #summaryValueNext = false;
  #summaryStreamed = false;
  /** What the current string is: a top-level key, the summary, or other. */
  #stringRole: 'key' | 'summary' | 'other' = 'other';
  #key = '';
  #unicodeHex = '';
  /** A high surrogate waiting for its low half. */
  #heldHighSurrogate = '';
  #emittedBytes = 0;
  readonly #maxBytes: number;
  readonly #encoder = new TextEncoder();

  constructor(maxBytes: number) {
    this.#maxBytes = maxBytes;
  }

  /** Feed the next tool-input chunk; returns the decoded summary characters it completed. */
  push(chunk: string): string {
    let out = '';
    for (let i = 0; i < chunk.length && this.#state !== 'done'; i += 1) {
      const ch = chunk[i]!;
      switch (this.#state) {
        case 'start':
          if (isJsonWhitespace(ch)) break;
          if (ch !== '{') {
            this.#state = 'done';
            break;
          }
          this.#depth = 1;
          this.#expectKey = true;
          this.#state = 'between';
          break;
        case 'between':
          out += this.#between(ch);
          break;
        case 'string':
          if (ch === '"') {
            out += this.#endString();
          } else if (ch === '\\') {
            this.#state = 'escape';
          } else {
            out += this.#char(ch);
          }
          break;
        case 'escape':
          if (ch === 'u') {
            this.#unicodeHex = '';
            this.#state = 'unicode';
          } else {
            const decoded = SIMPLE_ESCAPES[ch];
            if (decoded === undefined) {
              this.#state = 'done';
              break;
            }
            this.#state = 'string';
            out += this.#char(decoded);
          }
          break;
        case 'unicode':
          if (!/[0-9a-fA-F]/.test(ch)) {
            this.#state = 'done';
            break;
          }
          this.#unicodeHex += ch;
          if (this.#unicodeHex.length === 4) {
            this.#state = 'string';
            out += this.#char(String.fromCharCode(parseInt(this.#unicodeHex, 16)));
          }
          break;
      }
    }
    return this.#accept(out);
  }

  #between(ch: string): string {
    if (isJsonWhitespace(ch)) return '';
    if (ch === '"') {
      if (this.#depth === 1 && this.#expectKey) {
        this.#stringRole = 'key';
        this.#key = '';
      } else if (this.#depth === 1 && this.#summaryValueNext) {
        this.#stringRole = 'summary';
        this.#summaryValueNext = false;
      } else {
        this.#stringRole = 'other';
      }
      this.#state = 'string';
      return '';
    }
    if (this.#depth === 1 && ch !== ':') this.#summaryValueNext = false;
    if (ch === '{' || ch === '[') {
      this.#depth += 1;
    } else if (ch === '}' || ch === ']') {
      this.#depth -= 1;
      if (this.#depth <= 0) this.#state = 'done';
    } else if (ch === ',' && this.#depth === 1) {
      this.#expectKey = true;
    }
    // ':' and scalar literals need no tracking: the value role was fixed when
    // its key ended.
    return '';
  }

  #endString(): string {
    this.#state = 'between';
    const role = this.#stringRole;
    this.#stringRole = 'other';
    if (role === 'key') {
      this.#expectKey = false;
      this.#summaryValueNext = this.#key === 'summary' && !this.#summaryStreamed;
      return '';
    }
    if (role === 'summary') {
      this.#summaryStreamed = true;
      // A lone high surrogate at the end decodes to itself, as in JSON.parse.
      const held = this.#heldHighSurrogate;
      this.#heldHighSurrogate = '';
      return held;
    }
    return '';
  }

  /** One decoded UTF-16 code unit of the current string. */
  #char(unit: string): string {
    if (this.#stringRole === 'key') {
      this.#key += unit;
      return '';
    }
    if (this.#stringRole !== 'summary') return '';
    const code = unit.charCodeAt(0);
    let out = this.#heldHighSurrogate;
    this.#heldHighSurrogate = '';
    if (code >= 0xd800 && code <= 0xdbff) {
      this.#heldHighSurrogate = unit;
      return out;
    }
    out += unit;
    return out;
  }

  #accept(out: string): string {
    if (out.length === 0) return '';
    this.#emittedBytes += this.#encoder.encode(out).byteLength;
    if (this.#emittedBytes > this.#maxBytes) {
      this.#state = 'done';
      return '';
    }
    return out;
  }
}

function isJsonWhitespace(ch: string): boolean {
  return ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r';
}
