import { Ajv } from 'ajv';
import Ajv2020 from 'ajv/dist/2020.js';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { toStandardSchema } from './json-schema';
import { InterpretedJsonSchema, isCanonicalRootId, startInterpreterLoad } from './json-schema-interpreter';

// The interpreter must accept nothing that fork main's Ajv refuses where Ajv can compile. This
// process allows code generation, so `toStandardSchema` validates with Ajv exactly as fork main
// does, and each generated plain-JSON schema and value goes through both. The generator is
// seeded and bounded, so a failure names a schema that reproduces.

// About 6 s per dialect: Ajv compiles every generated schema.
const SEEDS = [101, 202];
const SCHEMAS_PER_SEED = 50;

function random(seed: number) {
  let state = seed | 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

function generator(rnd: () => number, dialect: 'draft-07' | '2020-12') {
  const pick = <T>(list: readonly T[]): T => list[Math.floor(rnd() * list.length)]!;
  const chance = (p: number) => rnd() < p;
  // Most schemas use only listed keywords and names, so values are compared under compiled
  // schemas; the rest mix in what the list refuses, so a refusal that should hold is exercised.
  let listedOnly = true;
  const LISTED_PROPS = ['p', 'q', 'r', '', 'type'];
  const PROPS = [...LISTED_PROPS, 'toString', 'constructor', 'valueOf', 'hasOwnProperty', '__proto__'];
  const odd = (): Json => pick<Json>(['x', 5, -1, 1.5, true, null, [], {}, ['a', 'a'], [5], 'string']);
  const lit = (): Json =>
    pick<Json>([5, 'a', null, true, 0, 1.5, {}, { p: 5 }, [1, 2], { p: 'a', q: null }, [], { valueOf: 1 }, 6]);
  const listedLeaves: Array<() => Json> = [
    () => ({ type: pick(['string', 'number', 'integer', 'boolean', 'null', 'object', 'array']) }),
    () => ({ type: ['string', 'null'] }),
    () => ({ type: 'number', minimum: pick([5, 0, -1, 5.5]) }),
    () => ({ type: 'number', maximum: pick([5, 0, 6]) }),
    () => ({ exclusiveMinimum: pick([5, 0]) }),
    () => ({ exclusiveMaximum: pick([5, 6]) }),
    () => ({ type: 'string', minLength: pick([1, 2, 0]) }),
    () => ({ maxLength: pick([1, 0]) }),
    () => ({ pattern: pick(['^a', 'a$', '^[0-9]+$', '\\d']) }),
    () => ({ enum: pick<Json>([['a', 5, null], [{ p: 5 }, 'a'], [[1, 2]], ['a'], [0, 0], ['a', 'a']]) }),
    () => ({ const: lit() }),
    () => ({ type: 'string', nullable: true }),
    () => ({ format: pick(['email', 'date-time', 'bogus']), type: 'string' }),
    () => ({ title: 't', description: 'd', default: lit(), examples: [lit()] }),
    () => ({ example: lit(), 'x-a': { $ref: '#/nowhere' }, discriminator: { propertyName: 'p' } }),
    () => ({ minItems: 1, maxItems: 2, type: 'array' }),
    () => ({ minProperties: 1, maxProperties: 2 }),
    () => ({ required: [pick(['p', ''])] }),
    () => ({ properties: { p: { $schema: 'https://json-schema.org/draft/2019-09/schema', minimum: 1 } } }),
    () => ({}),
    () => true,
    () => false,
    ...(dialect === '2020-12'
      ? [
          () => ({ type: 'array', prefixItems: [{ type: 'string' }, { type: 'integer' }], items: false }),
          () => ({ dependentRequired: { p: ['q'] } }),
          () => ({ $comment: 'c' }),
        ]
      : [() => ({ type: 'array', items: [{ type: 'string' }, { type: 'integer' }], additionalItems: false })]),
  ];
  const refusedLeaves: Array<() => Json> = [
    () => ({ multipleOf: pick([0.01, 0.5, 2]) }),
    () => ({ nullable: true }),
    () => ({ uniqueItems: true }),
    () => ({ type: 'array', prefixItems: [{ type: 'string' }, { type: 'integer' }], items: false }),
    () => ({ type: 'array', items: [{ type: 'string' }, { type: 'integer' }], additionalItems: false }),
    () => ({ dependentRequired: { p: ['q'] } }),
    () => ({ dependencies: { p: ['q'] } }),
    () => ({ contains: { type: 'integer' } }),
    () => ({ 'x-a': pick<Json>([{ $anchor: 'a' }, { $id: 'https://example.test/s' }]) }),
    () => ({ required: [pick(['hasOwnProperty', 'toString'])] }),
    () => ({ $schema: pick<Json>(['http://json-schema.org/draft-07/schema#', 'junk', 5]), required: ['p'] }),
  ];
  const leaf = (): Json => (!listedOnly && chance(0.3) ? pick(refusedLeaves) : pick(listedLeaves))();
  const LISTED = [
    'type',
    'minimum',
    'exclusiveMinimum',
    'minLength',
    'enum',
    'required',
    'nullable',
    'items',
    'properties',
    'anyOf',
    'format',
    '$defs',
  ];
  const gen = (depth: number, refs: string[]): Json => {
    if (depth <= 0 || chance(0.3)) {
      if (chance(0.25)) return { $ref: pick(refs) };
      const l = leaf();
      if (!listedOnly && chance(0.1) && typeof l === 'object' && l !== null && !Array.isArray(l))
        l[pick(LISTED)] = odd();
      return l;
    }
    const s: Record<string, Json> = {};
    for (let i = 0, n = 1 + Math.floor(rnd() * 2); i < n; i++) {
      switch (pick(['object', 'object', 'array', 'allOf', 'anyOf', 'oneOf', 'ref', 'leaf', 'names', 'not'])) {
        case 'object': {
          if (chance(0.7)) s.type = 'object';
          const properties: Record<string, Json> = {};
          for (let j = 0, m = 1 + Math.floor(rnd() * 3); j < m; j++) {
            properties[pick(listedOnly ? LISTED_PROPS : PROPS)] = gen(depth - 1, refs);
          }
          s.properties = properties;
          if (chance(0.4)) s.required = [pick(Object.keys(properties))];
          if (chance(0.4)) s.additionalProperties = chance(0.5) ? false : gen(depth - 1, refs);
          break;
        }
        case 'array':
          if (chance(0.7)) s.type = 'array';
          s.items = gen(depth - 1, refs);
          break;
        case 'allOf':
        case 'anyOf':
        case 'oneOf':
          s[pick(['allOf', 'anyOf', 'oneOf'])] = [gen(depth - 1, refs), gen(depth - 1, refs)];
          break;
        case 'ref':
          s.$ref = pick(refs);
          break;
        case 'leaf': {
          const l = leaf();
          if (typeof l === 'object' && l !== null && !Array.isArray(l)) Object.assign(s, l);
          break;
        }
        case 'names':
          s.propertyNames = pick<Json>([{ pattern: '^[a-z]' }, { enum: ['p', 'q', 'toString'] }]);
          break;
        case 'not':
          if (!listedOnly) s.not = gen(depth - 1, refs);
          break;
      }
    }
    return s;
  };
  return (): Json => {
    listedOnly = chance(0.7);
    const defKey = chance(0.5) ? '$defs' : 'definitions';
    const refs = ['#', `#/${defKey}/A`, `#/${defKey}/B`];
    if (!listedOnly && chance(0.3)) refs.push(`#/${defKey}/missing`, '#/properties/p');
    const generated = gen(2, refs);
    const root: Record<string, Json> =
      typeof generated === 'object' && generated !== null && !Array.isArray(generated) ? generated : {};
    delete root.$ref;
    root.properties = {
      ...((root.properties as Record<string, Json> | undefined) ?? {}),
      p: gen(2, refs),
      q: gen(1, refs),
    };
    root[defKey] = { A: gen(2, refs), B: gen(1, refs) };
    if (dialect === '2020-12') {
      root.$schema = 'https://json-schema.org/draft/2020-12/schema';
      if (chance(0.2)) {
        root.$id = listedOnly
          ? pick(['https://example.test/s', 'https://example.test/a/b.json'])
          : pick(['urn:example:x', 'tree', 'https://example.test/s#f']);
      }
    } else if (chance(0.3)) {
      root.$schema = 'http://json-schema.org/draft-07/schema#';
    }
    return root;
  };
}

const VALUES = (): unknown[] => [
  null,
  5,
  6,
  4,
  5.5,
  0.07,
  1e21,
  'a',
  '',
  'ab',
  true,
  {},
  [],
  [5],
  [6, 6],
  [null],
  ['a', 1],
  ['a', 1, true],
  [1, 'a'],
  { p: 5 },
  { p: 6 },
  { p: null },
  { p: 'a', q: null },
  { q: 5 },
  { r: 1 },
  { p: { p: 6 } },
  { p: [6] },
  { p: 5, q: 'a', r: 1 },
  { p: undefined },
  { p: 5, r: undefined },
  [{ p: 6 }, { p: 6 }],
  ['__proto__', '__proto__'],
  JSON.parse('{"toString":1}'),
  JSON.parse('{"constructor":{}}'),
  JSON.parse('{"valueOf":1}'),
  JSON.parse('{"__proto__":1}'),
  JSON.parse('{"hasOwnProperty":1}'),
  JSON.parse('{"p":{"__proto__":{"q":1}}}'),
];

const outcome = (result: { issues?: readonly unknown[] } | Promise<unknown>) => {
  if (result instanceof Promise) throw new Error('validation was asynchronous');
  return result.issues ? 'refuse' : 'accept';
};

describe('JSON Schema interpreter parity with fork main Ajv', () => {
  beforeAll(async () => {
    // Ajv warns once per unknown format it ignores; both validators ignore every format.
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    await startInterpreterLoad();
  });
  afterAll(() => {
    vi.restoreAllMocks();
  });

  for (const dialect of ['draft-07', '2020-12'] as const) {
    it(`accepts no ${dialect} value that Ajv refuses (seeds ${SEEDS.join(', ')})`, () => {
      const accepted: string[] = [];
      let compared = 0;
      let bothAccepted = 0;
      let compiled = 0;
      for (const seed of SEEDS) {
        const nextSchema = generator(random(seed), dialect);
        for (let i = 0; i < SCHEMAS_PER_SEED; i++) {
          const text = JSON.stringify(nextSchema());
          const ajv = toStandardSchema(JSON.parse(text));
          const interpreted = new InterpretedJsonSchema(JSON.parse(text));
          if (interpreted.refusal() === undefined) compiled++;
          for (const value of VALUES()) {
            compared++;
            const ajvOutcome = outcome(ajv['~standard'].validate(value));
            if (outcome(interpreted.validate(value)) !== 'accept') continue;
            if (ajvOutcome === 'accept') bothAccepted++;
            else if (accepted.length < 5) accepted.push(`${text} with ${JSON.stringify(value)}`);
          }
        }
      }

      expect(accepted).toEqual([]);
      expect(compared).toBe(SEEDS.length * SCHEMAS_PER_SEED * VALUES().length);
      // The comparison is not vacuous: listed schemas are compiled and accept values.
      expect(compiled, 'schemas the interpreter compiled').toBeGreaterThanOrEqual(
        SEEDS.length * SCHEMAS_PER_SEED * 0.3,
      );
      expect(bothAccepted, 'values both accepted').toBeGreaterThanOrEqual(compared * 0.05);
    }, 60_000);
  }

  it('resolves every canonical root $id with Ajv2020 as the interpreter reads it', () => {
    const rnd = random(303);
    const pick = <T>(list: readonly T[]): T => list[Math.floor(rnd() * list.length)]!;
    const label = () =>
      Array.from({ length: 1 + Math.floor(rnd() * 8) }, () => pick([...'abcxyz019-']))
        .join('')
        .replace(/^-|-$/g, 'a');
    const segment = () => Array.from({ length: 1 + Math.floor(rnd() * 6) }, () => pick([...'aZ09._~-'])).join('');
    let checked = 0;
    for (let i = 0; i < 400; i++) {
      const host = Array.from({ length: 1 + Math.floor(rnd() * 3) }, label).join('.');
      const port = rnd() < 0.3 ? `:${pick(['8080', '1', '65535', '3000'])}` : '';
      const path = Array.from({ length: Math.floor(rnd() * 4) }, () => `/${segment()}`).join('');
      const id = `${pick(['http', 'https'])}://${host}${port}${path}${rnd() < 0.3 ? '/' : ''}`;
      if (!isCanonicalRootId(id)) continue;
      checked++;
      for (const AjvClass of [Ajv, Ajv2020]) {
        const ajv = new AjvClass({ allErrors: true, strict: false });
        const validate = ajv.compile({ $id: id, $defs: { A: { type: 'string' } }, $ref: '#/$defs/A' });
        expect([validate('a'), validate(1)], id).toEqual([true, false]);
        expect(ajv.getSchema(id), id).toBeDefined();
      }
    }
    expect(checked).toBeGreaterThan(200);
  });
});
