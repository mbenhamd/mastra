/**
 * JSON-Schema dialect checks: every schema embedded in the definition must
 * belong to the admitted 2020-12 subset `jsonSchemaToZod` can convert
 * losslessly. Covers the four top-level schemas plus each `agent.outputSchema`
 * reachable through containers.
 */
import { forEachSingleStepEntryWithPath } from '../graph';
import { ADMITTED_JSON_SCHEMA_DIALECT, validateStorableJsonSchema } from '../json-schema-to-zod';
import type { JsonSchema, JsonSchemaAdmissionIssue } from '../json-schema-to-zod';
import type { WorkflowValidationInput, WorkflowValidationIssue } from './types';

export function collectWorkflowJsonSchemaAdmissionIssues(def: WorkflowValidationInput): JsonSchemaAdmissionIssue[] {
  const issues: JsonSchemaAdmissionIssue[] = [];
  const check = (schema: JsonSchema | undefined, path: string): void => {
    const result = validateStorableJsonSchema(schema);
    if (result.ok) return;
    for (const issue of result.issues) {
      issues.push({ ...issue, path });
    }
  };
  check(def.inputSchema, 'inputSchema');
  check(def.outputSchema, 'outputSchema');
  if (def.stateSchema !== undefined) check(def.stateSchema, 'stateSchema');
  if (def.requestContextSchema !== undefined) check(def.requestContextSchema, 'requestContextSchema');
  forEachSingleStepEntryWithPath(def.graph, (entry, path) => {
    if (entry.type === 'agent' && entry.outputSchema !== undefined) {
      check(entry.outputSchema, `${path}.outputSchema`);
    }
  });
  return issues;
}

export function validateWorkflowSchemas(def: WorkflowValidationInput): WorkflowValidationIssue[] {
  const admissions = collectWorkflowJsonSchemaAdmissionIssues(def);
  if (admissions.length === 0) return [];

  const byPath = new Map<string, JsonSchemaAdmissionIssue[]>();
  for (const issue of admissions) {
    const path = issue.path ?? '';
    const group = byPath.get(path) ?? [];
    group.push(issue);
    byPath.set(path, group);
  }

  const issues: WorkflowValidationIssue[] = [];
  for (const [path, group] of byPath) {
    const stepId = path.startsWith('graph') && path.endsWith('.outputSchema') ? findAgentStepId(def, path) : undefined;
    const label = stepId ? `step "${stepId}" outputSchema` : path;
    issues.push({
      code: 'unsupported-schema-keyword',
      path,
      message: `${label} uses JSON Schema features outside the admitted ${ADMITTED_JSON_SCHEMA_DIALECT} dialect: ${group
        .map(issue => `${issue.pointer} (${issue.keyword})`)
        .join(', ')}.`,
    });
  }
  return issues;
}

function findAgentStepId(def: WorkflowValidationInput, outputSchemaPath: string): string | undefined {
  let found: string | undefined;
  forEachSingleStepEntryWithPath(def.graph, (entry, path) => {
    if (entry.type === 'agent' && `${path}.outputSchema` === outputSchemaPath) {
      found = entry.id;
    }
  });
  return found;
}
