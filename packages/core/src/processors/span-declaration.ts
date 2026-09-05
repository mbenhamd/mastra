/**
 * Resolution of a processor's declared span identity.
 *
 * A processor may declare the span type, name and attributes it should be
 * traced as (see `Processor.spanType`). Spans for processors are created in two
 * places — the legacy `ProcessorRunner` and the processor-workflow executor —
 * so the resolution lives here and both call it. A declaration honoured by only
 * one executor would apply or not depending on how the agent happened to run
 * its processors.
 */
import type { Processor, ProcessorSpanPhase } from './index';

/**
 * Phase names used by the processor-workflow executor, mapped onto
 * `ProcessorSpanPhase`. The executor distinguishes `outputStream` from
 * `outputResult`; both are the output phase as far as a declaration is
 * concerned, matching how they share one entity type.
 */
const WORKFLOW_PHASE_TO_SPAN_PHASE: Record<string, ProcessorSpanPhase> = {
  input: 'input',
  inputStep: 'inputStep',
  llmRequest: 'llmRequest',
  llmResponse: 'llmResponse',
  outputStream: 'output',
  outputResult: 'output',
  outputStep: 'outputStep',
  toolResult: 'toolResult',
  requestError: 'requestError',
};

/** Map a processor-workflow phase string onto the declaration phase. */
export function toProcessorSpanPhase(phase: string): ProcessorSpanPhase {
  return WORKFLOW_PHASE_TO_SPAN_PHASE[phase] ?? 'output';
}

/** The span type a processor declared, or `undefined` to use the default. */
export function resolveProcessorSpanType(processor: Pick<Processor, 'spanType'>) {
  try {
    return processor.spanType;
  } catch {
    return undefined;
  }
}

/**
 * Resolve a processor's declared span name for the phase the span is being
 * created in, falling back to the caller's default label.
 */
export function resolveProcessorSpanName(
  processor: Pick<Processor, 'spanName'>,
  phase: ProcessorSpanPhase,
  fallback: string,
): string {
  try {
    const declared = processor.spanName;
    const resolved = typeof declared === 'function' ? declared(phase) : declared;
    return typeof resolved === 'string' ? resolved : fallback;
  } catch {
    // Observability metadata is advisory. It must never bypass a processor
    // whose body may enforce filtering, moderation, or persistence policy.
    return fallback;
  }
}

/** Resolve a processor's declared span attributes for this phase. */
export function resolveProcessorSpanAttributes(
  processor: Pick<Processor, 'spanAttributes'>,
  phase: ProcessorSpanPhase,
) {
  try {
    const declared = processor.spanAttributes;
    const resolved = typeof declared === 'function' ? declared(phase) : declared;
    if (!resolved || typeof resolved !== 'object') return undefined;
    // Materialize inside the guard so hostile getters/proxies cannot defer a
    // throw until the caller spreads the attributes into a span declaration.
    return { ...resolved };
  } catch {
    return undefined;
  }
}
