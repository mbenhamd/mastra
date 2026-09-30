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
import type { ProcessorSpanPayloadPhase } from '../observability';
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

/**
 * Resolve a processor's declared span attributes for this phase, with the phase
 * itself recorded alongside them.
 *
 * The phase is applied last so a declaration cannot misreport which phase ran:
 * readers narrow a processor span's payloads on this attribute, and a processor
 * naming itself into another phase would hand them the wrong shape.
 */
export function resolveProcessorSpanAttributes(
  processor: Pick<Processor, 'spanAttributes'> | undefined,
  phase: ProcessorSpanPayloadPhase,
) {
  try {
    const declared = processor?.spanAttributes;
    // The declaration callback keeps seeing the coarser phase it was written
    // against; only the recorded attribute distinguishes the two output hooks.
    const declarationPhase = toProcessorSpanPhase(phase);
    const resolved = typeof declared === 'function' ? declared(declarationPhase) : declared;
    // Materialize inside the guard so hostile getters/proxies cannot defer a
    // throw until the caller spreads the attributes into a span declaration.
    const base = resolved && typeof resolved === 'object' ? { ...resolved } : {};
    // The phase is applied last (and even when nothing is declared) so a
    // declaration cannot misreport which phase ran: readers narrow a processor
    // span's payloads on this attribute.
    return { ...base, processorPhase: phase };
  } catch {
    // Observability metadata is advisory. It must never bypass a processor
    // whose body may enforce filtering, moderation, or persistence policy.
    return { processorPhase: phase };
  }
}
