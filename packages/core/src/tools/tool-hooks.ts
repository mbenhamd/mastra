import type { CoreTool, MastraToolInvocationOptions, ToolHooks } from './types';

type ToolExecute = NonNullable<CoreTool['execute']>;

type ToolHookWrapperState = {
  originalExecute: ToolExecute;
  originalReceiver: CoreTool;
  beforeToolCall: ToolHooks['beforeToolCall'];
  afterToolCall: ToolHooks['afterToolCall'];
  toolName: string;
  agentId: string;
  agentName: string;
};

const TOOL_HOOK_WRAPPER_STATE = Symbol('mastra.toolHookWrapperState');

type HookWrappedExecute = ToolExecute & {
  [TOOL_HOOK_WRAPPER_STATE]?: ToolHookWrapperState;
};

/**
 * Removes the agent hook layer before tools are exposed to input processors.
 * Processors can then decorate an existing executor without closing over an
 * already-hooked function that would fire again after the final surface is
 * wrapped.
 *
 * @internal
 */
export function unwrapToolsFromHooks(tools: Record<string, CoreTool>): Record<string, CoreTool> {
  let changed = false;
  const entries = Object.entries(tools).map(([toolName, tool]) => {
    const execute = tool.execute as HookWrappedExecute | undefined;
    const state = execute?.[TOOL_HOOK_WRAPPER_STATE];
    if (!state) return [toolName, tool] as const;

    changed = true;
    return [
      toolName,
      {
        ...tool,
        execute: state.originalExecute.bind(state.originalReceiver) as ToolExecute,
      },
    ] as const;
  });

  return changed ? Object.fromEntries(entries) : tools;
}

function hasSameHookBinding(
  state: ToolHookWrapperState,
  toolName: string,
  hooks: ToolHooks,
  metadata: { agentId: string; agentName: string },
): boolean {
  return (
    state.beforeToolCall === hooks.beforeToolCall &&
    state.afterToolCall === hooks.afterToolCall &&
    state.toolName === toolName &&
    state.agentId === metadata.agentId &&
    state.agentName === metadata.agentName
  );
}

function wrapToolWithHooks(
  toolName: string,
  tool: CoreTool,
  hooks: ToolHooks,
  metadata: { agentId: string; agentName: string },
): CoreTool {
  if (typeof tool.execute !== 'function') return tool;

  const currentExecute = tool.execute as HookWrappedExecute;
  const currentState = currentExecute[TOOL_HOOK_WRAPPER_STATE];
  if (currentState && hasSameHookBinding(currentState, toolName, hooks, metadata)) {
    return tool;
  }

  // A processor may shallow-clone an existing tool. Keep the marker on the
  // execute function so that clone retains the binding, and unwrap before
  // applying a changed run-level hook instead of nesting hook invocations.
  const originalExecute = currentState?.originalExecute ?? currentExecute;
  const originalReceiver = currentState?.originalReceiver ?? tool;
  const wrappedExecute: HookWrappedExecute = async (input: unknown, context: MastraToolInvocationOptions) => {
    const hookContext = {
      toolName,
      input,
      context,
      metadata,
    };
    const beforeResult = await hooks.beforeToolCall?.(hookContext);
    if (beforeResult?.proceed === false) {
      return beforeResult.output;
    }

    let output: unknown;
    try {
      output = await originalExecute.call(originalReceiver, input, context);
    } catch (error) {
      await hooks.afterToolCall?.({ ...hookContext, output, error });
      throw error;
    }

    await hooks.afterToolCall?.({ ...hookContext, output });
    return output;
  };

  Object.defineProperty(wrappedExecute, TOOL_HOOK_WRAPPER_STATE, {
    value: {
      originalExecute,
      originalReceiver,
      beforeToolCall: hooks.beforeToolCall,
      afterToolCall: hooks.afterToolCall,
      toolName,
      agentId: metadata.agentId,
      agentName: metadata.agentName,
    } satisfies ToolHookWrapperState,
  });

  return {
    ...tool,
    execute: wrappedExecute,
  };
}

/**
 * Applies agent tool hooks to the final executable tool surface.
 *
 * Reapplying the same binding is idempotent, including after a processor
 * shallow-clones a tool object. A changed executor is treated as a replacement
 * and receives the effective hook binding exactly once.
 *
 * @internal
 */
export function wrapToolsWithHooks(
  tools: Record<string, CoreTool>,
  hooks: ToolHooks | undefined,
  metadata: { agentId: string; agentName: string },
): Record<string, CoreTool> {
  if (!hooks?.beforeToolCall && !hooks?.afterToolCall) return tools;

  return Object.fromEntries(
    Object.entries(tools).map(([toolName, tool]) => [toolName, wrapToolWithHooks(toolName, tool, hooks, metadata)]),
  );
}
