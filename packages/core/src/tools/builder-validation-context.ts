const builderValidatedContexts = new WeakSet<object>();
const builderValidatedSuspendContexts = new WeakSet<object>();

export function markBuilderValidatedInput(context: object): void {
  builderValidatedContexts.add(context);
}

export function consumeBuilderValidatedInput(context: unknown): boolean {
  if (typeof context !== 'object' || context === null || !builderValidatedContexts.has(context)) {
    return false;
  }

  builderValidatedContexts.delete(context);
  return true;
}

export function markBuilderValidatedSuspend(context: object): void {
  builderValidatedSuspendContexts.add(context);
}

export function consumeBuilderValidatedSuspend(context: unknown): boolean {
  if (typeof context !== 'object' || context === null || !builderValidatedSuspendContexts.has(context)) {
    return false;
  }

  builderValidatedSuspendContexts.delete(context);
  return true;
}
