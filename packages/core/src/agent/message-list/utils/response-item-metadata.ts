const RESPONSE_ITEM_ID_PROVIDERS = ['openai', 'azure'] as const;

export type ResponseItemIdProvider = (typeof RESPONSE_ITEM_ID_PROVIDERS)[number];

function formatResponseProviderItemKey(provider: ResponseItemIdProvider, itemId: string): string {
  // Keep the provider namespace in the key so matching Azure/OpenAI item IDs
  // cannot merge across provider-specific response streams.
  return `${provider}:${itemId}`;
}

export function getResponseProviderItemId(
  providerMetadata: Record<string, unknown> | undefined,
): { provider: ResponseItemIdProvider; itemId: string } | undefined {
  return getResponseProviderItemIds(providerMetadata)[0];
}

export function getResponseProviderItemKey(providerMetadata: Record<string, unknown> | undefined): string | undefined {
  const item = getResponseProviderItemId(providerMetadata);
  return item ? formatResponseProviderItemKey(item.provider, item.itemId) : undefined;
}

export function getResponseProviderItemIds(
  providerMetadata: Record<string, unknown> | undefined,
): Array<{ provider: ResponseItemIdProvider; itemId: string }> {
  if (!providerMetadata) return [];

  const azureMetadata = providerMetadata.azure as Record<string, unknown> | undefined;
  const azureItemId = azureMetadata?.itemId;
  const openaiMetadata = providerMetadata.openai as Record<string, unknown> | undefined;
  const openaiItemId = openaiMetadata?.itemId;
  if (typeof azureItemId === 'string' && azureItemId === openaiItemId) {
    return [{ provider: 'azure', itemId: azureItemId }];
  }

  // AI SDK Responses metadata is expected to use exactly one provider namespace
  // per part. If a future proxy adds both, keep this deterministic.
  return RESPONSE_ITEM_ID_PROVIDERS.flatMap(provider => {
    const metadata = providerMetadata[provider] as Record<string, unknown> | undefined;
    const itemId = metadata?.itemId;
    return typeof itemId === 'string' ? [{ provider, itemId }] : [];
  });
}

export function getResponseProviderItemKeys(providerMetadata: Record<string, unknown> | undefined): string[] {
  return getResponseProviderItemIds(providerMetadata).map(({ provider, itemId }) =>
    formatResponseProviderItemKey(provider, itemId),
  );
}

/**
 * Drops assistant-content parts whose Responses provider item id already
 * appeared earlier in the prompt (PF-2279). A declined-suspension resume can
 * persist the same provider items twice — once from the suspended segment and
 * once from the resumed segment — and the Azure/OpenAI Responses API rejects
 * any request whose input repeats an item id ("Duplicate item found with id
 * rs_…"), permanently wedging the thread. First occurrence wins; parts
 * without a provider item id are never touched.
 */
export function dedupeResponseProviderItemParts<T extends { role: string; content: unknown }>(messages: T[]): T[] {
  const seen = new Set<string>();
  return messages.map(message => {
    if (message.role !== 'assistant' || !Array.isArray(message.content)) return message;
    let dropped = false;
    const content = (message.content as Array<Record<string, unknown> | null | undefined>).filter(part => {
      const metadata =
        (part?.providerOptions as Record<string, unknown> | undefined) ??
        (part?.providerMetadata as Record<string, unknown> | undefined);
      const keys = getResponseProviderItemKeys(metadata);
      if (keys.length === 0) return true;
      if (keys.some(key => seen.has(key))) {
        dropped = true;
        return false;
      }
      for (const key of keys) seen.add(key);
      return true;
    });
    return dropped ? ({ ...message, content } as T) : message;
  });
}
