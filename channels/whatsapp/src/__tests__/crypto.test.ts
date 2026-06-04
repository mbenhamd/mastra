import { createHmac } from 'node:crypto';
import { describe, it, expect } from 'vitest';

import { verifyWhatsAppSignature, verifyWebhookChallenge } from '../crypto';

const APP_SECRET = 'test-app-secret';

function sign(secret: string, body: string): string {
  return `sha256=${createHmac('sha256', secret).update(Buffer.from(body, 'utf8')).digest('hex')}`;
}

describe('verifyWhatsAppSignature', () => {
  const rawBody = JSON.stringify({ object: 'whatsapp_business_account', entry: [] });

  it('accepts a correctly-signed body (string raw body)', () => {
    const signature = sign(APP_SECRET, rawBody);
    expect(verifyWhatsAppSignature({ appSecret: APP_SECRET, rawBody, signature })).toBe(true);
  });

  it('accepts a correctly-signed body (Uint8Array raw body)', () => {
    const bytes = new TextEncoder().encode(rawBody);
    const signature = sign(APP_SECRET, rawBody);
    expect(verifyWhatsAppSignature({ appSecret: APP_SECRET, rawBody: bytes, signature })).toBe(true);
  });

  it('rejects a tampered body', () => {
    const signature = sign(APP_SECRET, rawBody);
    const tampered = rawBody + ' ';
    expect(verifyWhatsAppSignature({ appSecret: APP_SECRET, rawBody: tampered, signature })).toBe(false);
  });

  it('rejects a signature computed with the wrong app secret', () => {
    const signature = sign('wrong-secret', rawBody);
    expect(verifyWhatsAppSignature({ appSecret: APP_SECRET, rawBody, signature })).toBe(false);
  });

  it('rejects a missing signature header', () => {
    expect(verifyWhatsAppSignature({ appSecret: APP_SECRET, rawBody, signature: undefined })).toBe(false);
  });

  it('rejects a header without the sha256= prefix', () => {
    const hex = createHmac('sha256', APP_SECRET).update(Buffer.from(rawBody, 'utf8')).digest('hex');
    expect(verifyWhatsAppSignature({ appSecret: APP_SECRET, rawBody, signature: hex })).toBe(false);
  });

  it('rejects a wrong-length digest without throwing', () => {
    expect(verifyWhatsAppSignature({ appSecret: APP_SECRET, rawBody, signature: 'sha256=abc' })).toBe(false);
  });
});

describe('verifyWebhookChallenge', () => {
  const verifyToken = 'my-verify-token';

  it('returns the challenge for a matching subscribe handshake', () => {
    const result = verifyWebhookChallenge({
      verifyToken,
      query: { 'hub.mode': 'subscribe', 'hub.verify_token': verifyToken, 'hub.challenge': '1234567890' },
    });
    expect(result).toEqual({ ok: true, challenge: '1234567890' });
  });

  it('handles array-valued query params (first wins)', () => {
    const result = verifyWebhookChallenge({
      verifyToken,
      query: { 'hub.mode': ['subscribe'], 'hub.verify_token': [verifyToken], 'hub.challenge': ['echo'] },
    });
    expect(result).toEqual({ ok: true, challenge: 'echo' });
  });

  it('rejects a mismatched verify token', () => {
    const result = verifyWebhookChallenge({
      verifyToken,
      query: { 'hub.mode': 'subscribe', 'hub.verify_token': 'WRONG', 'hub.challenge': 'x' },
    });
    expect(result).toEqual({ ok: false, reason: 'token_mismatch' });
  });

  it('rejects a non-subscribe mode', () => {
    const result = verifyWebhookChallenge({
      verifyToken,
      query: { 'hub.mode': 'unsubscribe', 'hub.verify_token': verifyToken, 'hub.challenge': 'x' },
    });
    expect(result).toEqual({ ok: false, reason: 'mode_mismatch' });
  });

  it('rejects a missing challenge', () => {
    const result = verifyWebhookChallenge({
      verifyToken,
      query: { 'hub.mode': 'subscribe', 'hub.verify_token': verifyToken },
    });
    expect(result).toEqual({ ok: false, reason: 'missing_challenge' });
  });
});
