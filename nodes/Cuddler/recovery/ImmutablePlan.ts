/** Internal-only plan primitive. Never return an envelope in node output/logs. */
export const CONTRACT = 'cuddler.cloud-recovery/1';
export const ENVELOPE_VERSION = 'cuddler.n8n.hmac-json/1';
export const ORIGIN = 'https://api.cuddler.ai';
export type Media = 'video' | 'image' | 'speech' | 'music';
const PATHS: Record<Media, string> = {
  video: '/v1/videos', image: '/v1/images', speech: '/v1/speech', music: '/v1/music',
};
const text = new TextEncoder();
const MAX_BYTES = 64 * 1024;
const record = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);
const has = (value: object, key: string) => Object.prototype.hasOwnProperty.call(value, key);

export interface Scope {
  workflowId: string;
  nodeId: string;
  businessBatchId: string;
  stableItemId: string;
}

interface Payload {
  contract: typeof CONTRACT;
  envelopeVersion: typeof ENVELOPE_VERSION;
  origin: typeof ORIGIN;
  method: 'POST';
  path: string;
  media: Media;
  scope: Scope;
  slot: string;
  operationId: string;
  idempotencyKey: string;
  credentialBinding: string;
  requestJson: string;
  budget: { present: boolean; decimal?: string };
}

export interface Envelope { payload: Payload; tag: string }

/** Sorted object keys; arrays, absent/explicit fields and decimal strings unchanged. */
export function canonical(value: unknown): string {
  const seen = new Set<object>();
  let count = 0;
  const visit = (current: unknown, depth: number): string => {
    if (++count > 10000 || depth > 32) throw new Error('Invalid immutable JSON');
    if (current === null || typeof current === 'boolean' || typeof current === 'string') return JSON.stringify(current);
    if (typeof current === 'number' && Number.isFinite(current) && !Object.is(current, -0)) return JSON.stringify(current);
    if (typeof current !== 'object' || current === null || seen.has(current)) throw new Error('Invalid immutable JSON');
    seen.add(current);
    let result: string;
    if (Array.isArray(current)) {
      if (Object.keys(current).length !== current.length ||
          Array.from({ length: current.length }, (_, index) => index).some((index) => !has(current, String(index)))) throw new Error('Invalid immutable JSON');
      result = '[' + current.map((entry) => visit(entry, depth + 1)).join(',') + ']';
    } else {
      const proto = Object.getPrototypeOf(current);
      if (proto !== Object.prototype && proto !== null) throw new Error('Invalid immutable JSON');
      const fields = Object.keys(current).sort();
      if (fields.some((key) => ['__proto__', 'constructor', 'prototype'].includes(key))) throw new Error('Invalid immutable JSON');
      result = '{' + fields.map((key) => JSON.stringify(key) + ':' + visit((current as Record<string, unknown>)[key], depth + 1)).join(',') + '}';
    }
    seen.delete(current);
    return result;
  };
  const result = visit(value, 0);
  if (text.encode(result).byteLength > MAX_BYTES) throw new Error('Immutable JSON is too large');
  return result;
}

function validateScope(scope: Scope): void {
  for (const value of [scope.workflowId, scope.nodeId, scope.businessBatchId, scope.stableItemId]) {
    if (typeof value !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(value)) throw new Error('Stable operation identity is required');
  }
  if (Object.keys(scope).sort().join(',') !== 'businessBatchId,nodeId,stableItemId,workflowId') throw new Error('Invalid stable operation scope');
}

function validateKey(apiKey: string): void {
  if (typeof apiKey !== 'string' || !apiKey || apiKey.length > 4096 || apiKey.trim() !== apiKey || /[\r\n]/.test(apiKey)) throw new Error('Protected credential is unavailable');
}

const hex = (value: ArrayBuffer) => Array.from(new Uint8Array(value), (byte) => byte.toString(16).padStart(2, '0')).join('');
function unhex(value: string): Uint8Array<ArrayBuffer> {
  if (!/^[a-f0-9]{64}$/.test(value)) throw new Error('Invalid authenticated plan');
  return Uint8Array.from(value.match(/../g)!, (byte) => Number.parseInt(byte, 16));
}

async function hmac(key: CryptoKey, input: string): Promise<string> {
  return hex(await crypto.subtle.sign('HMAC', key, text.encode(input)));
}

async function keys(apiKey: string): Promise<{ signer: CryptoKey; binding: string }> {
  validateKey(apiKey);
  const root = await crypto.subtle.importKey('raw', text.encode(apiKey), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const derive = async (label: string) => await crypto.subtle.importKey('raw', unhex(await hmac(root, `${ENVELOPE_VERSION}/${label}`)), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify']);
  return { signer: await derive('plan-signing'), binding: await hmac(root, `${ENVELOPE_VERSION}/credential-binding`) };
}

export async function stableSlot(scope: Scope): Promise<string> {
  validateScope(scope);
  // Key rotation, media changes and request edits must still find the original
  // ledger entry. The real native helper's project-scoped table supplies tenant
  // isolation; immutable plan authentication supplies credential binding.
  return hex(await crypto.subtle.digest('SHA-256', text.encode(canonical({
    contract: CONTRACT, origin: ORIGIN, operation: 'create', ...scope,
  }))));
}

function request(body: unknown): { json: string; budget: Payload['budget'] } {
  if (!record(body) || typeof body.model !== 'string' || !body.model) throw new Error('A complete model request is required');
  const json = canonical(body);
  const present = has(body, 'max_usd');
  if (present && (typeof body.max_usd !== 'string' || !/^(?:0|[1-9]\d*)(?:\.\d{1,6})?$/.test(body.max_usd))) throw new Error('Budget must be an exact decimal string');
  return { json, budget: present ? { present, decimal: body.max_usd as string } : { present } };
}

export async function prepare(
  media: Media, body: unknown, scope: Scope, apiKey: string,
  hostMode: string, intent: 'new' | 'resume',
): Promise<Envelope> {
  if (intent !== 'new' || hostMode === 'retry') throw new Error('Resume requires the original persistent plan');
  if (!has(PATHS, media)) throw new Error('Unsupported media type');
  const frozenScope = JSON.parse(canonical(scope)) as Scope;
  validateScope(frozenScope);
  const frozen = request(body);
  const key = await keys(apiKey);
  const operationId = crypto.randomUUID();
  const payload: Payload = {
    contract: CONTRACT, envelopeVersion: ENVELOPE_VERSION, origin: ORIGIN,
    method: 'POST', path: PATHS[media], media, scope: frozenScope,
    slot: await stableSlot(frozenScope),
    operationId, idempotencyKey: `n8n-${operationId}`,
    credentialBinding: key.binding, requestJson: frozen.json, budget: frozen.budget,
  };
  return { payload, tag: await hmac(key.signer, canonical(payload)) };
}

/** Verifies the entire envelope plus caller's original approved request/scope. */
export async function verify(
  envelope: unknown, media: Media, approvedBody: unknown, scope: Scope, apiKey: string,
): Promise<Envelope> {
  // All caller-owned inputs are detached before the first asynchronous step.
  // Authentication and returned bytes must refer to the same exact snapshot.
  const snapshot: unknown = JSON.parse(canonical(envelope));
  const frozenScope = JSON.parse(canonical(scope)) as Scope;
  validateScope(frozenScope);
  const frozen = request(approvedBody);
  if (!record(snapshot) || Object.keys(snapshot).sort().join(',') !== 'payload,tag' || !record(snapshot.payload) || typeof snapshot.tag !== 'string') throw new Error('Invalid authenticated plan');
  const serialized = canonical(snapshot.payload);
  const key = await keys(apiKey);
  if (!await crypto.subtle.verify('HMAC', key.signer, unhex(snapshot.tag), text.encode(serialized))) throw new Error('Invalid authenticated plan');
  const payload = snapshot.payload;
  const expectedSlot = await stableSlot(frozenScope);
  if (payload.contract !== CONTRACT || payload.envelopeVersion !== ENVELOPE_VERSION || payload.origin !== ORIGIN ||
      payload.method !== 'POST' || payload.path !== PATHS[media] || payload.media !== media ||
      canonical(payload.scope) !== canonical(frozenScope) || payload.slot !== expectedSlot || payload.credentialBinding !== key.binding ||
      payload.requestJson !== frozen.json || canonical(payload.budget) !== canonical(frozen.budget) ||
      typeof payload.operationId !== 'string' || !/^[a-f0-9-]{36}$/.test(payload.operationId) ||
      payload.idempotencyKey !== `n8n-${payload.operationId}`) throw new Error('Original operation binding mismatch');
  // Return a detached copy so callers cannot mutate authenticated bytes in place.
  return snapshot as unknown as Envelope;
}

export function generationUnavailable(): never {
  throw new Error('Generation remains disabled until native persistence and retry mapping are verified');
}
