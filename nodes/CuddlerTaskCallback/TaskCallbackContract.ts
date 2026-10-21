import type { ICredentialTestFunction } from 'n8n-workflow';

export const VERSION = 'cuddler.n8n.per-task-callback/2';
export const MAX_BODY = 1024 * 1024;
export const MAX_RECEIPT = 16 * 1024;
export const RECEIPT_TTL = 25 * 60 * 60 * 1000;
export const LEASE_MS = 60 * 1000;
export const TYPES = [
  'video.succeeded', 'video.failed', 'video.canceled', 'image.succeeded', 'image.failed',
  'speech.succeeded', 'speech.failed', 'speech.canceled', 'music.succeeded', 'music.failed', 'music.canceled',
] as const;

export type EventType = typeof TYPES[number];
export interface SafeEvent {
  object: 'cuddler.event'; eventId: string; type: EventType; eventCreatedAt: string; callbackScope: string;
  subject: { object: 'video' | 'image' | 'speech' | 'music'; id: string; status: 'succeeded' | 'failed' | 'canceled' } | { object: 'balance' };
}
export interface Binding {
  workflowId: string; triggerNodeId: string; tableId: string; rowId: number;
  keyFingerprint: string; secretFingerprint: string;
  callbackDigest: string; callbackScope: string;
}
export interface CredentialSnapshot {
  apiKey: string; signingSecret: string; tableId: string; rowId: number;
  callbackUrl: string; waitTableId: string; waitRowId: number;
}
export function fail(): never { throw new Error('CUDDLER_TASK_CALLBACK_REJECTED'); }
export function record(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === 'object' && !Array.isArray(value); }
export function exact(value: unknown, keys: string): asserts value is Record<string, unknown> {
  if (!record(value) || Object.keys(value).sort().join(',') !== keys.split(',').sort().join(',')) fail();
}
export function canonical(value: unknown): string {
  if (value === null || typeof value === 'string' || typeof value === 'boolean' || (typeof value === 'number' && Number.isFinite(value))) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (record(value)) return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`;
  return fail();
}
export function identifier(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(value)) fail();
}
export function credentials(value: unknown): CredentialSnapshot {
  if (!record(value) || typeof value.apiKey !== 'string' || !value.apiKey || value.apiKey.length > 4096 || value.apiKey.trim() !== value.apiKey || /[\r\n]/.test(value.apiKey) ||
      typeof value.signingSecret !== 'string' || !/^whsec_[A-Za-z0-9_-]{43}$/.test(value.signingSecret) ||
      typeof value.ledgerTableId !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(value.ledgerTableId) ||
      typeof value.ledgerRowId !== 'string' || !/^[1-9][0-9]{0,14}$/.test(value.ledgerRowId) || !Number.isSafeInteger(Number(value.ledgerRowId)) ||
      typeof value.waitTableId !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(value.waitTableId) ||
      typeof value.waitRowId !== 'string' || !/^[1-9][0-9]{0,14}$/.test(value.waitRowId) || !Number.isSafeInteger(Number(value.waitRowId)) ||
      (value.waitTableId === value.ledgerTableId && value.waitRowId === value.ledgerRowId) ||
      typeof value.callbackUrl !== 'string' || value.callbackUrl.length > 2048) fail();
  const url = new URL(value.callbackUrl);
  if ((url.protocol !== 'https:' && !(url.protocol === 'http:' && url.hostname === '127.0.0.1')) || url.username || url.password || url.search || url.hash || url.href !== value.callbackUrl) fail();
  return { apiKey: value.apiKey, signingSecret: value.signingSecret, tableId: value.ledgerTableId, rowId: Number(value.ledgerRowId),
    callbackUrl: value.callbackUrl, waitTableId: value.waitTableId, waitRowId: Number(value.waitRowId) };
}
export function sameCredentials(a: CredentialSnapshot, b: CredentialSnapshot): boolean { return canonical(a) === canonical(b); }
export const localCredentialTest: ICredentialTestFunction = async (credential) => {
  try {
    credentials(credential.data);
    return { status: 'OK', message: 'Local format only. Organization ownership, native table access and signed webhook delivery still require runtime acceptance.' };
  } catch { return { status: 'Error', message: 'Webhook credential format is invalid' }; }
};
export async function digest(value: string): Promise<string> { return Buffer.from(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value))).toString('hex'); }
export function validBinding(value: unknown): asserts value is Binding {
  exact(value, 'workflowId,triggerNodeId,tableId,rowId,keyFingerprint,secretFingerprint,callbackDigest,callbackScope');
  identifier(value.workflowId); identifier(value.triggerNodeId);
  if (typeof value.tableId !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(value.tableId) || !Number.isSafeInteger(value.rowId) || Number(value.rowId) < 1 ||
      typeof value.keyFingerprint !== 'string' || !/^[a-f0-9]{64}$/.test(value.keyFingerprint) || typeof value.secretFingerprint !== 'string' || !/^[a-f0-9]{64}$/.test(value.secretFingerprint) ||
      typeof value.callbackScope !== 'string' || !/^[a-f0-9]{64}$/.test(value.callbackScope) ||
      typeof value.callbackDigest !== 'string' || !/^[a-f0-9]{64}$/.test(value.callbackDigest)) fail();
}
const ids = { video: /^vid_[A-Za-z0-9]{12,40}$/, image: /^img_[A-Za-z0-9]{12,40}$/, speech: /^aud_[A-Za-z0-9]{12,40}$/, music: /^mus_[A-Za-z0-9]{20}$/ };
function iso(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value) || !Number.isFinite(Date.parse(value)) || new Date(value).toISOString() !== value) fail();
}
export function safeEvent(value: unknown): SafeEvent {
  exact(value, 'object,eventId,type,eventCreatedAt,subject,callbackScope');
  if (typeof value.callbackScope !== 'string' || !/^[a-f0-9]{64}$/.test(value.callbackScope) || value.object !== 'cuddler.event' || typeof value.eventId !== 'string' || !/^evt_[A-Za-z0-9]{12,40}$/.test(value.eventId) || !TYPES.includes(value.type as EventType)) fail();
  iso(value.eventCreatedAt);
  const [kind, status] = (value.type as string).split('.');
  if (kind === 'balance') { exact(value.subject, 'object'); if (value.subject.object !== 'balance') fail(); }
  else {
    exact(value.subject, 'object,id,status');
    if (!(kind in ids) || value.subject.object !== kind || value.subject.status !== status || typeof value.subject.id !== 'string' || !ids[kind as keyof typeof ids].test(value.subject.id)) fail();
  }
  return JSON.parse(canonical(value)) as SafeEvent;
}
export function projection(value: unknown): SafeEvent {
  exact(value, 'object,id,type,created_at,data');
  if (value.object !== 'event' || !record(value.data)) fail();
  const kind = typeof value.type === 'string' ? value.type.split('.')[0] : '';
  let subject: unknown;
  if (kind === 'balance') {
    const d = value.data;
    if (d.object !== 'balance' || d.currency !== 'usd' || !['total_usd', 'held_usd', 'available_usd', 'daily_limit_usd', 'spent_today_usd'].every((key) => typeof d[key] === 'string' && /^-?\d+\.\d{6}$/.test(d[key] as string)) || (d.expires_at !== null && typeof d.expires_at !== 'string')) fail();
    if (d.expires_at !== null) iso(d.expires_at);
    subject = { object: 'balance' };
  } else subject = { object: value.data.object, id: value.data.id, status: value.data.status };
  return safeEvent({ object: 'cuddler.event', eventId: value.id, type: value.type, eventCreatedAt: value.created_at, subject, callbackScope: record(value.data.metadata) ? value.data.metadata.cuddler_callback_scope : undefined });
}
export function identity(event: SafeEvent): string { return canonical({ eventId: event.eventId, type: event.type, subject: event.subject, callbackScope: event.callbackScope }); }

/** Verify exact received UTF8 bytes before parsing; the complete whsec_ string is the HMAC key. */
export async function verify(raw: Buffer, signature: string, eventId: string, signingSecret: string): Promise<SafeEvent> {
  if (!raw.length || raw.length > MAX_BODY || signature.length > 1024 || !/^evt_[A-Za-z0-9]{12,40}$/.test(eventId)) fail();
  const parts = signature.split(',');
  if (parts.length < 2 || parts.length > 9 || !/^t=[1-9][0-9]{0,11}$/.test(parts[0]) || parts.slice(1).some((part) => !/^v1=[a-f0-9]{64}$/.test(part))) fail();
  const timestamp = Number(parts[0].slice(2));
  if (!Number.isSafeInteger(timestamp) || Math.abs(Math.floor(Date.now() / 1000) - timestamp) > 300) fail();
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(signingSecret), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']);
  const bytes = Uint8Array.from(Buffer.concat([Buffer.from(`${timestamp}.`), raw]));
  let accepted = false;
  for (const candidate of parts.slice(1)) {
    const matches = await crypto.subtle.verify('HMAC', key, Uint8Array.from(Buffer.from(candidate.slice(3), 'hex')), bytes);
    accepted = matches || accepted;
  }
  if (!accepted || Math.abs(Math.floor(Date.now() / 1000) - timestamp) > 300) fail();
  const event = projection(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(raw)));
  if (event.eventId !== eventId) fail();
  return event;
}

/** Non-secret marker frozen in the original paid request metadata before create. */
export async function callbackScope(credentials: CredentialSnapshot, workflowId: string): Promise<string> {
  identifier(workflowId);
  return await digest(canonical({ version: 'cuddler.n8n.per-task-wait/1', workflowId, callbackUrl: credentials.callbackUrl,
    inboxTableId: credentials.tableId, inboxRowId: credentials.rowId, waitTableId: credentials.waitTableId, waitRowId: credentials.waitRowId,
    keyFingerprint: await digest(`${VERSION}/key\n${credentials.apiKey}`), secretFingerprint: await digest(`${VERSION}/secret\n${credentials.signingSecret}`) }));
}
