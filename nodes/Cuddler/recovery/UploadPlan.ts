import { generateSecureToken } from 'n8n-workflow';
import { canonical, ORIGIN, type Scope } from './ImmutablePlan';
import type { LedgerBinding } from './ProtectedPlanStore';

export const UPLOAD_VERSION = 'cuddler.n8n.upload-plan/1';

export interface UploadSource {
  descriptor: { data: ''; id: string; mimeType: string; fileName?: string };
  kind: 'image' | 'video' | 'audio';
  contentType: string;
  bytes: number;
  sha256: string;
}

export interface UploadPlan {
  version: typeof UPLOAD_VERSION;
  slot: string;
  scope: Scope;
  ledger: LedgerBinding;
  source: UploadSource;
  idempotencyKey: string;
  operationId: string;
  credentialBinding: string;
}

const text = new TextEncoder();
const HASH = /^[a-f0-9]{64}$/;
const IDEMPOTENCY = /^[A-Za-z0-9][A-Za-z0-9_.:-]{7,127}$/;
const TYPES = {
  image: ['image/jpeg', 'image/png', 'image/webp'],
  video: ['video/mp4', 'video/quicktime'],
  audio: ['audio/wav', 'audio/x-wav', 'audio/mpeg'],
};
const LIMITS = { image: 30 * 1024 * 1024, video: 200 * 1024 * 1024, audio: 50 * 1024 * 1024 };
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
const fields = (value: Record<string, unknown>, expected: string[]) => Object.keys(value).sort().join(',') === [...expected].sort().join(',');
const detach = <T>(value: unknown): T => JSON.parse(canonical(value)) as T;
const controls = (value: string) => Array.from(value).some((character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127);

function checkedScope(value: unknown): Scope {
  const scope: unknown = detach(value);
  if (!record(scope) || !fields(scope, ['workflowId', 'nodeId', 'businessBatchId', 'stableItemId']) ||
      Object.values(scope).some((entry) => typeof entry !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(entry))) throw new Error('Invalid original upload scope');
  return scope as unknown as Scope;
}

function checkedLedger(value: unknown, scope: Scope): LedgerBinding {
  const ledger: unknown = detach(value);
  if (!record(ledger) || !fields(ledger, ['workflowId', 'tableId', 'rowId']) || ledger.workflowId !== scope.workflowId ||
      typeof ledger.tableId !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(ledger.tableId) ||
      !Number.isSafeInteger(ledger.rowId) || (ledger.rowId as number) < 1) throw new Error('Invalid original upload ledger');
  return ledger as unknown as LedgerBinding;
}

/** Validate actual helper bytes before creating any persistent binary snapshot. */
export function assertUploadMetadata(kind: unknown, contentType: unknown, bytes: unknown): asserts kind is UploadSource['kind'] {
  if (typeof kind !== 'string' || !Object.prototype.hasOwnProperty.call(TYPES, kind) ||
      typeof contentType !== 'string' || !TYPES[kind as UploadSource['kind']].includes(contentType) ||
      !Number.isSafeInteger(bytes) || (bytes as number) < 1 || (bytes as number) > LIMITS[kind as UploadSource['kind']]) throw new Error('Invalid original upload metadata');
}

function checkedSource(value: unknown): UploadSource {
  const source: unknown = detach(value);
  if (!record(source) || !fields(source, ['descriptor', 'kind', 'contentType', 'bytes', 'sha256']) ||
      typeof source.sha256 !== 'string' || !HASH.test(source.sha256) || !record(source.descriptor)) throw new Error('Invalid original upload source');
  assertUploadMetadata(source.kind, source.contentType, source.bytes);
  const descriptor = source.descriptor;
  const descriptorFields = ['data', 'id', 'mimeType'];
  if (Object.prototype.hasOwnProperty.call(descriptor, 'fileName')) descriptorFields.push('fileName');
  if (!fields(descriptor, descriptorFields) || descriptor.data !== '' || typeof descriptor.id !== 'string' ||
      !descriptor.id || descriptor.id.length > 4096 || descriptor.id.trim() !== descriptor.id || controls(descriptor.id) ||
      descriptor.mimeType !== source.contentType) throw new Error('Invalid original binary descriptor');
  if (Object.prototype.hasOwnProperty.call(descriptor, 'fileName')) {
    const name = descriptor.fileName;
    if (typeof name !== 'string' || !name || name.length > 128 || name.trim() !== name || name === '.' || name === '..' ||
        name.endsWith('.') || /[/\\<>:"|?*]/.test(name) || controls(name)) throw new Error('Invalid original binary filename');
  }
  return source as unknown as UploadSource;
}

function checkedKey(key: string): string {
  if (typeof key !== 'string' || !key || key.length > 4096 || key.trim() !== key || /[\r\n]/.test(key)) throw new Error('Protected upload credential unavailable');
  return key;
}

async function digest(value: string): Promise<string> {
  const bytes = await crypto.subtle.digest('SHA-256', text.encode(value));
  return Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, '0')).join('');
}

/** Independent of source and key so retries/rotation still find the original winner. */
export async function uploadSlot(scope: Scope): Promise<string> {
  const frozen = checkedScope(scope);
  return await digest(canonical({ version: UPLOAD_VERSION, origin: ORIGIN, scope: frozen }));
}

/** Only the persisted CAS winner's generated operation/key may be used for upload. */
export async function makeUploadPlan(scope: Scope, ledger: LedgerBinding, source: UploadSource, key: string): Promise<UploadPlan> {
  const frozenScope = checkedScope(scope);
  const frozenLedger = checkedLedger(ledger, frozenScope);
  const frozenSource = checkedSource(source);
  const frozenKey = checkedKey(key);
  const operationId = generateSecureToken();
  if (!HASH.test(operationId)) throw new Error('Secure upload identity unavailable');
  const slot = await uploadSlot(frozenScope);
  const credentialBinding = await digest(`${UPLOAD_VERSION}\n${frozenKey}`);
  return detach<UploadPlan>({ version: UPLOAD_VERSION, slot, scope: frozenScope, ledger: frozenLedger,
    source: frozenSource, operationId, idempotencyKey: `n8n-upload-${operationId}`, credentialBinding });
}

/** Caller must obtain these bytes through the protected AEAD ledger, never node JSON. */
export async function validateUploadPlan(value: unknown, scope: Scope, ledger: LedgerBinding, key: string): Promise<UploadPlan> {
  // Snapshot all inputs before the first await; returned bytes are the exact checked snapshot.
  const plan: unknown = detach(value);
  const expectedScope = checkedScope(scope);
  const expectedLedger = checkedLedger(ledger, expectedScope);
  const frozenKey = checkedKey(key);
  if (!record(plan) || !fields(plan, ['version', 'slot', 'scope', 'ledger', 'source', 'idempotencyKey', 'operationId', 'credentialBinding']) ||
      plan.version !== UPLOAD_VERSION || typeof plan.slot !== 'string' || !HASH.test(plan.slot) ||
      typeof plan.operationId !== 'string' || !HASH.test(plan.operationId) ||
      typeof plan.idempotencyKey !== 'string' || !IDEMPOTENCY.test(plan.idempotencyKey) || plan.idempotencyKey !== `n8n-upload-${plan.operationId}` ||
      typeof plan.credentialBinding !== 'string' || !HASH.test(plan.credentialBinding)) throw new Error('Invalid original upload plan');
  const originalScope = checkedScope(plan.scope);
  const originalLedger = checkedLedger(plan.ledger, originalScope);
  checkedSource(plan.source);
  if (canonical(originalScope) !== canonical(expectedScope) || canonical(originalLedger) !== canonical(expectedLedger) ||
      plan.slot !== await uploadSlot(expectedScope) || plan.credentialBinding !== await digest(`${UPLOAD_VERSION}\n${frozenKey}`)) throw new Error('Original upload binding mismatch');
  return detach<UploadPlan>(plan);
}

/** A concurrent identical-byte snapshot may have a different native opaque ID/name. */
export function sameUploadSource(a: UploadSource, b: UploadSource): boolean {
  try {
    const first = checkedSource(a); const second = checkedSource(b);
    return first.kind === second.kind && first.contentType === second.contentType && first.bytes === second.bytes && first.sha256 === second.sha256;
  } catch {
    return false;
  }
}
