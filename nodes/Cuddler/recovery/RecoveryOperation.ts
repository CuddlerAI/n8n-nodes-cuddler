import type { IDataObject, IExecuteFunctions } from 'n8n-workflow';
import { canonical, stableSlot, type Envelope, type Media, type Scope } from './ImmutablePlan';
import { RecoveryBarrier, type ApprovedOperation, type OpenedOperation } from './RecoveryBarrier';
import { BuiltinDataTableBridge } from './BuiltinDataTableBridge';
import { NativeDataTableCasStore } from './NativeDataTableCasStore';
import { ProtectedPlanStore } from './ProtectedPlanStore';
import { disabledRecoveryTransport, type FrozenCreateRequest, type RecoveryJob, type RecoveryTransport, type OriginalJobRequest } from './RecoveryTransport';
import { assertRetryResume } from './RetryResumePolicy';

const HANDLE_VERSION = 'cuddler.n8n.operation-handle/1';
const HANDLE_PREFIX = 'cuddler-op1.';
const PATHS = { video: '/v1/videos', image: '/v1/images', speech: '/v1/speech', music: '/v1/music' } as const;
const JOB_IDS: Record<Media, RegExp> = { video: /^vid_[A-Za-z0-9]{12,40}$/, image: /^img_[A-Za-z0-9]{12,40}$/, speech: /^aud_[A-Za-z0-9]{12,40}$/, music: /^mus_[A-Za-z0-9]{20}$/ };
const isRecord = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);

interface Handle {
  version: typeof HANDLE_VERSION;
  scope: Scope;
  slot: string;
  media: Media;
  operationId: string;
  prepareExecutionId: string;
}

export function isRecoveryOperation(operation: string): boolean {
  return ['prepareMedia', 'executePrepared', 'readPrepared', 'pollPrepared', 'downloadPreparedFile', 'cancelPrepared'].includes(operation);
}

function mediaParameter(context: IExecuteFunctions, index: number): Media {
  const value = context.getNodeParameter('resource', index);
  if (typeof value !== 'string' || !Object.prototype.hasOwnProperty.call(PATHS, value)) throw new Error('Unsupported media');
  return value as Media;
}

function bodyParameter(context: IExecuteFunctions, index: number, required: boolean): unknown {
  const raw = context.getNodeParameter('requestBody', index, '');
  if (raw === '' && !required) return undefined;
  const value: unknown = typeof raw === 'string' ? JSON.parse(raw) : raw;
  if (!isRecord(value)) throw new Error('A complete JSON request is required');
  return JSON.parse(canonical(value)) as unknown;
}

function scopeParameter(context: IExecuteFunctions, index: number): Scope {
  const scope: Scope = {
    workflowId: context.getWorkflow().id ?? '', nodeId: context.getNode().id,
    businessBatchId: context.getNodeParameter('businessBatchId', index) as string,
    stableItemId: context.getNodeParameter('stableItemId', index) as string,
  };
  return JSON.parse(canonical(scope)) as Scope;
}

export interface RecoveryInput {
  operation: string;
  media: Media;
  body: unknown;
  scope?: Scope;
  opaque?: string;
  expectedItem?: string;
  file?: { property: string; maxBytes: number };
}

/** Detach every user-owned recovery value synchronously, before the first await. */
export function snapshotRecoveryInputs(context: IExecuteFunctions): Array<RecoveryInput | undefined> {
  return context.getInputData().map((_, index) => {
    const operation = context.getNodeParameter('operation', index) as string;
    if (!isRecoveryOperation(operation)) return undefined;
    const common = { operation, media: mediaParameter(context, index), body: bodyParameter(context, index, operation === 'prepareMedia') };
    if (operation === 'prepareMedia') return { ...common, scope: scopeParameter(context, index) };
    const opaque = context.getNodeParameter('operationHandle', index) as string;
    const expectedItem = context.getNodeParameter('stableItemId', index, '') as string;
    if (typeof opaque !== 'string' || typeof expectedItem !== 'string') throw new Error('Invalid original operation parameters');
    if (operation === 'downloadPreparedFile') {
      const property = context.getNodeParameter('outputBinaryProperty', index, 'data');
      const maxBytes = context.getNodeParameter('maxFileBytes', index, 20 * 1024 * 1024);
      if (typeof property !== 'string' || !/^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(property) ||
          !Number.isSafeInteger(maxBytes) || (maxBytes as number) < 1 || (maxBytes as number) > 200 * 1024 * 1024) throw new Error('Invalid binary output settings');
      return { ...common, opaque, expectedItem, file: { property, maxBytes: maxBytes as number } };
    }
    return { ...common, opaque, expectedItem };
  });
}

/** Reject ambiguous input before any item is persisted. Indices never identify business items. */
export async function preflightRecovery(inputs: Array<RecoveryInput | undefined>): Promise<void> {
  const seen = new Set<string>();
  for (const input of inputs) {
    if (input?.operation !== 'prepareMedia' || !input.scope) continue;
    const slot = await stableSlot(input.scope);
    if (seen.has(slot)) throw new Error('Duplicate stable business item');
    seen.add(slot);
  }
}

async function protectedRuntime(context: IExecuteFunctions, index: number) {
  const value = await context.getCredentials('cuddlerApi', index);
  const key = value.apiKey;
  const tableId = value.ledgerTableId;
  const rowRaw = value.ledgerRowId;
  const rowId = typeof rowRaw === 'number' ? rowRaw : typeof rowRaw === 'string' && /^[1-9]\d*$/.test(rowRaw) ? Number(rowRaw) : NaN;
  const workflowId = context.getWorkflow().id;
  if (typeof key !== 'string' || typeof tableId !== 'string' || !workflowId || !context.getExecutionId()) throw new Error('Protected credentials and trusted host identity are required');
  const bridge = new BuiltinDataTableBridge(context, []);
  const native = new NativeDataTableCasStore(bridge.capability(tableId), rowId);
  const store = await ProtectedPlanStore.open(native, { workflowId, tableId, rowId }, key);
  return { key, store, barrier: new RecoveryBarrier(store) };
}

function publicOperation(envelope: Envelope, operationHandle: string): IDataObject {
  return {
    object: 'cuddler.operation', operationHandle, media: envelope.payload.media,
    operationId: envelope.payload.operationId, businessBatchId: envelope.payload.scope.businessBatchId,
    stableItemId: envelope.payload.scope.stableItemId,
  };
}

async function makeHandle(context: IExecuteFunctions, store: ProtectedPlanStore, envelope: Envelope): Promise<string> {
  const handle: Handle = {
    version: HANDLE_VERSION, scope: envelope.payload.scope, slot: envelope.payload.slot,
    media: envelope.payload.media, operationId: envelope.payload.operationId,
    prepareExecutionId: context.getExecutionId(),
  };
  return HANDLE_PREFIX + Buffer.from(await store.seal('handle', handle.slot, canonical(handle))).toString('base64url');
}

async function openHandle(context: IExecuteFunctions, input: RecoveryInput, store: ProtectedPlanStore): Promise<{ handle: Handle; opaque: string }> {
  const opaque = input.opaque;
  if (typeof opaque !== 'string' || !opaque.startsWith(HANDLE_PREFIX) || opaque.length > 16384 || !/^[A-Za-z0-9_-]+$/.test(opaque.slice(HANDLE_PREFIX.length))) throw new Error('Original operation handle is required');
  const opened = await store.unseal('handle', Buffer.from(opaque.slice(HANDLE_PREFIX.length), 'base64url').toString('utf8'));
  const raw: unknown = JSON.parse(opened.plaintext);
  if (!isRecord(raw) || Object.keys(raw).sort().join(',') !== 'media,operationId,prepareExecutionId,scope,slot,version' ||
      raw.version !== HANDLE_VERSION || raw.slot !== opened.slot || typeof raw.operationId !== 'string' ||
      typeof raw.prepareExecutionId !== 'string' || !raw.prepareExecutionId || !isRecord(raw.scope)) throw new Error('Invalid original operation handle');
  const handle = raw as unknown as Handle;
  if (handle.scope.workflowId !== context.getWorkflow().id || handle.media !== input.media || await stableSlot(handle.scope) !== opened.slot) throw new Error('Original host scope mismatch');
  const expectedItem = input.expectedItem;
  if (expectedItem !== '' && expectedItem !== handle.scope.stableItemId) throw new Error('Original stable item mismatch');
  return { handle, opaque };
}

function validatedJob(value: unknown, media: Media, expectedId?: string): RecoveryJob {
  // Detach before using a transport-owned response; never return provider extras/URLs.
  if (!isRecord(value) || typeof value.id !== 'string' || !JOB_IDS[media].test(value.id) || (expectedId !== undefined && value.id !== expectedId) ||
      !['queued', 'running', 'succeeded', 'failed', 'canceled'].includes(String(value.status))) throw new Error('Invalid original job response');
  return { id: value.id, status: value.status as RecoveryJob['status'] };
}

async function approvedFromHandle(store: ProtectedPlanStore, handle: Handle, expectedBody: unknown): Promise<ApprovedOperation> {
  const receipt = await store.read(handle.slot);
  if (!receipt) throw new Error('Original persistent plan missing');
  const envelope: unknown = JSON.parse(receipt.entry.planJson);
  if (!isRecord(envelope) || !isRecord(envelope.payload) || typeof envelope.payload.requestJson !== 'string') throw new Error('Invalid original plan');
  const body: unknown = JSON.parse(envelope.payload.requestJson);
  if (expectedBody !== undefined && canonical(body) !== canonical(expectedBody)) throw new Error('Original request mismatch');
  return { media: handle.media, scope: handle.scope, body };
}

export async function recoveryOperation(
  context: IExecuteFunctions, index: number, input: RecoveryInput, transport: RecoveryTransport,
): Promise<IDataObject> {
  const operation = input.operation;
  assertRetryResume(context.getMode(), operation, input.opaque);
  const { key, store, barrier } = await protectedRuntime(context, index);
  if (operation === 'prepareMedia') {
    if (!input.scope) throw new Error('Original scope missing');
    const approved: ApprovedOperation = { media: input.media, body: input.body, scope: input.scope };
    const opened = await barrier.prepareAndPersist(approved, key, context.getMode());
    const opaque = await makeHandle(context, store, opened.envelope);
    return { ...publicOperation(opened.envelope, opaque), state: 'prepared', paidNetworkEnabled: false };
  }
  if (!['executePrepared', 'readPrepared'].includes(operation)) throw new Error('Unsupported recovery operation');
  const { handle, opaque } = await openHandle(context, input, store);
  const approved = await approvedFromHandle(store, handle, input.body);
  let opened: OpenedOperation = await barrier.resumeExplicit(approved, key, context.getMode());
  if (opened.envelope.payload.operationId !== handle.operationId) throw new Error('Original operation identity mismatch');
  let job: RecoveryJob;
  if (opened.knownJobId) {
    job = validatedJob(await transport.get(Object.freeze({ origin: 'https://api.cuddler.ai', method: 'GET',
      path: `${PATHS[handle.media]}/${encodeURIComponent(opened.knownJobId)}`, media: handle.media,
      jobId: opened.knownJobId, credentialBinding: opened.envelope.payload.credentialBinding })), handle.media, opened.knownJobId);
  } else {
    if (operation === 'readPrepared') throw new Error('No original job identity is persisted');
    const payload = opened.envelope.payload;
    const request: FrozenCreateRequest = Object.freeze({ origin: 'https://api.cuddler.ai', method: 'POST',
      path: PATHS[handle.media], media: handle.media, bodyJson: payload.requestJson,
      idempotencyKey: payload.idempotencyKey, credentialBinding: payload.credentialBinding });
    job = validatedJob(await transport.create(request), handle.media);
    opened = await barrier.recordOriginalJob(approved, key, context.getMode(), job.id);
  }
  return { ...publicOperation(opened.envelope, opaque), state: 'original-job', jobId: job.id, status: job.status,
    providerMode: transport === disabledRecoveryTransport ? 'disabled' : 'constructor-capability' };
}

/** Authenticate the original pointer and complete persisted plan, with no create fallback. */
export async function resolveOriginalPrepared(context: IExecuteFunctions, index: number, input: RecoveryInput): Promise<{
  identity: IDataObject;
  request: Readonly<OriginalJobRequest>;
  recheck(): Promise<void>;
}> {
  assertRetryResume(context.getMode(), input.operation, input.opaque);
  if (!['pollPrepared', 'downloadPreparedFile', 'cancelPrepared'].includes(input.operation)) throw new Error('Unsupported original read');
  const { key, store, barrier } = await protectedRuntime(context, index);
  const { handle, opaque } = await openHandle(context, input, store);
  const approved = await approvedFromHandle(store, handle, input.body);
  const opened = await barrier.resumeExplicit(approved, key, context.getMode());
  if (opened.envelope.payload.operationId !== handle.operationId || !opened.knownJobId || !JOB_IDS[handle.media].test(opened.knownJobId)) throw new Error('Original job identity unavailable');
  const request: Readonly<OriginalJobRequest> = Object.freeze({ origin: 'https://api.cuddler.ai', method: 'GET',
    path: `${PATHS[handle.media]}/${encodeURIComponent(opened.knownJobId)}`, media: handle.media,
    jobId: opened.knownJobId, credentialBinding: opened.envelope.payload.credentialBinding });
  return { identity: publicOperation(opened.envelope, opaque), request, async recheck() {
    const current = await resolveOriginalPrepared(context, index, input);
    if (canonical(current.request) !== canonical(request)) throw new Error('Original protected binding changed');
  } };
}
