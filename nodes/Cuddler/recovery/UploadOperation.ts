import type { IBinaryData, IDataObject, IExecuteFunctions } from 'n8n-workflow';
import { canonical, type Scope } from './ImmutablePlan';
import { BuiltinDataTableBridge } from './BuiltinDataTableBridge';
import { NativeDataTableCasStore } from './NativeDataTableCasStore';
import { ProtectedPlanStore, type LedgerBinding } from './ProtectedPlanStore';
import { makeUploadPlan, sameUploadSource, uploadSlot, validateUploadPlan, assertUploadMetadata, type UploadPlan, type UploadSource } from './UploadPlan';
import { disabledUploadTransport, type UploadApiRequest, type UploadOptions } from './UploadTransport';
import { assertRetryResume } from './RetryResumePolicy';

const HANDLE = 'cuddler.n8n.upload-handle/1';
const PREFIX = 'cuddler-up1.';
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
const digest = async (bytes: Buffer) => Buffer.from(await crypto.subtle.digest('SHA-256', new Uint8Array(bytes))).toString('hex');
const mime = (value: string) => value.split(';', 1)[0].trim().toLowerCase();
const ID = /^upl_[A-Za-z0-9]{12,40}$/;

export class UploadRefusal extends Error {
  constructor(readonly code: string) { super(code); }
}
const refuse = (code: string): never => { throw new UploadRefusal(code); };

export interface UploadInput {
  operation: 'prepareUpload' | 'executePreparedUpload' | 'readPreparedUpload';
  scope?: Scope;
  kind?: UploadSource['kind'];
  inputBinary?: IBinaryData;
  opaque?: string;
  expectedItem?: string;
}

/** Snapshot every native descriptor and parameter before any credential/blob await. */
export function snapshotUploadInputs(context: IExecuteFunctions): Array<UploadInput | undefined> {
  return context.getInputData().map((_, index) => {
    const operation = context.getNodeParameter('operation', index);
    if (!['prepareUpload', 'executePreparedUpload', 'readPreparedUpload'].includes(String(operation))) return undefined;
    if (context.getNodeParameter('resource', index) !== 'upload') return refuse('INVALID_UPLOAD_RESOURCE');
    if (operation === 'prepareUpload') {
      const property = context.getNodeParameter('inputBinaryProperty', index, 'data');
      const kind = context.getNodeParameter('uploadKind', index, 'image');
      if (typeof property !== 'string' || !/^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(property) || !['image', 'video', 'audio'].includes(String(kind))) return refuse('INVALID_BINARY_INPUT');
      const scope: Scope = { workflowId: context.getWorkflow().id ?? '', nodeId: context.getNode().id,
        businessBatchId: context.getNodeParameter('businessBatchId', index) as string,
        stableItemId: context.getNodeParameter('stableItemId', index) as string };
      const metadata = context.helpers.assertBinaryData(index, property);
      // A native SDK descriptor only: its data field is never decoded by this node.
      return { operation, scope: JSON.parse(canonical(scope)) as Scope, kind: kind as UploadSource['kind'], inputBinary: { ...metadata } };
    }
    const opaque = context.getNodeParameter('operationHandle', index);
    const expectedItem = context.getNodeParameter('stableItemId', index, '');
    if (typeof opaque !== 'string' || typeof expectedItem !== 'string') return refuse('INVALID_UPLOAD_HANDLE');
    return { operation: operation as UploadInput['operation'], opaque, expectedItem };
  });
}

export async function preflightUploads(inputs: Array<UploadInput | undefined>): Promise<void> {
  const seen = new Set<string>();
  for (const input of inputs) {
    if (!input?.scope) continue;
    const slot = await uploadSlot(input.scope);
    if (seen.has(slot)) return refuse('DUPLICATE_UPLOAD_ITEM');
    seen.add(slot);
  }
}

async function runtime(context: IExecuteFunctions, index: number) {
  const credentials = await context.getCredentials('cuddlerApi', index);
  const row = credentials.ledgerRowId;
  const ledger: LedgerBinding = { workflowId: context.getWorkflow().id ?? '', tableId: credentials.ledgerTableId as string,
    rowId: typeof row === 'number' ? row : typeof row === 'string' && /^[1-9]\d*$/.test(row) ? Number(row) : NaN };
  if (typeof credentials.apiKey !== 'string' || !context.getExecutionId()) return refuse('PROTECTED_UPLOAD_BINDING_REQUIRED');
  const bridge = new BuiltinDataTableBridge(context, []);
  const native = new NativeDataTableCasStore(bridge.capability(ledger.tableId), ledger.rowId);
  const store = await ProtectedPlanStore.open(native, ledger, credentials.apiKey);
  return { key: credentials.apiKey, ledger, store };
}

type Runtime = Awaited<ReturnType<typeof runtime>>;

async function persisted(rt: Runtime, slot: string, scope: Scope) {
  const receipt = await rt.store.read(slot);
  if (!receipt) return refuse('ORIGINAL_UPLOAD_PLAN_MISSING');
  const plan = await validateUploadPlan(JSON.parse(receipt.entry.planJson), scope, rt.ledger, rt.key);
  const id = receipt.entry.knownJobId;
  if (id !== null && !ID.test(id)) return refuse('ORIGINAL_UPLOAD_ID_INVALID');
  return { plan, id, planJson: receipt.entry.planJson };
}

async function open(context: IExecuteFunctions, index: number, input: UploadInput) {
  const rt = await runtime(context, index);
  const opaque = input.opaque;
  if (typeof opaque !== 'string' || !opaque.startsWith(PREFIX) || opaque.length > 16384 || !/^[A-Za-z0-9_-]+$/.test(opaque.slice(PREFIX.length))) return refuse('INVALID_UPLOAD_HANDLE');
  const box = await rt.store.unseal('handle', Buffer.from(opaque.slice(PREFIX.length), 'base64url').toString('utf8'));
  const pointer: unknown = JSON.parse(box.plaintext);
  if (!record(pointer) || Object.keys(pointer).sort().join(',') !== 'operationId,scope,slot,version' || pointer.version !== HANDLE ||
      pointer.slot !== box.slot || !record(pointer.scope) || pointer.scope.workflowId !== context.getWorkflow().id ||
      await uploadSlot(pointer.scope as unknown as Scope) !== box.slot) return refuse('INVALID_UPLOAD_HANDLE');
  const current = await persisted(rt, box.slot, pointer.scope as unknown as Scope);
  if (current.plan.operationId !== pointer.operationId || (input.expectedItem !== '' && input.expectedItem !== current.plan.scope.stableItemId)) return refuse('ORIGINAL_UPLOAD_IDENTITY_MISMATCH');
  return { ...current, rt, opaque };
}

async function originalBytes(context: IExecuteFunctions, index: number, source: UploadSource): Promise<Buffer> {
  let bytes: Buffer;
  try { bytes = Buffer.from(await context.helpers.getBinaryDataBuffer(index, source.descriptor)); }
  catch { return refuse('ORIGINAL_BINARY_UNAVAILABLE'); }
  if (bytes.length !== source.bytes || await digest(bytes) !== source.sha256 || mime(source.descriptor.mimeType) !== source.contentType) return refuse('ORIGINAL_BINARY_CHANGED');
  return bytes;
}

function projection(plan: UploadPlan, opaque: string, status: string, id?: string): IDataObject {
  return { object: 'cuddler.upload-operation', operationHandle: opaque, operationId: plan.operationId,
    businessBatchId: plan.scope.businessBatchId, stableItemId: plan.scope.stableItemId,
    state: status, ready: status === 'ready', ...(status === 'ready' && id ? { uploadId: id } : {}) };
}

function upload(value: unknown, plan: UploadPlan, expectedId?: string) {
  if (!record(value) || value.object !== 'upload' || typeof value.id !== 'string' || !ID.test(value.id) ||
      (expectedId && value.id !== expectedId) || value.kind !== plan.source.kind || value.content_type !== plan.source.contentType ||
      value.bytes !== plan.source.bytes || !['pending', 'ready', 'rejected', 'expired'].includes(String(value.status)) ||
      typeof value.expires_at !== 'string' || !Number.isFinite(Date.parse(value.expires_at))) return refuse('INVALID_UPLOAD_RESPONSE');
  return { id: value.id, status: String(value.status), expires: Date.parse(value.expires_at),
    url: value.upload_url, headers: record(value.upload_headers) ? { ...value.upload_headers } : undefined };
}

export function uploadCapability(options: UploadOptions = {}) {
  const transport = options.transport ?? disabledUploadTransport;
  const hosts = Object.freeze([...(options.trustedAssetHosts ?? [])]);
  if (hosts.some((host) => !/^(?:[a-z0-9-]+\.)+[a-z][a-z0-9-]*$/.test(host) || /(?:^|\.)(?:localhost|local|internal)$/.test(host))) throw new Error('Invalid upload asset host policy');
  return async (context: IExecuteFunctions, index: number, input: UploadInput): Promise<IDataObject> => {
    try {
      assertRetryResume(context.getMode(), input.operation, input.opaque);
      if (input.operation === 'prepareUpload') {
        if (!input.scope || !input.kind || !input.inputBinary) return refuse('INVALID_UPLOAD_SOURCE');
        const rt = await runtime(context, index);
        const bytes = Buffer.from(await context.helpers.getBinaryDataBuffer(index, input.inputBinary));
        const contentType = mime(input.inputBinary.mimeType);
        assertUploadMetadata(input.kind, contentType, bytes.length);
        const metadata = { kind: input.kind, contentType, bytes: bytes.length, sha256: await digest(bytes) };
        const slot = await uploadSlot(input.scope);
        let current;
        const prior = await rt.store.read(slot);
        if (prior) {
          current = await persisted(rt, slot, input.scope);
          if (!sameUploadSource(current.plan.source, { ...metadata, descriptor: current.plan.source.descriptor })) return refuse('UPLOAD_SOURCE_CONFLICT');
        } else {
          const stored = await context.helpers.prepareBinaryData(Buffer.from(bytes), 'cuddler-upload.bin', contentType);
          if (typeof stored.id !== 'string' || !stored.id || stored.id.length > 4096 || mime(stored.mimeType) !== contentType) return refuse('DURABLE_BINARY_STORAGE_REQUIRED');
          const source: UploadSource = { ...metadata, descriptor: { data: '', id: stored.id, mimeType: contentType, fileName: 'cuddler-upload.bin' } };
          const plan = await makeUploadPlan(input.scope, rt.ledger, source, rt.key);
          await originalBytes(context, index, plan.source);
          const before = await runtime(context, index);
          if (before.key !== rt.key || canonical(before.ledger) !== canonical(rt.ledger)) return refuse('UPLOAD_BINDING_CHANGED');
          const saved = await rt.store.saveOnce(slot, canonical(plan));
          current = await persisted(rt, slot, input.scope);
          if (saved.entry.planJson !== current.planJson || !sameUploadSource(current.plan.source, source)) return refuse('UPLOAD_PLAN_READBACK_FAILED');
        }
        await originalBytes(context, index, current.plan.source);
        const pointer = { version: HANDLE, slot, scope: current.plan.scope, operationId: current.plan.operationId };
        const opaque = PREFIX + Buffer.from(await rt.store.seal('handle', slot, canonical(pointer))).toString('base64url');
        await open(context, index, { operation: 'readPreparedUpload', opaque, expectedItem: current.plan.scope.stableItemId });
        return projection(current.plan, opaque, 'prepared');
      }
      let current = await open(context, index, input);
      const plan = current.plan;
      const recheck = async () => {
        const next = await open(context, index, input);
        if (next.planJson !== current.planJson || next.plan.operationId !== plan.operationId || (current.id && next.id !== current.id)) return refuse('UPLOAD_BINDING_CHANGED');
        current = next;
      };
      const api = async (method: 'GET' | 'POST', path: string, body?: UploadApiRequest['body'], key?: string) => {
        await recheck();
        const response = await transport.api(Object.freeze({ origin: 'https://api.cuddler.ai', method, path,
          credentialBinding: plan.credentialBinding, ...(body ? { body: Object.freeze(body) } : {}), ...(key ? { idempotencyKey: key } : {}) }));
        const allowed = method === 'POST' && path === '/v1/uploads' ? [200, 201] : [200];
        if (!allowed.includes(response.statusCode)) return refuse('UPLOAD_API_FAILED');
        return upload(response.upload, plan, current.id ?? undefined);
      };
      let state = current.id ? await api('GET', `/v1/uploads/${current.id}`) : undefined;
      if (input.operation === 'readPreparedUpload') {
        await recheck();
        if (!state) return projection(plan, current.opaque, 'prepared');
      } else if (!state || (state.status === 'pending' && state.expires > Date.now())) {
        const bytes = await originalBytes(context, index, plan.source);
        state = await api('POST', '/v1/uploads', { kind: plan.source.kind, content_type: plan.source.contentType, bytes: plan.source.bytes }, plan.idempotencyKey);
        if (!current.id) {
          await recheck();
          await current.rt.store.recordKnownJob(plan.slot, current.planJson, state.id);
          const confirmed = await open(context, index, input);
          if (confirmed.id !== state.id || confirmed.planJson !== current.planJson) return refuse('UPLOAD_ID_READBACK_FAILED');
          current = confirmed;
        }
        if (state.status === 'pending' && state.expires > Date.now()) {
          if (typeof state.url !== 'string' || !state.headers) return refuse('UPLOAD_PRESIGN_UNAVAILABLE');
          const url = new URL(state.url);
          if (url.protocol !== 'https:' || url.username || url.password || url.hash || (url.port && url.port !== '443') ||
              url.origin === 'https://api.cuddler.ai' || !hosts.includes(url.hostname)) return refuse('UPLOAD_ASSET_HOST_REFUSED');
          const headers: Record<string, string> = {};
          for (const [key, value] of Object.entries(state.headers)) {
            if (!['content-type', 'content-length'].includes(key.toLowerCase()) || typeof value !== 'string') return refuse('UPLOAD_HEADERS_REFUSED');
            headers[key.toLowerCase()] = value;
          }
          if (headers['content-type'] !== plan.source.contentType || headers['content-length'] !== String(bytes.length)) return refuse('UPLOAD_HEADERS_REFUSED');
          await recheck();
          if (state.expires <= Date.now()) return refuse('UPLOAD_PRESIGN_EXPIRED');
          const put = await transport.put(Object.freeze({ method: 'PUT', url: state.url, headers: Object.freeze(headers), body: Buffer.from(bytes),
            maxBytes: bytes.length, credentials: 'omit', followRedirects: false }));
          if (![200, 201, 204].includes(put.statusCode)) return refuse('UPLOAD_PUT_FAILED');
        }
      }
      if (!state) return refuse('ORIGINAL_UPLOAD_MISSING');
      if (input.operation === 'executePreparedUpload' && state.status === 'pending') state = await api('POST', `/v1/uploads/${current.id}/complete`);
      await recheck();
      if (state.status === 'rejected' || state.status === 'expired' || state.expires <= Date.now()) return refuse('UPLOAD_REJECTED_OR_EXPIRED');
      return projection(plan, current.opaque, state.status, state.id);
    } catch (error) {
      if (error instanceof UploadRefusal) return refuse(error.code);
      return refuse('UPLOAD_RECOVERY_FAILED');
    }
  };
}
