import { createHash, timingSafeEqual } from 'node:crypto';
import type { IExecuteFunctions, IHttpRequestOptions } from 'n8n-workflow';
import type { UploadApiRequest, UploadTransport } from './UploadTransport';

const ORIGIN = 'https://api.cuddler.ai';
const UPLOAD_DOMAIN = 'cuddler.n8n.upload-plan/1';
const FAILURE = 'Cuddler upload transport request was rejected or could not complete.';
const ID = /^upl_[A-Za-z0-9]{12,40}$/;
const HASH = /^[a-f0-9]{64}$/;
const MAX_JSON = 1024 * 1024;
const MAX_PUT_RESPONSE = 64 * 1024;
const TYPES = { image: ['image/jpeg', 'image/png', 'image/webp'], video: ['video/mp4', 'video/quicktime'], audio: ['audio/wav', 'audio/x-wav', 'audio/mpeg'] };
const LIMITS = { image: 30 * 1024 * 1024, video: 200 * 1024 * 1024, audio: 50 * 1024 * 1024 };
type Kind = keyof typeof TYPES;
interface Metadata { kind: Kind; content_type: string; bytes: number }
interface UploadState extends Metadata {
  object: 'upload'; id: string; status: 'pending' | 'ready' | 'rejected' | 'expired'; expires_at: string;
  upload_url?: string; upload_headers?: Record<string, string>;
}
interface OriginalRequest { request: Readonly<UploadApiRequest>; stage: 'create' | 'get' | 'complete'; id?: string; metadata?: Readonly<Metadata> }
interface Grant { id: string; binding: string; metadata: Readonly<Metadata>; expires: number; headers: Readonly<Record<string, string>> }
interface BoundedReadable extends AsyncIterable<unknown> {
  on(event: 'error', listener: (error: unknown) => void): unknown;
  destroy(error?: Error): unknown;
}
interface Response { statusCode: number; headers: Record<string, string>; body: BoundedReadable }

const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
function fail(): never { throw new Error(FAILURE); }
function exact(value: unknown, fields: readonly string[]): asserts value is Record<string, unknown> {
  if (!record(value) || Object.keys(value).sort().join(',') !== [...fields].sort().join(',')) fail();
}
function metadata(value: unknown): Metadata {
  if (!record(value) || typeof value.kind !== 'string' || !Object.hasOwn(TYPES, value.kind)) fail();
  const kind = value.kind as Kind;
  if (typeof value.content_type !== 'string' || !TYPES[kind].includes(value.content_type) ||
      !Number.isSafeInteger(value.bytes) || (value.bytes as number) < 1 || (value.bytes as number) > LIMITS[kind]) fail();
  return { kind, content_type: value.content_type, bytes: value.bytes as number };
}
const same = (first: Metadata, second: Metadata) => first.kind === second.kind && first.content_type === second.content_type && first.bytes === second.bytes;
function original(input: UploadApiRequest): OriginalRequest {
  if (!record(input)) fail();
  const copy = { ...input };
  if (copy.origin !== ORIGIN || typeof copy.credentialBinding !== 'string' || !HASH.test(copy.credentialBinding)) fail();
  if (copy.method === 'POST' && copy.path === '/v1/uploads') {
    exact(copy, ['origin', 'method', 'path', 'credentialBinding', 'body', 'idempotencyKey']);
    exact(copy.body, ['kind', 'content_type', 'bytes']);
    const body = Object.freeze(metadata(copy.body));
    if (typeof copy.idempotencyKey !== 'string' || !/^n8n-upload-[a-f0-9]{64}$/.test(copy.idempotencyKey)) fail();
    return { stage: 'create', metadata: body, request: Object.freeze({ ...copy, body }) };
  }
  exact(copy, ['origin', 'method', 'path', 'credentialBinding']);
  const match = typeof copy.path === 'string' ? /^\/v1\/uploads\/(upl_[A-Za-z0-9]{12,40})(\/complete)?$/.exec(copy.path) : null;
  if (!match || copy.method !== (match[2] ? 'POST' : 'GET')) fail();
  return { stage: match[2] ? 'complete' : 'get', id: match[1], request: Object.freeze(copy) };
}
function publicHost(value: unknown): value is string {
  return typeof value === 'string' && value.length <= 253 && value === value.toLowerCase() &&
    /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/.test(value) &&
    !/(?:^|\.)(?:localhost|local|internal|invalid|test|example|onion)$/.test(value) && value !== 'api.cuddler.ai';
}
function assetUrl(value: unknown, hosts: readonly string[]): string {
  if (typeof value !== 'string' || value.length > 8192) fail();
  const url = new URL(value);
  if (url.protocol !== 'https:' || url.username || url.password || url.hash || (url.port && url.port !== '443') ||
      !publicHost(url.hostname) || !hosts.includes(url.hostname)) fail();
  return value;
}
function uploadHeaders(value: unknown, source: Metadata): Readonly<Record<string, string>> {
  if (!record(value)) fail();
  const result: Record<string, string> = {};
  for (const [field, entry] of Object.entries(value)) {
    const name = field.toLowerCase();
    if (!['content-type', 'content-length'].includes(name) || typeof entry !== 'string' || Object.hasOwn(result, name)) fail();
    result[name] = entry;
  }
  if (result['content-type'] !== source.content_type || result['content-length'] !== String(source.bytes)) fail();
  return Object.freeze(result);
}
function responseHeaders(value: unknown): Record<string, string> {
  if (!record(value)) fail();
  const result: Record<string, string> = {};
  for (const [field, entry] of Object.entries(value)) {
    const name = field.toLowerCase();
    if (!['content-type', 'content-length', 'content-encoding'].includes(name)) continue;
    if ((typeof entry !== 'string' && typeof entry !== 'number') || Object.hasOwn(result, name)) fail();
    const text = String(entry);
    if (text.length > 256 || /[\r\n]/.test(text)) fail();
    result[name] = text;
  }
  return result;
}
function readable(value: unknown): value is BoundedReadable {
  return record(value) && typeof value.on === 'function' && typeof value.destroy === 'function' &&
    typeof (value as unknown as AsyncIterable<unknown>)[Symbol.asyncIterator] === 'function';
}
function response(value: unknown): Response {
  if (!record(value) || !Number.isSafeInteger(value.statusCode) || (value.statusCode as number) < 100 ||
      (value.statusCode as number) > 599 || !readable(value.body)) fail();
  return { statusCode: value.statusCode as number, headers: responseHeaders(value.headers), body: value.body };
}
function dispose(value: unknown): void {
  if (record(value) && readable(value.body)) { value.body.on('error', () => {}); value.body.destroy(); }
}
function abortable<T>(pending: Promise<T>, signal: AbortSignal, late: (value: T) => void = () => {}): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const abort = () => { if (!settled) { settled = true; reject(new Error(FAILURE)); } };
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) abort();
    pending.then(value => {
      signal.removeEventListener('abort', abort);
      if (settled) { try { late(value); } catch { /* No late error escapes. */ } return; }
      settled = true; resolve(value);
    }, () => { signal.removeEventListener('abort', abort); if (!settled) { settled = true; reject(new Error(FAILURE)); } });
  });
}
async function consume(stream: BoundedReadable, headers: Record<string, string>, max: number, signal: AbortSignal, empty = false): Promise<Buffer> {
  const length = headers['content-length'];
  if ((headers['content-encoding'] && headers['content-encoding'].toLowerCase() !== 'identity') ||
      (length !== undefined && (!/^\d+$/.test(length) || !Number.isSafeInteger(Number(length)) || Number(length) > max))) {
    stream.on('error', () => {}); stream.destroy(); fail();
  }
  const chunks: Buffer[] = []; let total = 0;
  const abort = () => stream.destroy(new Error(FAILURE));
  stream.on('error', () => {}); signal.addEventListener('abort', abort, { once: true });
  try {
    if (signal.aborted) fail();
    for await (const value of stream) {
      if (signal.aborted || (!Buffer.isBuffer(value) && !(value instanceof Uint8Array)) || value.byteLength > max - total) fail();
      chunks.push(Buffer.from(value)); total += value.byteLength;
    }
    if (signal.aborted || (!empty && total === 0) || (length !== undefined && Number(length) !== total)) fail();
    return Buffer.concat(chunks, total);
  } finally { signal.removeEventListener('abort', abort); stream.destroy(); }
}

export interface NativeUploadTransportOptions {
  /** Exact approved public hosts from trusted installed code; empty by default. */
  trustedAssetHosts?: readonly string[];
  timeoutMs?: number;
}

/** Unactivated per-item factory. Original plan/ledger/source verification remains required. */
export function createNativeUploadHttpTransport(context: IExecuteFunctions, itemIndex: number, options: NativeUploadTransportOptions = {}): {
  transport: UploadTransport; trustedAssetHosts: readonly string[];
} {
  const hosts = Object.freeze([...(options.trustedAssetHosts ?? [])]);
  const timeout = options.timeoutMs ?? 30000;
  if (!Number.isSafeInteger(itemIndex) || itemIndex < 0 || !Number.isSafeInteger(timeout) || timeout < 1 || timeout > 60000 ||
      hosts.some(host => !publicHost(host)) || new Set(hosts).size !== hosts.length) fail();
  const grants = new Map<string, Readonly<Grant>>();
  const seen = new Map<string, Readonly<{ binding: string; metadata: Readonly<Metadata> }>>();
  async function credential(expected: string, signal: AbortSignal): Promise<string> {
    if (!HASH.test(expected) || signal.aborted) fail();
    const value = await abortable(context.getCredentials('cuddlerApi', itemIndex), signal);
    const key = value.apiKey;
    if (typeof key !== 'string' || !key || key.length > 4096 || key.trim() !== key || /[\r\n]/.test(key)) fail();
    const actual = createHash('sha256').update(`${UPLOAD_DOMAIN}\n${key}`).digest();
    if (!timingSafeEqual(actual, Buffer.from(expected, 'hex')) || signal.aborted) fail();
    return key;
  }
  async function issue<T>(make: (signal: AbortSignal) => Promise<IHttpRequestOptions>, accept: (reply: Response, signal: AbortSignal) => Promise<T>): Promise<T> {
    const controller = new AbortController(); const parent = context.getExecutionCancelSignal();
    const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(timeout), ...(parent ? [parent] : [])]);
    let reply: Response | undefined;
    try {
      if (signal.aborted) fail();
      const request = await make(signal);
      if (signal.aborted) fail();
      // The official helper mutates headers for OTel. Clone at its boundary,
      // preserving our private captured headers while allowing normal tracing.
      const raw: unknown = await abortable(Promise.resolve(context.helpers.httpRequest({ ...request, headers: { ...request.headers },
        encoding: 'stream', returnFullResponse: true, ignoreHttpStatusErrors: true, disableFollowRedirect: true,
        maxRedirects: 0, sendCredentialsOnCrossOriginRedirect: false, skipSslCertificateValidation: false, timeout, abortSignal: signal })), signal, dispose);
      try { reply = response(raw); } catch { dispose(raw); fail(); }
      if (reply.statusCode >= 300 && reply.statusCode < 400) fail();
      return await abortable(Promise.resolve(accept(reply, signal)), signal);
    } catch { return fail(); }
    finally { controller.abort(); if (reply) { reply.body.on('error', () => {}); reply.body.destroy(); } }
  }
  function project(value: unknown, request: OriginalRequest): UploadState {
    if (!record(value) || value.object !== 'upload' || typeof value.id !== 'string' || !ID.test(value.id) ||
        (request.id !== undefined && value.id !== request.id) || typeof value.status !== 'string' ||
        !['pending', 'ready', 'rejected', 'expired'].includes(value.status) || typeof value.expires_at !== 'string' ||
        !Number.isFinite(Date.parse(value.expires_at))) fail();
    const source = Object.freeze(metadata(value)); const prior = seen.get(value.id);
    if ((request.metadata && !same(source, request.metadata)) || (prior && (prior.binding !== request.request.credentialBinding || !same(source, prior.metadata)))) fail();
    const result: UploadState = { object: 'upload', id: value.id, status: value.status as UploadState['status'], expires_at: value.expires_at, ...source };
    const expires = Date.parse(result.expires_at);
    if (result.status === 'pending' && expires > Date.now() && value.upload_url !== undefined) {
      // Only original create/replay grants PUT; GET/complete are not refresh APIs.
      if (request.stage !== 'create') fail();
      const url = assetUrl(value.upload_url, hosts); const headers = uploadHeaders(value.upload_headers, source);
      const existing = grants.get(url);
      if (existing && (existing.id !== result.id || existing.binding !== request.request.credentialBinding || !same(existing.metadata, source))) fail();
      result.upload_url = url; result.upload_headers = { ...headers };
    }
    return result;
  }
  function remember(value: UploadState, binding: string): void {
    const source = Object.freeze(metadata(value));
    if (seen.size >= 32 && !seen.has(value.id)) fail();
    seen.set(value.id, Object.freeze({ binding, metadata: source }));
    for (const [url, grant] of grants) if (grant.id === value.id && (value.status !== 'pending' || grant.expires <= Date.now())) grants.delete(url);
    if (value.upload_url && value.upload_headers) {
      if (grants.size >= 16 && !grants.has(value.upload_url)) grants.delete(grants.keys().next().value!);
      grants.set(value.upload_url, Object.freeze({ id: value.id, binding, metadata: source,
        expires: Date.parse(value.expires_at), headers: Object.freeze({ ...value.upload_headers }) }));
    }
  }
  const transport: UploadTransport = Object.freeze({
    async api(input: UploadApiRequest) {
      try {
        const frozen = original(input); const request = frozen.request;
        return await issue(async signal => {
          const key = await credential(request.credentialBinding, signal);
          const headers = Object.freeze({ Authorization: `Bearer ${key}`, Cookie: '', Accept: 'application/json', 'Accept-Encoding': 'identity',
            ...(frozen.stage === 'create' ? { 'Content-Type': 'application/json', 'Idempotency-Key': request.idempotencyKey! } : {}) });
          return { url: ORIGIN + request.path, method: request.method, headers, allowedDomains: 'api.cuddler.ai',
            ...(frozen.stage === 'create' ? { body: Buffer.from(JSON.stringify(request.body), 'utf8') } : {}) };
        }, async (reply, signal) => {
          if (!(frozen.stage === 'create' ? [200, 201] : [200]).includes(reply.statusCode)) return { statusCode: reply.statusCode };
          if (!/^application\/json(?:\s*;|$)/i.test(reply.headers['content-type'] ?? '')) fail();
          const bytes = await consume(reply.body, reply.headers, MAX_JSON, signal);
          const result = project(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)), frozen);
          await credential(request.credentialBinding, signal);
          remember(result, request.credentialBinding);
          return { statusCode: reply.statusCode, upload: result };
        });
      } catch { return fail(); }
    },
    async put(input: Parameters<UploadTransport['put']>[0]) {
      try {
        exact(input, ['method', 'url', 'headers', 'body', 'maxBytes', 'credentials', 'followRedirects']);
        const request = { ...input }; const url = assetUrl(request.url, hosts); const grant = grants.get(url);
        if (!grant || grant.expires <= Date.now() || request.method !== 'PUT' || request.credentials !== 'omit' || request.followRedirects !== false ||
            !Buffer.isBuffer(request.body) || request.body.length !== grant.metadata.bytes || request.maxBytes !== grant.metadata.bytes) fail();
        const bytes = Buffer.from(request.body); const headers = uploadHeaders(request.headers, grant.metadata);
        if (['content-type', 'content-length'].some(field => headers[field] !== grant.headers[field])) fail();
        return await issue(async signal => {
          await credential(grant.binding, signal);
          if (grant.expires <= Date.now() || grants.get(url) !== grant) fail();
          return { url, method: 'PUT', headers: Object.freeze({ ...headers, Authorization: '', Cookie: '', 'Accept-Encoding': 'identity' }),
            body: Buffer.from(bytes), allowedDomains: new URL(url).hostname };
        }, async (reply, signal) => {
          if (![200, 201, 204].includes(reply.statusCode)) return { statusCode: reply.statusCode };
          await consume(reply.body, reply.headers, MAX_PUT_RESPONSE, signal, true);
          await credential(grant.binding, signal);
          if (grants.get(url) !== grant || grant.expires <= Date.now()) fail();
          return { statusCode: reply.statusCode };
        });
      } catch { return fail(); }
    },
  });
  return { transport, trustedAssetHosts: hosts };
}
