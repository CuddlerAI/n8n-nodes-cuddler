import { createHmac, timingSafeEqual } from 'node:crypto';
import type { IExecuteFunctions, IHttpRequestOptions } from 'n8n-workflow';
import type { FrozenCreateRequest, OriginalJobRequest, RecoveryJob, RecoveryTransport } from './RecoveryTransport';
import type { OriginalAssetRequest, PreparedReadTransport } from './PreparedReadTransport';
import type { Media } from './ImmutablePlan';
import type { OriginalCancelRequest, PreparedCancelTransport } from './PreparedCancelTransport';

const ORIGIN = 'https://api.cuddler.ai';
const BINDING_DOMAIN = 'cuddler.n8n.hmac-json/1/credential-binding';
const FAILURE = 'Cuddler transport request was rejected or could not complete.';
const PATHS = { video:'/v1/videos', image:'/v1/images', speech:'/v1/speech', music:'/v1/music' } as const;
const IDS = { video:/^vid_[A-Za-z0-9]{12,40}$/, image:/^img_[A-Za-z0-9]{12,40}$/, speech:/^aud_[A-Za-z0-9]{12,40}$/, music:/^mus_[A-Za-z0-9]{20}$/ };
const STATUSES = new Set(['queued','running','succeeded','failed','canceled']);
const MAX_JSON = 1024 * 1024;
const MAX_REQUEST = 64 * 1024;
const MAX_ASSET = 200 * 1024 * 1024;
const record = (v:unknown):v is Record<string,unknown> => v !== null && typeof v === 'object' && !Array.isArray(v);
function fail():never { throw new Error(FAILURE); }
function exact(value:unknown, keys:readonly string[]):asserts value is Record<string,unknown> {
  if (!record(value) || Object.keys(value).sort().join(',') !== [...keys].sort().join(',')) fail();
}
function media(value:unknown):asserts value is Media {
  if (typeof value !== 'string' || !Object.hasOwn(PATHS,value)) fail();
}
function binding(value:unknown):asserts value is string {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/.test(value)) fail();
}
function original(value:OriginalJobRequest):OriginalJobRequest {
  exact(value,['origin','method','path','media','jobId','credentialBinding']);
  const copy = { ...value };
  media(copy.media); binding(copy.credentialBinding);
  if (copy.origin !== ORIGIN || copy.method !== 'GET' || typeof copy.jobId !== 'string' || !IDS[copy.media].test(copy.jobId) || copy.path !== `${PATHS[copy.media]}/${copy.jobId}`) fail();
  return Object.freeze(copy);
}
function create(value:FrozenCreateRequest):FrozenCreateRequest {
  exact(value,['origin','method','path','media','bodyJson','idempotencyKey','credentialBinding']);
  const copy = { ...value };
  media(copy.media); binding(copy.credentialBinding);
  if (copy.origin !== ORIGIN || copy.method !== 'POST' || copy.path !== PATHS[copy.media] || typeof copy.bodyJson !== 'string' || Buffer.byteLength(copy.bodyJson) > MAX_REQUEST || !record(JSON.parse(copy.bodyJson)) || !/^n8n-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(copy.idempotencyKey)) fail();
  return Object.freeze(copy);
}
function cancelRequest(value:OriginalCancelRequest):OriginalCancelRequest {
  exact(value,['origin','method','path','media','jobId','credentialBinding']);
  const copy={...value};
  if(copy.method!=='POST' || (copy.media as Media)==='image' || copy.path!==`${PATHS[copy.media]}/${copy.jobId}/cancel`) fail();
  original({...copy,method:'GET',path:`${PATHS[copy.media]}/${copy.jobId}`});
  return Object.freeze(copy);
}
function publicHost(host:unknown):host is string {
  // ASCII DNS labels plus a letter-only TLD exclude IPv4/IPv6 literals. URL
  // parsing canonicalizes alternate numeric IP syntax before this check.
  return typeof host === 'string' && host.length <= 253 && host === host.toLowerCase() &&
    /^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/.test(host) &&
    !/(?:^|\.)(?:localhost|local|internal|invalid|test|example|onion)$/.test(host) && host !== 'api.cuddler.ai';
}
function assetUrl(value:unknown, hosts:readonly string[]):string {
  if (typeof value !== 'string' || value.length > 8192) fail();
  const url = new URL(value);
  if (url.protocol !== 'https:' || url.username || url.password || url.hash || (url.port && url.port !== '443') || !publicHost(url.hostname) || !hosts.includes(url.hostname)) fail();
  return value;
}
function headers(value:unknown):Record<string,string> {
  if (!record(value)) fail();
  const safe:Record<string,string> = {};
  for (const [key,entry] of Object.entries(value)) {
    const lower = key.toLowerCase();
    if (!['content-type','content-length','content-encoding','retry-after'].includes(lower)) continue;
    if (typeof entry !== 'string' && typeof entry !== 'number') fail();
    const text = String(entry);
    if (text.length > 256 || /[\r\n]/.test(text) || Object.hasOwn(safe,lower)) fail();
    safe[lower] = text;
  }
  return safe;
}
function retryAfter(value:string|undefined):Record<string,string>|undefined {
  if (value===undefined) return undefined;
  const seconds=/^\d+$/.test(value)?Number(value):Math.ceil((Date.parse(value)-Date.now())/1000);
  if(!Number.isSafeInteger(seconds) || seconds<1 || seconds>86400) fail();
  // Only a validated bounded number crosses the transport boundary.
  return {'retry-after':String(seconds)};
}
// Structural stream contract avoids a forbidden runtime node:stream dependency.
interface BoundedReadable extends AsyncIterable<unknown> {
  on(event:'error', listener:(error:unknown)=>void):unknown;
  destroy(error?:Error):unknown;
}
function readable(value:unknown):value is BoundedReadable {
  return record(value) && typeof value.on==='function' && typeof value.destroy==='function' &&
    typeof (value as unknown as AsyncIterable<unknown>)[Symbol.asyncIterator]==='function';
}
interface FullResponse { statusCode:number; headers:Record<string,string>; body:BoundedReadable }
function response(value:unknown):FullResponse {
  if (!record(value) || !Number.isSafeInteger(value.statusCode) || (value.statusCode as number) < 100 || (value.statusCode as number) > 599 || !readable(value.body)) fail();
  return {statusCode:value.statusCode as number,headers:headers(value.headers),body:value.body};
}
function dispose(value:unknown):void {
  if (record(value) && readable(value.body)) { value.body.on('error',()=>{}); value.body.destroy(); }
}
function abortable<T>(pending:Promise<T>, signal:AbortSignal, late:(value:T)=>void = ()=>{}):Promise<T> {
  return new Promise<T>((resolve,reject)=>{
    let settled = false;
    const abort = () => { if (!settled) { settled=true; reject(new Error(FAILURE)); } };
    signal.addEventListener('abort',abort,{once:true});
    if (signal.aborted) abort();
    pending.then(value=>{
      signal.removeEventListener('abort',abort);
      if (settled) { try { late(value); } catch { /* No late error escapes. */ } return; }
      settled=true; resolve(value);
    },()=>{
      signal.removeEventListener('abort',abort);
      if (!settled) { settled=true; reject(new Error(FAILURE)); }
    });
  });
}
async function consume(stream:BoundedReadable, metadata:Record<string,string>, max:number, signal:AbortSignal):Promise<Buffer> {
  const length = metadata['content-length'];
  const encoding = metadata['content-encoding'];
  if ((encoding && encoding.toLowerCase() !== 'identity') || (length !== undefined && (!/^\d+$/.test(length) || !Number.isSafeInteger(Number(length)) || Number(length)>max))) { stream.on('error',()=>{}); stream.destroy(); fail(); }
  const chunks:Buffer[] = [];
  let total=0;
  const abort=()=>stream.destroy(new Error(FAILURE));
  stream.on('error',()=>{});
  signal.addEventListener('abort',abort,{once:true});
  try {
    if (signal.aborted) fail();
    for await (const value of stream) {
      if (signal.aborted || (!Buffer.isBuffer(value) && !(value instanceof Uint8Array))) fail();
      const chunk = value as Buffer | Uint8Array;
      if (chunk.byteLength > max-total) fail();
      // Copy only a bounded accepted chunk; never buffer the complete response first.
      chunks.push(Buffer.from(chunk)); total+=chunk.byteLength;
    }
    if (signal.aborted || total===0 || (length !== undefined && Number(length)!==total)) fail();
    return Buffer.concat(chunks,total);
  } finally {
    signal.removeEventListener('abort',abort);
    stream.destroy();
  }
}
function job(value:unknown, type:Media, expected?:string):Record<string,unknown> {
  if (!record(value) || value.object !== type || typeof value.id !== 'string' || !IDS[type].test(value.id) || (expected !== undefined && value.id !== expected) || typeof value.status !== 'string' || !STATUSES.has(value.status)) fail();
  const result:Record<string,unknown> = {object:type,id:value.id,status:value.status};
  if (value.stage==='preparing_references') result.stage=value.stage;
  if (value.poll_after_seconds !== undefined) {
    if (!Number.isSafeInteger(value.poll_after_seconds) || (value.poll_after_seconds as number)<1 || (value.poll_after_seconds as number)>86400) fail();
    result.poll_after_seconds=value.poll_after_seconds;
  }
  if (record(value.output)) {
    for (const key of ['url','url_expires_at','retained_until','content_type']) if (value.output[key] !== undefined && (typeof value.output[key] !== 'string' || (value.output[key] as string).length>(key==='url'?8192:256))) fail();
    result.output={url:value.output.url,url_expires_at:value.output.url_expires_at,retained_until:value.output.retained_until,content_type:value.output.content_type};
  }
  return result;
}

export interface NativeTransportOptions {
  /** Trusted installed-code policy. Empty by default; never derive from workflow/env. */
  trustedAssetHosts?:readonly string[];
  timeoutMs?:number;
  maxAssetBytes?:number;
}

/** Unactivated factory: construct per item in trusted installed code only. */
export function createNativeHttpTransports(context:IExecuteFunctions, itemIndex:number, options:NativeTransportOptions = {}):{
  recoveryTransport:RecoveryTransport;
  preparedCancel:{transport:PreparedCancelTransport};
  preparedRead:{transport:PreparedReadTransport;trustedAssetHosts:readonly string[]};
} {
  const hosts=Object.freeze([...(options.trustedAssetHosts ?? [])]);
  const timeout=options.timeoutMs ?? 30000;
  const maxAsset=options.maxAssetBytes ?? 20*1024*1024;
  if (!Number.isSafeInteger(itemIndex) || itemIndex<0 || hosts.some(host=>!publicHost(host)) || new Set(hosts).size!==hosts.length || !Number.isSafeInteger(timeout) || timeout<1 || timeout>60000 || !Number.isSafeInteger(maxAsset) || maxAsset<1 || maxAsset>MAX_ASSET) fail();
  const links=new Map<string,Readonly<{binding:string;expires:number}>>();
  const queuedCancels=new Map<string,string>();
  async function credentials(expected:string, signal:AbortSignal):Promise<string> {
    binding(expected);
    if (signal.aborted) fail();
    const value=await abortable(context.getCredentials('cuddlerApi',itemIndex),signal);
    const key=value.apiKey;
    if (typeof key !== 'string' || !key || key.length>4096 || key.trim()!==key || /[\r\n]/.test(key)) fail();
    const actual=createHmac('sha256',key).update(BINDING_DOMAIN).digest();
    if (!timingSafeEqual(actual,Buffer.from(expected,'hex')) || signal.aborted) fail();
    return key;
  }
  async function issue<T>(make:(signal:AbortSignal)=>Promise<IHttpRequestOptions>, accept:(reply:FullResponse,signal:AbortSignal)=>Promise<T>):Promise<T> {
    const controller=new AbortController();
    const parent=context.getExecutionCancelSignal();
    const signal=AbortSignal.any([controller.signal,AbortSignal.timeout(timeout),...(parent?[parent]:[])]);
    let reply:FullResponse|undefined;
    try {
      const request=await make(signal);
      if (signal.aborted) fail();
      const raw:unknown=await abortable(Promise.resolve(context.helpers.httpRequest({...request,headers:{...request.headers},encoding:'stream',returnFullResponse:true,ignoreHttpStatusErrors:true,disableFollowRedirect:true,maxRedirects:0,sendCredentialsOnCrossOriginRedirect:false,skipSslCertificateValidation:false,timeout,abortSignal:signal})),signal,dispose);
      try { reply=response(raw); } catch { dispose(raw); fail(); }
      if (reply.statusCode>=300 && reply.statusCode<400) fail();
      return await accept(reply,signal);
    } catch { return fail(); }
    finally { controller.abort(); if(reply) { reply.body.on('error',()=>{}); reply.body.destroy(); } }
  }
  async function api(request:FrozenCreateRequest|OriginalJobRequest|OriginalCancelRequest):Promise<{statusCode:number;headers?:Record<string,string>;job?:Record<string,unknown>}> {
    const isCreate=request.method==='POST' && 'bodyJson' in request;
    return await issue(async signal=>{
      const key=await credentials(request.credentialBinding,signal);
      const requestHeaders=Object.freeze({'Authorization':'Bearer '+key,'Cookie':'','Accept':'application/json','Accept-Encoding':'identity',...(isCreate?{'Content-Type':'application/json','Idempotency-Key':(request as FrozenCreateRequest).idempotencyKey}:{})});
      return {url:ORIGIN+request.path,method:request.method,headers:requestHeaders,allowedDomains:'api.cuddler.ai',...(isCreate?{body:Buffer.from((request as FrozenCreateRequest).bodyJson,'utf8')}:{})};
    },async(reply,signal)=>{
      if (request.method==='GET' && [429,503].includes(reply.statusCode)) return {statusCode:reply.statusCode,headers:retryAfter(reply.headers['retry-after'])};
      if (![200,...(isCreate?[202]:[])].includes(reply.statusCode)) return {statusCode:reply.statusCode};
      if (!/^application\/json(?:\s*;|$)/i.test(reply.headers['content-type'] ?? '')) fail();
      const bytes=await consume(reply.body,reply.headers,MAX_JSON,signal);
      const current=job(JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes)),request.media,'jobId' in request?request.jobId:undefined);
      return {statusCode:reply.statusCode,job:current};
    });
  }
  const recoveryTransport:RecoveryTransport=Object.freeze({
    async create(input:FrozenCreateRequest):Promise<RecoveryJob> {
      try { const result=await api(create(input)); if(!result.job) fail(); return {id:result.job.id as string,status:result.job.status as RecoveryJob['status']}; } catch { fail(); }
    },
    async get(input:OriginalJobRequest):Promise<RecoveryJob> {
      try { const result=await api(original(input)); if(!result.job) fail(); return {id:result.job.id as string,status:result.job.status as RecoveryJob['status']}; } catch { fail(); }
    },
  });
  const transport:PreparedReadTransport=Object.freeze({
    async getOriginal(input:OriginalJobRequest) {
      try {
        const request=original(input),result=await api(request);
        if(result.job && record(result.job.output)) {
          const output=result.job.output;
          if(typeof output.url==='string' && typeof output.url_expires_at==='string') {
            try {
              const url=assetUrl(output.url,hosts),expires=Date.parse(output.url_expires_at);
              if(Number.isFinite(expires) && expires>Date.now()) { if(links.size>=16) links.delete(links.keys().next().value!); links.set(url,Object.freeze({binding:request.credentialBinding,expires})); }
            } catch { /* No asset capability is granted for untrusted/expired links. */ }
          }
        }
        return result;
      } catch { fail(); }
    },
    async readAsset(input:OriginalAssetRequest) {
      try {
        exact(input,['method','url','headers','credentials','followRedirects','maxBytes']);
        const request={...input}; exact(request.headers,[]);
        if(request.method!=='GET' || request.credentials!=='omit' || request.followRedirects!==false || !Number.isSafeInteger(request.maxBytes) || request.maxBytes<1 || request.maxBytes>maxAsset) fail();
        const url=assetUrl(request.url,hosts),grant=links.get(url);
        if(!grant || grant.expires<=Date.now()) fail();
        return await issue(async signal=>{
          // Verify the original protected key again, but never send it to assets.
          await credentials(grant.binding,signal);
          if(grant.expires<=Date.now()) fail();
          return {url,method:'GET',headers:Object.freeze({'Cookie':'','Authorization':'','Accept-Encoding':'identity'}),allowedDomains:new URL(url).hostname};
        },async(reply,signal)=>{
          if([401,403,404,410].includes(reply.statusCode)) return {statusCode:reply.statusCode,headers:{},body:Buffer.alloc(0)};
          if(reply.statusCode!==200) fail();
          const body=await consume(reply.body,reply.headers,request.maxBytes,signal);
          const safe:Record<string,string>={}; for(const key of ['content-type','content-length']) if(reply.headers[key]!==undefined) safe[key]=reply.headers[key];
          return {statusCode:200,headers:safe,body};
        });
      } catch { fail(); }
    },
  });
  const preparedCancel:PreparedCancelTransport=Object.freeze({
    async getOriginal(input:OriginalJobRequest) {
      try {
        const request=original(input),result=await api(request);
        const key=request.media+':'+request.jobId;
        queuedCancels.delete(key);
        if(result.statusCode===200 && result.job?.status==='queued' && request.media!=='image') {
          if(queuedCancels.size>=16) queuedCancels.delete(queuedCancels.keys().next().value!);
          queuedCancels.set(key,request.credentialBinding);
        }
        return result;
      } catch { fail(); }
    },
    async cancelQueued(input:OriginalCancelRequest) {
      try {
        const request=cancelRequest(input),key=request.media+':'+request.jobId;
        if(queuedCancels.get(key)!==request.credentialBinding) fail();
        queuedCancels.delete(key);
        return await api(request);
      } catch { fail(); }
    },
  });
  return {recoveryTransport,preparedRead:{transport,trustedAssetHosts:hosts},preparedCancel:{transport:preparedCancel}};
}

/**
 * Separate installed-code capability for protected original-job status reads.
 * No create, cancel, upload, asset URL grant or asset read escapes this wrapper.
 * Native request validation, captured credential binding and bounded HTTP still
 * belong to the same adapter; the caller cannot supply an origin or host policy.
 */
export function createNativeOriginalJobReadTransport(
  context:IExecuteFunctions, itemIndex:number,
):ReturnType<typeof createNativeHttpTransports>['preparedRead'] {
  const native=createNativeHttpTransports(context,itemIndex);
  const transport:PreparedReadTransport=Object.freeze({
    async getOriginal(input:OriginalJobRequest) {
      const reply=await native.preparedRead.transport.getOriginal(input);
      if (!record(reply.job)) return reply;
      const current=reply.job;
      // Status recovery never exposes an asset link, prompt or provider error.
      const safe:Record<string,unknown>={object:current.object,id:current.id,status:current.status};
      for (const key of ['stage','poll_after_seconds']) if (current[key]!==undefined) safe[key]=current[key];
      return {statusCode:reply.statusCode,job:safe};
    },
    async readAsset():Promise<never> { return fail(); },
  });
  return Object.freeze({transport,trustedAssetHosts:Object.freeze([] as string[])});
}
