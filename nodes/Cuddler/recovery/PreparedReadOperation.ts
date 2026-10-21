import type { IDataObject, IExecuteFunctions, INodeExecutionData } from 'n8n-workflow';
import type { Media } from './ImmutablePlan';
import { resolveOriginalPrepared, type RecoveryInput } from './RecoveryOperation';
import { disabledPreparedReadTransport, type PreparedReadOptions } from './PreparedReadTransport';

const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
const TERMINAL = new Set(['succeeded', 'failed', 'canceled']);
const mime = (value: string) => value.split(';', 1)[0].trim().toLowerCase();
const TYPES = { video: ['video/mp4', 'video/quicktime'], image: ['image/jpeg', 'image/png', 'image/webp'], speech: ['audio/mpeg'], music: ['audio/mpeg'] };
const timestamp = (value: unknown) => typeof value === 'string' ? Date.parse(value) : NaN;

function headers(value: Readonly<Record<string, string>> = {}): Record<string, string> {
  return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key.toLowerCase(), entry]));
}

function waitHint(value: unknown): number {
  if (!Number.isSafeInteger(value) || (value as number) < 1 || (value as number) > 86400) throw new Error('Invalid wait hint');
  return value as number;
}

function retryHint(value: string | undefined): number {
  if (value === undefined) return 30;
  return waitHint(/^\d+$/.test(value) ? Number(value) : Math.ceil((Date.parse(value) - Date.now()) / 1000));
}

function job(value: unknown, media: Media, id: string) {
  if (!record(value) || value.object !== media || value.id !== id ||
      !['queued', 'running', 'succeeded', 'failed', 'canceled'].includes(String(value.status))) throw new Error('Invalid original job');
  // No provider-owned mutable object crosses the next await.
  return { status: String(value.status), stage: value.stage === 'preparing_references' ? value.stage : undefined,
    pollAfter: value.poll_after_seconds === undefined ? 10 : waitHint(value.poll_after_seconds),
    output: record(value.output) ? { url: value.output.url, expires: value.output.url_expires_at,
      retained: value.output.retained_until, contentType: value.output.content_type } : undefined };
}

function assetUrl(value: unknown, trusted: readonly string[]): string {
  if (typeof value !== 'string' || value.length > 8192) throw new Error('Invalid original asset');
  const url = new URL(value);
  if (url.protocol !== 'https:' || url.username || url.password || url.hash || (url.port && url.port !== '443') ||
      url.origin === 'https://api.cuddler.ai' || !trusted.includes(url.hostname)) throw new Error('Untrusted original asset');
  return value;
}

/** Never selected by node JSON, credentials, environment or a user-supplied URL. */
export function preparedReadCapability(options: PreparedReadOptions = {}) {
  const transport = options.transport ?? disabledPreparedReadTransport;
  const hosts = Object.freeze([...(options.trustedAssetHosts ?? [])]);
  if (hosts.some((host) => !/^(?:[a-z0-9-]+\.)+[a-z][a-z0-9-]*$/.test(host) ||
      /(?:^|\.)(?:localhost|local|internal)$/.test(host))) throw new Error('Invalid asset host policy');
  return async (context: IExecuteFunctions, index: number, input: RecoveryInput): Promise<Pick<INodeExecutionData, 'json' | 'binary'>> => {
    const original = await resolveOriginalPrepared(context, index, input);
    const identity = { ...original.identity, jobId: original.request.jobId };
    for (let attempt = 0; attempt < (input.operation === 'pollPrepared' ? 1 : 2); attempt++) {
      await original.recheck();
      const response = await transport.getOriginal(original.request);
      if (input.operation === 'pollPrepared' && [429, 503].includes(response.statusCode)) {
        const nextPollAfterSeconds = Math.max(10, retryHint(headers(response.headers)['retry-after']));
        await original.recheck();
        return { json: { ...identity, state: 'retry-later', terminal: false, nextPollAfterSeconds } };
      }
      if (response.statusCode !== 200) throw new Error('Original job could not be read');
      const current = job(response.job, input.media, original.request.jobId);
      if (input.operation === 'pollPrepared') {
        await original.recheck();
        const terminal = TERMINAL.has(current.status);
        const json: IDataObject = { ...identity, state: 'original-job', status: current.status, terminal };
        if (!terminal) json.nextPollAfterSeconds = Math.max(10, current.pollAfter);
        if (current.stage) json.stage = current.stage;
        return { json };
      }
      const settings = input.file;
      if (input.operation !== 'downloadPreparedFile' || !settings || current.status !== 'succeeded' || !current.output) throw new Error('Original file unavailable');
      const output = current.output;
      if (!Number.isFinite(timestamp(output.retained)) || timestamp(output.retained) <= Date.now()) throw new Error('Original file retention expired');
      if (!Number.isFinite(timestamp(output.expires))) throw new Error('Invalid file expiry');
      if (timestamp(output.expires) <= Date.now()) continue;
      const url = assetUrl(output.url, hosts);
      await original.recheck();
      const asset = await transport.readAsset(Object.freeze({ method: 'GET', url, headers: Object.freeze({}),
        credentials: 'omit', followRedirects: false, maxBytes: settings.maxBytes }));
      if ([401, 403, 404, 410].includes(asset.statusCode)) continue;
      if (asset.statusCode !== 200 || !Buffer.isBuffer(asset.body) || !asset.body.length || asset.body.length > settings.maxBytes) throw new Error('Incomplete original asset');
      const bytes = Buffer.from(asset.body);
      const metadata = headers(asset.headers);
      const contentType = mime(metadata['content-type'] ?? '');
      if (!TYPES[input.media].includes(contentType) ||
          (input.media !== 'video' && mime(String(output.contentType)) !== contentType)) throw new Error('Unexpected original media');
      if (metadata['content-length'] !== undefined && (!/^\d+$/.test(metadata['content-length']) || Number(metadata['content-length']) !== bytes.length)) throw new Error('Incomplete original asset');
      const sha256 = Buffer.from(await crypto.subtle.digest('SHA-256', new Uint8Array(bytes))).toString('hex');
      await original.recheck();
      const extension = ({ 'image/png': 'png', 'image/jpeg': 'jpg', 'image/webp': 'webp', 'video/mp4': 'mp4', 'video/quicktime': 'mov', 'audio/mpeg': 'mp3' } as Record<string, string>)[contentType];
      const filename = `${original.request.jobId}.${extension}`;
      const binary = await context.helpers.prepareBinaryData(Buffer.from(bytes), filename, contentType);
      await original.recheck();
      return { json: { ...identity, state: 'file-delivered', status: current.status, bytes: bytes.length, sha256, contentType, filename },
        binary: { [settings.property]: binary } };
    }
    throw new Error('Original file link unavailable');
  };
}
