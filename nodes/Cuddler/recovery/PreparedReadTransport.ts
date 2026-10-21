import type { OriginalJobRequest } from './RecoveryTransport';

export interface OriginalReadResponse {
  statusCode: number;
  headers?: Readonly<Record<string, string>>;
  job?: unknown;
}

export interface OriginalAssetRequest {
  method: 'GET';
  url: string;
  headers: Readonly<Record<string, string>>;
  credentials: 'omit';
  followRedirects: false;
  maxBytes: number;
}

/** Installed-code capability only. The asset adapter must bound bytes while reading. */
export interface PreparedReadTransport {
  getOriginal(request: Readonly<OriginalJobRequest>): Promise<OriginalReadResponse>;
  readAsset(request: Readonly<OriginalAssetRequest>): Promise<{
    statusCode: number; headers: Readonly<Record<string, string>>; body: Buffer;
  }>;
}

export interface PreparedReadOptions {
  transport?: PreparedReadTransport;
  trustedAssetHosts?: readonly string[];
}

export const disabledPreparedReadTransport: PreparedReadTransport = Object.freeze({
  async getOriginal(): Promise<OriginalReadResponse> { throw new Error('Original read transport is not enabled'); },
  async readAsset(): Promise<never> { throw new Error('Asset transport is not enabled'); },
});
