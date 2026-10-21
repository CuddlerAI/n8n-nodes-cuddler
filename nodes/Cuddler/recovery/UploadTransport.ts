export interface UploadApiRequest {
  origin: 'https://api.cuddler.ai';
  method: 'GET' | 'POST';
  path: string;
  credentialBinding: string;
  body?: Readonly<{ kind: string; content_type: string; bytes: number }>;
  idempotencyKey?: string;
}

/** Constructor-only offline capability; never provided by workflow JSON. */
export interface UploadTransport {
  api(request: Readonly<UploadApiRequest>): Promise<{ statusCode: number; upload?: unknown }>;
  put(request: Readonly<{ method: 'PUT'; url: string; headers: Readonly<Record<string, string>>;
    body: Buffer; maxBytes: number; credentials: 'omit'; followRedirects: false }>): Promise<{ statusCode: number }>;
}

export interface UploadOptions { transport?: UploadTransport; trustedAssetHosts?: readonly string[] }

export const disabledUploadTransport: UploadTransport = Object.freeze({
  async api(): Promise<never> { throw new Error('Upload network transport is disabled'); },
  async put(): Promise<never> { throw new Error('Upload asset transport is disabled'); },
});
