import type { Media } from './ImmutablePlan';
import type { OriginalJobRequest } from './RecoveryTransport';
import type { OriginalReadResponse } from './PreparedReadTransport';

export interface OriginalCancelRequest {
  origin: 'https://api.cuddler.ai';
  method: 'POST';
  path: string;
  media: Exclude<Media, 'image'>;
  jobId: string;
  credentialBinding: string;
}

/** Installed-code capability only; the protected node must read the original first. */
export interface PreparedCancelTransport {
  getOriginal(request: Readonly<OriginalJobRequest>): Promise<OriginalReadResponse>;
  cancelQueued(request: Readonly<OriginalCancelRequest>): Promise<OriginalReadResponse>;
}

export interface PreparedCancelOptions { transport?: PreparedCancelTransport }

export const disabledPreparedCancelTransport: PreparedCancelTransport = Object.freeze({
  async getOriginal(): Promise<never> { throw new Error('Original cancel transport is not enabled'); },
  async cancelQueued(): Promise<never> { throw new Error('Original cancel transport is not enabled'); },
});
