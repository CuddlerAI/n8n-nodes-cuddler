import type { Media } from './ImmutablePlan';

export interface RecoveryJob {
  id: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed' | 'canceled';
}
export interface FrozenCreateRequest {
  origin: 'https://api.cuddler.ai';
  method: 'POST';
  path: '/v1/videos' | '/v1/images' | '/v1/speech' | '/v1/music';
  media: Media;
  bodyJson: string;
  idempotencyKey: string;
  credentialBinding: string;
}
export interface OriginalJobRequest {
  origin: 'https://api.cuddler.ai';
  method: 'GET';
  path: string;
  media: Media;
  jobId: string;
  credentialBinding: string;
}
/** Constructor-only capability. Never selected by a workflow parameter. */
export interface RecoveryTransport {
  create(request: FrozenCreateRequest): Promise<RecoveryJob>;
  get(request: OriginalJobRequest): Promise<RecoveryJob>;
}
export const disabledRecoveryTransport: RecoveryTransport = Object.freeze({
  async create(): Promise<RecoveryJob> { throw new Error('Paid network transport is not enabled'); },
  async get(): Promise<RecoveryJob> { throw new Error('Recovery network transport is not enabled'); },
});
