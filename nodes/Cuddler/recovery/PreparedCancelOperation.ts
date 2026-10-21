import type { IDataObject, IExecuteFunctions } from 'n8n-workflow';
import { resolveOriginalPrepared, type RecoveryInput } from './RecoveryOperation';
import { disabledPreparedCancelTransport, type PreparedCancelOptions } from './PreparedCancelTransport';

const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
const statuses = ['queued', 'running', 'succeeded', 'failed', 'canceled'];
export class CancelRefusal extends Error {
  constructor(readonly code: string) { super(code); }
}
const refuse = (code: string): never => { throw new CancelRefusal(code); };

/** Cancel the protected original job only. Never creates, replaces, or claims settlement. */
export function preparedCancelCapability(options: PreparedCancelOptions = {}) {
  const transport = options.transport ?? disabledPreparedCancelTransport;
  return async (context: IExecuteFunctions, index: number, input: RecoveryInput): Promise<IDataObject> => {
    try {
      if (input.operation !== 'cancelPrepared' || input.media === 'image') return refuse('CANCEL_UNSUPPORTED_MEDIA');
      if (context.getMode() === 'retry') return refuse('AUTOMATIC_RETRY_UNSUPPORTED');
      const original = await resolveOriginalPrepared(context, index, input);
      const status = (value: unknown): string => {
        if (!record(value) || value.object !== input.media || value.id !== original.request.jobId ||
            typeof value.status !== 'string' || !statuses.includes(value.status)) return refuse('CANCEL_INVALID_ORIGINAL_RESPONSE');
        return value.status;
      };
      await original.recheck();
      const read = await transport.getOriginal(original.request);
      if (read.statusCode !== 200) return refuse('CANCEL_ORIGINAL_GET_FAILED');
      const before = status(read.job);
      await original.recheck();
      if (before === 'canceled') return { ...original.identity, state: 'original-job', jobId: original.request.jobId,
        status: 'canceled', cancelRequested: false };
      if (before !== 'queued') return refuse('CANCEL_ONLY_QUEUED');
      // The server atomically enforces queued -> canceled. A concurrent admission
      // returns 409; it must never become a retry, replacement create or refund claim.
      const result = await transport.cancelQueued(Object.freeze({ ...original.request, method: 'POST',
        path: original.request.path + '/cancel', media: input.media }));
      if (result.statusCode === 409) return refuse('CANCEL_NOT_CANCELABLE');
      if (result.statusCode !== 200) return refuse('CANCEL_REQUEST_FAILED');
      if (status(result.job) !== 'canceled') return refuse('CANCEL_UNCONFIRMED');
      await original.recheck();
      return { ...original.identity, state: 'original-job', jobId: original.request.jobId,
        status: 'canceled', cancelRequested: true };
    } catch (error) {
      if (error instanceof CancelRefusal) return refuse(error.code);
      return refuse('CANCEL_RECOVERY_FAILED');
    }
  };
}
