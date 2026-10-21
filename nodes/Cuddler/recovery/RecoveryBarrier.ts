import { canonical, prepare, stableSlot, verify, type Envelope, type Media, type Scope } from './ImmutablePlan';

interface Entry { planJson: string; knownJobId: string | null }
interface Receipt { entry: Entry; durableReadback: true }

/** Internal capability implemented by the real native CAS class, never input JSON. */
export interface PlanStore {
  read(slot: string): Promise<Receipt | undefined>;
  saveOnce(slot: string, planJson: string): Promise<Receipt>;
  recordKnownJob(slot: string, planJson: string, jobId: string): Promise<Receipt>;
}

export interface ApprovedOperation { media: Media; body: unknown; scope: Scope }
export interface OpenedOperation { envelope: Envelope; knownJobId: string | null }

/** No networking or side effects other than the injected native ledger capability. */
export class RecoveryBarrier {
  constructor(private readonly store: PlanStore) {}

  private async validated(
    receipt: Receipt | undefined, approved: ApprovedOperation, apiKey: string,
  ): Promise<OpenedOperation> {
    if (!receipt || receipt.durableReadback !== true || typeof receipt.entry.planJson !== 'string') throw new Error('Complete persistent plan readback is required');
    const envelope = await verify(JSON.parse(receipt.entry.planJson), approved.media, approved.body, approved.scope, apiKey);
    if (canonical(envelope) !== receipt.entry.planJson) throw new Error('Persistent plan representation mismatch');
    return { envelope, knownJobId: receipt.entry.knownJobId };
  }

  async prepareAndPersist(
    approved: ApprovedOperation, apiKey: string, hostMode: string,
  ): Promise<OpenedOperation> {
    const frozen = JSON.parse(canonical(approved)) as ApprovedOperation;
    const candidate = await prepare(frozen.media, frozen.body, frozen.scope, apiKey, hostMode, 'new');
    const saved = await this.store.saveOnce(candidate.payload.slot, canonical(candidate));
    // Ignore the candidate if another worker already won. Verify the complete
    // winner against the exact original request, budget, scope and credential.
    await this.validated(saved, frozen, apiKey);
    const readback = await this.store.read(candidate.payload.slot);
    if (!readback || readback.entry.planJson !== saved.entry.planJson) throw new Error('Persistent winner readback mismatch');
    return await this.validated(readback, frozen, apiKey);
  }

  async resumeExplicit(
    approved: ApprovedOperation, apiKey: string, hostMode: string,
  ): Promise<OpenedOperation> {
    // Existing-plan read/verify only. Retry mode is never an instruction to
    // prepare: the node must first authenticate its retained opaque handle.
    if (hostMode === 'retry' && !approved?.scope) throw new Error('Original persistent scope is required');
    const frozen = JSON.parse(canonical(approved)) as ApprovedOperation;
    const slot = await stableSlot(frozen.scope);
    return await this.validated(await this.store.read(slot), frozen, apiKey);
  }

  async recordOriginalJob(
    approved: ApprovedOperation, apiKey: string, hostMode: string, jobId: string,
  ): Promise<OpenedOperation> {
    const frozen = JSON.parse(canonical(approved)) as ApprovedOperation;
    const current = await this.resumeExplicit(frozen, apiKey, hostMode);
    if (current.knownJobId && current.knownJobId !== jobId) throw new Error('Original job ID cannot change');
    const slot = current.envelope.payload.slot;
    await this.store.recordKnownJob(slot, canonical(current.envelope), jobId);
    const readback = await this.store.read(slot);
    const confirmed = await this.validated(readback, frozen, apiKey);
    if (confirmed.knownJobId !== jobId) throw new Error('Original job readback mismatch');
    return confirmed;
  }
}
