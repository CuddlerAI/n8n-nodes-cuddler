import type { DataTableRowReturn, IDataTableProjectService } from 'n8n-workflow';
import { generateSecureToken, sleep } from 'n8n-workflow';

export const LEDGER_FORMAT = 'cuddler.n8n.cas-ledger/1';

export class OriginalJobConflictError extends Error {
  readonly code = 'ORIGINAL_JOB_CONFLICT';
  constructor() { super('Original job ID cannot change'); }
}

export interface StoreEntry {
  planJson: string;
  knownJobId: string | null;
}

interface Ledger {
  format: typeof LEDGER_FORMAT;
  slots: Record<string, StoreEntry>;
}

export interface StoreReceipt {
  entry: StoreEntry;
  etag: string;
  outcome: 'existing' | 'saved' | 'other-winner';
  attempts: number;
  durableReadback: true;
}

export interface StoreObservation {
  kind: 'read' | 'cas' | 'readback';
  attempt: number;
  etag: string;
  requestedEtag?: string;
}

const MAX_LEDGER_BYTES = 1024 * 1024;
const MAX_PLAN_BYTES = 64 * 1024;
const MAX_ATTEMPTS = 5;
const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value);
const has = (value: object, key: string) => Object.prototype.hasOwnProperty.call(value, key);

/**
 * Native DataTable CAS capability, separate from plan authentication/encryption.
 * Uses one explicitly provisioned row, never upsert or in-memory persistence.
 * This adapter consumes actual built-in child executions, not restricted custom proxies.
 * A receipt is returned only after reading complete persisted bytes back.
 */
export class NativeDataTableCasStore {
  constructor(
    private readonly table: Pick<IDataTableProjectService, 'getManyRowsAndCount' | 'updateRows'>,
    private readonly rowId: number,
    private readonly observe: (event: StoreObservation) => void = () => {},
  ) {
    if (!Number.isSafeInteger(rowId) || rowId < 1) throw new Error('Invalid ledger row');
  }

  private validateSlot(slot: string): void {
    if (!/^[a-f0-9]{64}$/.test(slot)) throw new Error('Invalid stable operation slot');
  }

  private decode(row: DataTableRowReturn): Ledger {
    if (row.id !== this.rowId || typeof row.etag !== 'string' || !/^[a-f0-9]{64}$/.test(row.etag)) {
      throw new Error('Invalid persistent ledger identity');
    }
    if (typeof row.payload !== 'string' || Buffer.byteLength(row.payload) > MAX_LEDGER_BYTES) {
      throw new Error('Invalid persistent ledger size');
    }
    const value: unknown = JSON.parse(row.payload);
    if (!isRecord(value) || value.format !== LEDGER_FORMAT || !isRecord(value.slots)) {
      throw new Error('Unsupported persistent ledger format');
    }
    for (const [slot, entry] of Object.entries(value.slots)) {
      this.validateSlot(slot);
      if (!isRecord(entry) || typeof entry.planJson !== 'string' ||
          Buffer.byteLength(entry.planJson) > MAX_PLAN_BYTES ||
          (entry.knownJobId !== null && typeof entry.knownJobId !== 'string')) {
        throw new Error('Invalid persistent operation entry');
      }
    }
    return value as unknown as Ledger;
  }

  private async row(attempt: number, kind: 'read' | 'readback'): Promise<{ row: DataTableRowReturn; ledger: Ledger }> {
    const result = await this.table.getManyRowsAndCount({
      filter: { type: 'and', filters: [{ columnName: 'id', condition: 'eq', value: this.rowId }] },
      take: 2,
    });
    if (result.count !== 1 || result.data.length !== 1) throw new Error('Missing or ambiguous persistent ledger');
    const row = result.data[0];
    const ledger = this.decode(row);
    this.observe({ kind, attempt, etag: row.etag as string });
    return { row, ledger };
  }

  async read(slot: string): Promise<StoreReceipt | undefined> {
    this.validateSlot(slot);
    const { row, ledger } = await this.row(0, 'readback');
    if (!has(ledger.slots, slot)) return undefined;
    return { entry: { ...ledger.slots[slot] }, etag: row.etag as string, outcome: 'existing', attempts: 0, durableReadback: true };
  }

  async saveOnce(slot: string, planJson: string): Promise<StoreReceipt> {
    this.validateSlot(slot);
    if (!planJson || Buffer.byteLength(planJson) > MAX_PLAN_BYTES) throw new Error('Invalid immutable plan size');
    return await this.mutate(slot, (existing) => existing ?? { planJson, knownJobId: null });
  }

  async recordKnownJob(slot: string, planJson: string, jobId: string): Promise<StoreReceipt> {
    this.validateSlot(slot);
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(jobId)) throw new Error('Invalid original job ID');
    return await this.mutate(slot, (existing) => {
      if (!existing || existing.planJson !== planJson) throw new Error('Original immutable plan mismatch');
      if (existing.knownJobId && existing.knownJobId !== jobId) throw new OriginalJobConflictError();
      return { ...existing, knownJobId: jobId };
    });
  }

  private async mutate(
    slot: string,
    mutation: (existing: StoreEntry | undefined) => StoreEntry,
  ): Promise<StoreReceipt> {
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      const { row, ledger } = await this.row(attempt, 'read');
      const existing = has(ledger.slots, slot) ? ledger.slots[slot] : undefined;
      const entry = mutation(existing);
      if (existing && existing.planJson === entry.planJson && existing.knownJobId === entry.knownJobId) {
        return { entry: { ...existing }, etag: row.etag as string, outcome: 'existing', attempts: attempt, durableReadback: true };
      }
      const next: Ledger = { format: LEDGER_FORMAT, slots: { ...ledger.slots, [slot]: entry } };
      const payload = JSON.stringify(next);
      if (Buffer.byteLength(payload) > MAX_LEDGER_BYTES) throw new Error('Persistent operation ledger is full');
      const etag = generateSecureToken();
      this.observe({ kind: 'cas', attempt, etag: row.etag as string, requestedEtag: etag });
      // In fixed 2.43.7 this filter remains in the final native SQL UPDATE.
      // Ignore updateRows' returned rows: SQLite may have selected them before UPDATE.
      await this.table.updateRows({
        filter: { type: 'and', filters: [
          { columnName: 'id', condition: 'eq', value: this.rowId },
          { columnName: 'etag', condition: 'eq', value: row.etag },
        ] },
        data: { etag, payload },
      });
      const readback = await this.row(attempt, 'readback');
      if (readback.row.etag === etag) {
        if (readback.row.payload !== payload) throw new Error('Persistent ledger readback mismatch');
        return { entry: { ...entry }, etag, outcome: 'saved', attempts: attempt, durableReadback: true };
      }
      if (has(readback.ledger.slots, slot)) {
        const winner = readback.ledger.slots[slot];
        // Re-check immutable plan and monotonic job semantics through the mutation.
        const requested = mutation(winner);
        if (requested.planJson === winner.planJson && requested.knownJobId === winner.knownJobId) {
          return { entry: { ...winner }, etag: readback.row.etag as string, outcome: 'other-winner', attempts: attempt, durableReadback: true };
        }
      }
      await sleep(attempt * 20);
    }
    throw new Error('Persistent ledger contention limit reached');
  }
}
