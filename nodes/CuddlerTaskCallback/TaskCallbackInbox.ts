import type { DataTableRowReturn, IDataTableProjectService } from 'n8n-workflow';
import { generateSecureToken, sleep } from 'n8n-workflow';
import { canonical, digest, exact, fail, identity, LEASE_MS, MAX_BODY, MAX_RECEIPT, record, safeEvent, VERSION, type SafeEvent } from './TaskCallbackContract';
import { TaskCallbackProtector } from './TaskCallbackProtector';

export const INBOX_FORMAT = 'cuddler.n8n.per-task-inbox/2';
export interface Lease { token: string; executionId: string; until: number }
interface State {
  version: typeof VERSION; slot: string; event: SafeEvent; identity: string;
  status: 'pending' | 'leased' | 'completed'; lease: Lease | null; receivedAt: number; completedAt: number | null;
}
interface Ledger { format: typeof INBOX_FORMAT; slots: Record<string, string> }
interface Row { row: DataTableRowReturn; ledger: Ledger }
export interface ClaimResult { outcome: 'accepted' | 'busy' | 'duplicate'; event: SafeEvent; slot: string; lease: Lease | null }

/** One preprovisioned native row, bounded whole-row etag CAS; never staticData or a process cache. */
export class TaskCallbackInbox {
  constructor(
    private readonly table: Pick<IDataTableProjectService, 'getManyRowsAndCount' | 'updateRows'>,
    private readonly protector: TaskCallbackProtector,
    private readonly recheck: () => Promise<void>,
  ) {}

  async slot(event: SafeEvent): Promise<string> {
    const b = this.protector.binding;
    // Stable across API key/secret changes; unreadable old state fails closed instead of making another inbox.
    return await digest(canonical({ version: INBOX_FORMAT, workflowId: b.workflowId, triggerNodeId: b.triggerNodeId, callbackDigest: b.callbackDigest, eventId: event.eventId }));
  }

  private async read(): Promise<Row> {
    await this.recheck();
    const result = await this.table.getManyRowsAndCount({ filter: { type: 'and', filters: [{ columnName: 'id', condition: 'eq', value: this.protector.binding.rowId }] }, take: 2 });
    await this.recheck();
    if (result.count !== 1 || result.data.length !== 1) fail();
    const row = result.data[0];
    if (row.id !== this.protector.binding.rowId || typeof row.etag !== 'string' || !/^[a-f0-9]{64}$/.test(row.etag) || typeof row.payload !== 'string' || Buffer.byteLength(row.payload) > MAX_BODY) fail();
    const parsed: unknown = JSON.parse(row.payload);
    exact(parsed, 'format,slots');
    if (parsed.format !== INBOX_FORMAT || !record(parsed.slots) || Object.keys(parsed.slots).length > 256) fail();
    for (const [slot, value] of Object.entries(parsed.slots)) if (!/^[a-f0-9]{64}$/.test(slot) || typeof value !== 'string' || !value || Buffer.byteLength(value) > MAX_RECEIPT) fail();
    return { row, ledger: parsed as unknown as Ledger };
  }

  private async state(slot: string, box: string | undefined): Promise<State | undefined> {
    if (box === undefined) return undefined;
    const value = await this.protector.open('state', box);
    exact(value, 'version,slot,event,identity,status,lease,receivedAt,completedAt');
    const event = safeEvent(value.event);
    if (value.version !== VERSION || value.slot !== slot || await this.slot(event) !== slot || value.identity !== identity(event) ||
        !Number.isSafeInteger(value.receivedAt) || Number(value.receivedAt) <= 0 || !['pending', 'leased', 'completed'].includes(String(value.status))) fail();
    if (value.status === 'pending') { if (value.lease !== null || value.completedAt !== null) fail(); }
    else {
      exact(value.lease, 'token,executionId,until');
      if (typeof value.lease.token !== 'string' || !/^[a-f0-9]{64}$/.test(value.lease.token) || typeof value.lease.executionId !== 'string' || !value.lease.executionId || value.lease.executionId.length > 128 ||
          !Number.isSafeInteger(value.lease.until) || Number(value.lease.until) <= 0) fail();
      if (value.status === 'leased' ? value.completedAt !== null : (!Number.isSafeInteger(value.completedAt) || Number(value.completedAt) <= 0)) fail();
    }
    await this.recheck();
    return { ...value, event } as unknown as State;
  }

  private async mutate(slot: string, change: (old: State | undefined) => State): Promise<State> {
    for (let attempt = 0; attempt < 5; attempt++) {
      const { row, ledger } = await this.read();
      const old = await this.state(slot, ledger.slots[slot]);
      const next = change(old);
      if (old && canonical(old) === canonical(next)) return old;
      if (!old && Object.keys(ledger.slots).length >= 256) fail();
      const box = await this.protector.seal('state', next);
      const payload = canonical({ format: INBOX_FORMAT, slots: { ...ledger.slots, [slot]: box } });
      if (Buffer.byteLength(payload) > MAX_BODY) fail();
      const etag = generateSecureToken();
      await this.recheck();
      // Ignore returned update rows: only complete independent readback proves the CAS winner.
      await this.table.updateRows({ filter: { type: 'and', filters: [
        { columnName: 'id', condition: 'eq', value: this.protector.binding.rowId },
        { columnName: 'etag', condition: 'eq', value: row.etag },
      ] }, data: { etag, payload } });
      await this.recheck();
      const saved = await this.read();
      if (saved.row.etag === etag) {
        if (saved.row.payload !== payload) fail();
        const actual = await this.state(slot, saved.ledger.slots[slot]);
        if (!actual || canonical(actual) !== canonical(next)) fail();
        return actual;
      }
      // A concurrent update may have retained our slot; the next iteration authenticates it again.
      await sleep(20 * (attempt + 1));
    }
    return fail();
  }

  async claim(event: SafeEvent, executionId: string, verifiedAt: number): Promise<ClaimResult> {
    if (!executionId || executionId.length > 128) fail();
    const slot = await this.slot(event);
    const token = generateSecureToken();
    const result = await this.mutate(slot, (old) => {
      if (old && old.identity !== identity(event)) fail();
      if (old?.status === 'completed' || (old?.status === 'leased' && old.lease && old.lease.until > Date.now())) return old;
      return { version: VERSION, slot, event: old?.event ?? event, identity: identity(event), status: 'leased',
        lease: { token, executionId, until: Date.now() + LEASE_MS }, receivedAt: old?.receivedAt ?? verifiedAt, completedAt: null };
    });
    await this.recheck();
    if (result.status === 'completed') return { outcome: 'duplicate', event: result.event, slot, lease: null };
    if (result.lease?.token === token && result.lease.executionId === executionId && result.lease.until > Date.now()) return { outcome: 'accepted', event: result.event, slot, lease: result.lease };
    return { outcome: 'busy', event: result.event, slot, lease: null };
  }

  async completed(event: SafeEvent): Promise<boolean> {
    const slot = await this.slot(event);
    const current = await this.read();
    const state = await this.state(slot, current.ledger.slots[slot]);
    if (state && state.identity !== identity(event)) fail();
    return state?.status === 'completed';
  }

  /** Read current durable ownership, not a workflow JSON receipt or process cache. */
  async assertCurrentLease(event: SafeEvent, slot: string, lease: Lease, executionId: string): Promise<boolean> {
    if (lease.executionId !== executionId || lease.until <= Date.now() || await this.slot(event) !== slot) fail();
    const current = await this.read();
    const state = await this.state(slot, current.ledger.slots[slot]);
    if (!state || state.identity !== identity(event) || state.status === 'pending' || canonical(state.lease) !== canonical(lease) || lease.until <= Date.now()) fail();
    return state.status === 'completed';
  }

  async complete(event: SafeEvent, slot: string, lease: Lease, executionId: string): Promise<void> {
    if (lease.executionId !== executionId || lease.until <= Date.now() || await this.slot(event) !== slot) fail();
    const result = await this.mutate(slot, (old) => {
      if (!old || old.identity !== identity(event) || old.status === 'pending' || canonical(old.lease) !== canonical(lease) || lease.until <= Date.now()) fail();
      return old.status === 'completed' ? old : { ...old, status: 'completed', completedAt: Date.now() };
    });
    if (result.status !== 'completed' || lease.until <= Date.now()) fail();
    await this.recheck();
  }
}
