import { canonical } from './ImmutablePlan';
import type { PlanStore } from './RecoveryBarrier';
import { NativeDataTableCasStore, type StoreReceipt } from './NativeDataTableCasStore';

const VERSION = 'cuddler.n8n.protected-state/1';
const text = new TextEncoder();
const hex = (bytes: ArrayBuffer) => Buffer.from(bytes).toString('hex');
const decode = (value: string): Uint8Array<ArrayBuffer> => Uint8Array.from(Buffer.from(value, 'base64url'));
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);

export interface LedgerBinding { workflowId: string; tableId: string; rowId: number }

/** Each native child receives ciphertext only, never a prompt or signed plan. */
export class ProtectedPlanStore implements PlanStore {
  private constructor(
    private readonly native: NativeDataTableCasStore,
    private readonly binding: LedgerBinding,
    private readonly encryption: CryptoKey,
    private readonly signer: CryptoKey,
  ) {}

  static async open(native: NativeDataTableCasStore, binding: LedgerBinding, apiKey: string): Promise<ProtectedPlanStore> {
    const frozen = JSON.parse(canonical(binding)) as LedgerBinding;
    if (!frozen.workflowId || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(frozen.tableId) ||
        !Number.isSafeInteger(frozen.rowId) || frozen.rowId < 1 || !apiKey || apiKey.length > 4096 || apiKey.trim() !== apiKey || /[\r\n]/.test(apiKey)) throw new Error('Protected ledger binding is unavailable');
    const root = await crypto.subtle.importKey('raw', text.encode(apiKey), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    const derive = async (label: string) => await crypto.subtle.sign('HMAC', root, text.encode(`${VERSION}/${label}`));
    const encryption = await crypto.subtle.importKey('raw', await derive('encryption'), 'AES-GCM', false, ['encrypt', 'decrypt']);
    const signer = await crypto.subtle.importKey('raw', await derive('original-job'), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
    return new ProtectedPlanStore(native, frozen, encryption, signer);
  }

  private aad(kind: 'plan' | 'handle', slot: string): Uint8Array<ArrayBuffer> {
    if (!/^[a-f0-9]{64}$/.test(slot)) throw new Error('Invalid persistent slot');
    return text.encode(canonical({ version: VERSION, kind, slot, ...this.binding }));
  }

  async seal(kind: 'plan' | 'handle', slot: string, plaintext: string): Promise<string> {
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const ciphertext = await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: this.aad(kind, slot) }, this.encryption, text.encode(plaintext));
    return canonical({ version: VERSION, slot, iv: Buffer.from(iv).toString('base64url'), ciphertext: Buffer.from(ciphertext).toString('base64url') });
  }

  async unseal(kind: 'plan' | 'handle', value: string, expectedSlot?: string): Promise<{ slot: string; plaintext: string }> {
    if (typeof value !== 'string' || Buffer.byteLength(value) > 128 * 1024) throw new Error('Invalid protected state');
    const box: unknown = JSON.parse(value);
    if (!record(box) || Object.keys(box).sort().join(',') !== 'ciphertext,iv,slot,version' || box.version !== VERSION ||
        typeof box.slot !== 'string' || typeof box.iv !== 'string' || !/^[A-Za-z0-9_-]{16}$/.test(box.iv) ||
        typeof box.ciphertext !== 'string' || !/^[A-Za-z0-9_-]+$/.test(box.ciphertext) || (expectedSlot !== undefined && box.slot !== expectedSlot)) throw new Error('Invalid protected state');
    const plaintext = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: decode(box.iv), additionalData: this.aad(kind, box.slot) }, this.encryption, decode(box.ciphertext));
    return { slot: box.slot, plaintext: new TextDecoder('utf-8', { fatal: true }).decode(plaintext) };
  }

  private async jobTag(slot: string, planJson: string, jobId: string): Promise<string> {
    // Hash the complete exact plan bytes before embedding them in the MAC input.
    // Re-escaping a large valid plan must not fail only after create succeeds.
    const planSha256 = hex(await crypto.subtle.digest('SHA-256', text.encode(planJson)));
    return hex(await crypto.subtle.sign('HMAC', this.signer, text.encode(canonical({ version: VERSION, slot, planSha256, jobId, ...this.binding }))));
  }

  private async unpack(slot: string, receipt: StoreReceipt) {
    const opened = await this.unseal('plan', receipt.entry.planJson, slot);
    let knownJobId: string | null = null;
    if (receipt.entry.knownJobId !== null) {
      const match = /^([A-Za-z0-9][A-Za-z0-9_-]{0,62})\.([a-f0-9]{64})$/.exec(receipt.entry.knownJobId);
      if (!match || await this.jobTag(slot, opened.plaintext, match[1]) !== match[2]) throw new Error('Original job authentication failed');
      knownJobId = match[1];
    }
    return { entry: { planJson: opened.plaintext, knownJobId }, durableReadback: true as const };
  }

  async read(slot: string) {
    const receipt = await this.native.read(slot);
    return receipt ? await this.unpack(slot, receipt) : undefined;
  }

  async saveOnce(slot: string, planJson: string) {
    // Encryption happens before the plan can enter the child graph's parameters.
    return await this.unpack(slot, await this.native.saveOnce(slot, await this.seal('plan', slot, planJson)));
  }

  async recordKnownJob(slot: string, planJson: string, jobId: string) {
    if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$/.test(jobId)) throw new Error('Invalid original job identity');
    const current = await this.native.read(slot);
    if (!current) throw new Error('Original plan missing');
    const opened = await this.unpack(slot, current);
    if (opened.entry.planJson !== planJson) throw new Error('Original plan authentication mismatch');
    const tagged = `${jobId}.${await this.jobTag(slot, planJson, jobId)}`;
    return await this.unpack(slot, await this.native.recordKnownJob(slot, current.entry.planJson, tagged));
  }
}
