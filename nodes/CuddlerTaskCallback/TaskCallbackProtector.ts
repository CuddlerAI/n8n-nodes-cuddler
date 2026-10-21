import { callbackScope, canonical, digest, exact, fail, identifier, MAX_RECEIPT, validBinding, VERSION, type Binding, type CredentialSnapshot } from './TaskCallbackContract';

type Purpose = 'ingress' | 'delivery' | 'state';
const text = new TextEncoder();
const decode = (value: string): Uint8Array<ArrayBuffer> => Uint8Array.from(Buffer.from(value, 'base64url'));

/** Separate webhook domain. Child graphs and workflow items receive ciphertext only. */
export class TaskCallbackProtector {
  private constructor(readonly binding: Binding, private readonly encryption: CryptoKey) {}

  static async create(workflowId: string, triggerNodeId: string, credentials: CredentialSnapshot, callbackDigest: string): Promise<TaskCallbackProtector> {
    identifier(workflowId); identifier(triggerNodeId);
    if (callbackDigest !== await digest(credentials.callbackUrl)) fail();
    const binding: Binding = {
      workflowId, triggerNodeId, tableId: credentials.tableId, rowId: credentials.rowId,
      keyFingerprint: await digest(`${VERSION}/key\n${credentials.apiKey}`),
      secretFingerprint: await digest(`${VERSION}/secret\n${credentials.signingSecret}`),
      callbackDigest, callbackScope: await callbackScope(credentials, workflowId),
    };
    validBinding(binding);
    const material = await crypto.subtle.digest('SHA-256', text.encode(canonical({ version: VERSION, purpose: 'encryption', apiKey: credentials.apiKey, signingSecret: credentials.signingSecret })));
    const key = await crypto.subtle.importKey('raw', material, 'AES-GCM', false, ['encrypt', 'decrypt']);
    return new TaskCallbackProtector(Object.freeze(binding), key);
  }

  static peekBinding(value: unknown, purpose: Purpose): Binding {
    if (typeof value !== 'string' || !value || Buffer.byteLength(value) > MAX_RECEIPT) fail();
    const box: unknown = JSON.parse(value);
    exact(box, 'version,purpose,binding,iv,ciphertext');
    if (box.version !== VERSION || box.purpose !== purpose || typeof box.iv !== 'string' || !/^[A-Za-z0-9_-]{16}$/.test(box.iv) ||
        typeof box.ciphertext !== 'string' || !/^[A-Za-z0-9_-]{22,}$/.test(box.ciphertext)) fail();
    validBinding(box.binding);
    return JSON.parse(canonical(box.binding)) as Binding;
  }

  private aad(purpose: Purpose): Uint8Array<ArrayBuffer> { return text.encode(canonical({ version: VERSION, purpose, binding: this.binding })); }

  async seal(purpose: Purpose, value: unknown): Promise<string> {
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const bytes = text.encode(canonical(value));
    if (bytes.byteLength > MAX_RECEIPT / 2) fail();
    const ciphertext = await crypto.subtle.encrypt({ name: 'AES-GCM', iv, additionalData: this.aad(purpose) }, this.encryption, bytes);
    const result = canonical({ version: VERSION, purpose, binding: this.binding, iv: Buffer.from(iv).toString('base64url'), ciphertext: Buffer.from(ciphertext).toString('base64url') });
    if (Buffer.byteLength(result) > MAX_RECEIPT) fail();
    return result;
  }

  async open(purpose: Purpose, value: unknown): Promise<unknown> {
    const binding = TaskCallbackProtector.peekBinding(value, purpose);
    if (canonical(binding) !== canonical(this.binding)) fail();
    const box = JSON.parse(value as string) as { iv: string; ciphertext: string };
    const bytes = await crypto.subtle.decrypt({ name: 'AES-GCM', iv: decode(box.iv), additionalData: this.aad(purpose) }, this.encryption, decode(box.ciphertext));
    return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  }
}
