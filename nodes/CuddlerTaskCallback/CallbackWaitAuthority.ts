import type { IDataObject, IExecuteFunctions, INodeExecutionData, NodeTypeAndVersion } from 'n8n-workflow';
import { canonical, stableSlot, verify, type Media, type Scope } from '../Cuddler/recovery/ImmutablePlan';
import { BuiltinDataTableBridge } from '../Cuddler/recovery/BuiltinDataTableBridge';
import { NativeDataTableCasStore } from '../Cuddler/recovery/NativeDataTableCasStore';
import { ProtectedPlanStore } from '../Cuddler/recovery/ProtectedPlanStore';
import { callbackScope, credentials, digest, exact, fail, identity, record, safeEvent, sameCredentials, type CredentialSnapshot, type SafeEvent } from './TaskCallbackContract';
import { TaskCallbackProtector } from './TaskCallbackProtector';
import { TaskCallbackInbox, type Lease } from './TaskCallbackInbox';

const VERSION = 'cuddler.n8n.single-wait/1';
const PREFIX = 'cuddler-wait1.';
const RECOVERY_VERSION = 'cuddler.n8n.original-event-consumption/1';
export type OriginalJobReader = (handle: string, media: Media) => Promise<Pick<INodeExecutionData, 'json' | 'binary'>>;
const JOBS: Record<Media, RegExp> = { video: /^vid_[A-Za-z0-9]{12,40}$/, image: /^img_[A-Za-z0-9]{12,40}$/, speech: /^aud_[A-Za-z0-9]{12,40}$/, music: /^mus_[A-Za-z0-9]{20}$/ };
interface Original { handle: string; slot: string; operationId: string; media: Media; jobId: string }
interface Mapping extends Original {
  version: typeof VERSION; workflowId: string; callbackScope: string; mapperNodeId: string;
  waitExecutionId: string; waitName: string; waitVersion: number; waitParametersDigest: string;
  instanceOrigin: string; resumeUrl: string;
}
interface MediaCredentials { apiKey: string; tableId: string; rowId: number }

function mediaCredentials(value: unknown): MediaCredentials {
  if (!record(value) || typeof value.apiKey !== 'string' || typeof value.ledgerTableId !== 'string' ||
      typeof value.ledgerRowId !== 'string' || !/^[1-9][0-9]{0,14}$/.test(value.ledgerRowId) || !Number.isSafeInteger(Number(value.ledgerRowId))) fail();
  return { apiKey: value.apiKey, tableId: value.ledgerTableId, rowId: Number(value.ledgerRowId) };
}

/** Scope is same workflow definition, separate original-job execution and receiver execution. */
export class CallbackWaitAuthority {
  private constructor(
    private readonly context: IExecuteFunctions, private readonly captured: CredentialSnapshot,
    private readonly mediaCredentials: MediaCredentials, private readonly workflowId: string,
    private readonly executionId: string, private readonly nodeId: string,
    private readonly bridge: BuiltinDataTableBridge, private readonly maps: ProtectedPlanStore,
    private readonly readOriginal: OriginalJobReader,
  ) {}

  static async open(context: IExecuteFunctions, readOriginal: OriginalJobReader): Promise<CallbackWaitAuthority> {
    const workflowId = context.getWorkflow().id;
    const executionId = context.getExecutionId();
    const nodeId = context.getNode().id;
    if (!workflowId || !/^[1-9][0-9]{0,18}$/.test(executionId) || !nodeId || context.continueOnFail() ||
        context.getNode().retryOnFail || context.getNode().continueOnFail ||
        (context.getNode().onError && context.getNode().onError !== 'stopWorkflow') ||
        (context.getWorkflowSettings() as Record<string, unknown>).engineType === 'v2') fail();
    const captured = credentials(await context.getCredentials('cuddlerTaskCallbackApi'));
    const media = mediaCredentials(await context.getCredentials('cuddlerApi'));
    if (captured.apiKey !== media.apiKey ||
        (captured.waitTableId === media.tableId && captured.waitRowId === media.rowId) ||
        (captured.tableId === media.tableId && captured.rowId === media.rowId)) fail();
    const bridge = new BuiltinDataTableBridge(context, []);
    const native = new NativeDataTableCasStore(bridge.capability(captured.waitTableId), captured.waitRowId);
    const maps = await ProtectedPlanStore.open(native, { workflowId, tableId: captured.waitTableId, rowId: captured.waitRowId }, captured.apiKey);
    const result = new CallbackWaitAuthority(context, captured, media, workflowId, executionId, nodeId, bridge, maps, readOriginal);
    await result.recheck();
    return result;
  }

  private async recheck(): Promise<void> {
    if (this.context.getWorkflow().id !== this.workflowId || this.context.getExecutionId() !== this.executionId || this.context.getNode().id !== this.nodeId ||
        !sameCredentials(this.captured, credentials(await this.context.getCredentials('cuddlerTaskCallbackApi'))) ||
        canonical(this.mediaCredentials) !== canonical(mediaCredentials(await this.context.getCredentials('cuddlerApi')))) fail();
  }

  async setup(): Promise<IDataObject> {
    const scope = await callbackScope(this.captured, this.workflowId);
    await this.recheck();
    return { object: 'cuddler.callback-setup', callbackUrl: this.captured.callbackUrl, callbackScope: scope };
  }

  private async original(handle: string): Promise<Original> {
    if (!handle.startsWith('cuddler-op1.') || handle.length > 16384 || !/^[A-Za-z0-9_-]+$/.test(handle.slice(12))) fail();
    const native = new NativeDataTableCasStore(this.bridge.capability(this.mediaCredentials.tableId), this.mediaCredentials.rowId);
    const plans = await ProtectedPlanStore.open(native, { workflowId: this.workflowId, tableId: this.mediaCredentials.tableId, rowId: this.mediaCredentials.rowId }, this.captured.apiKey);
    const opened = await plans.unseal('handle', Buffer.from(handle.slice(12), 'base64url').toString('utf8'));
    const pointer: unknown = JSON.parse(opened.plaintext);
    exact(pointer, 'version,scope,slot,media,operationId,prepareExecutionId');
    if (pointer.version !== 'cuddler.n8n.operation-handle/1' || !record(pointer.scope) || pointer.scope.workflowId !== this.workflowId ||
        typeof pointer.operationId !== 'string' || typeof pointer.prepareExecutionId !== 'string' || !pointer.prepareExecutionId ||
        typeof pointer.media !== 'string' || !(pointer.media in JOBS) || pointer.slot !== opened.slot || await stableSlot(pointer.scope as unknown as Scope) !== opened.slot) fail();
    const saved = await plans.read(opened.slot);
    if (!saved || !saved.entry.knownJobId || !JOBS[pointer.media as Media].test(saved.entry.knownJobId)) fail();
    const raw: unknown = JSON.parse(saved.entry.planJson);
    if (!record(raw) || !record(raw.payload) || typeof raw.payload.requestJson !== 'string') fail();
    const body: unknown = JSON.parse(raw.payload.requestJson);
    if (!record(body) || body.webhook_url !== this.captured.callbackUrl || !record(body.metadata) ||
        body.metadata.cuddler_callback_scope !== await callbackScope(this.captured, this.workflowId)) fail();
    const plan = await verify(raw, pointer.media as Media, body, pointer.scope as unknown as Scope, this.captured.apiKey);
    if (plan.payload.operationId !== pointer.operationId) fail();
    await this.recheck();
    return { handle, slot: opened.slot, operationId: pointer.operationId, media: pointer.media as Media, jobId: saved.entry.knownJobId };
  }

  private singleWait(direction: 'before' | 'after'): NodeTypeAndVersion {
    const parents = this.context.getParentNodes(this.context.getNode().name, { includeNodeParameters: true });
    const children = this.context.getChildNodes(this.context.getNode().name, { includeNodeParameters: true });
    const waiting = [...parents, ...children].filter((node) => node.type === 'n8n-nodes-base.wait');
    const expected = (direction === 'before' ? children : parents).filter((node) => node.type === 'n8n-nodes-base.wait');
    if (waiting.length !== 1 || expected.length !== 1) fail();
    const wait = expected[0];
    const p = wait.parameters;
    const options: unknown = p?.options;
    // Stock WaitingWebhooks may disable the already resumed stack node; do not use that flag as a fresh generation token.
    // The official Workflow removes responseCode when responseNode hides it.
    // Missing is not a 503 promise: durable Inbox readback, never HTTP status, authorizes ACK.
    if ((direction === 'before' && wait.disabled) || ![1, 1.1, 1.2].includes(wait.typeVersion) || !p || p.resume !== 'webhook' ||
        p.httpMethod !== 'POST' || p.responseMode !== 'responseNode' ||
        (p.responseCode !== undefined && p.responseCode !== 503) || p.limitWaitTime !== true ||
        p.limitType !== 'afterTimeInterval' || p.resumeUnit !== 'seconds' || typeof p.resumeAmount !== 'number' ||
        p.resumeAmount < 1 || p.resumeAmount > 300 ||
        (p.incomingAuthentication !== undefined && p.incomingAuthentication !== 'none') ||
        (p.authentication !== undefined && p.authentication !== 'none') ||
        (record(options) && options.webhookSuffix !== undefined && options.webhookSuffix !== '')) fail();
    return JSON.parse(canonical(wait)) as NodeTypeAndVersion;
  }

  private target(value: string, executionId: string, expectedOrigin?: string): URL {
    const base = new URL(this.context.getInstanceBaseUrl());
    const url = new URL(value);
    if ((base.protocol !== 'https:' && !(base.protocol === 'http:' && base.hostname === '127.0.0.1')) || base.username || base.password ||
        url.origin !== base.origin || (expectedOrigin !== undefined && url.origin !== expectedOrigin) || url.username || url.password || url.hash ||
        url.pathname !== `/webhook-waiting/${executionId}` || [...url.searchParams.keys()].join(',') !== 'signature' ||
        !/^[a-f0-9]{64}$/.test(url.searchParams.get('signature') ?? '')) fail();
    return url;
  }

  private async slot(jobId: string): Promise<string> {
    return await digest(canonical({ version: VERSION, workflowId: this.workflowId, callbackScope: await callbackScope(this.captured, this.workflowId), jobId }));
  }

  private async readMapping(slot: string): Promise<Mapping | undefined> {
    const saved = await this.maps.read(slot);
    if (!saved) return undefined;
    const value: unknown = JSON.parse(saved.entry.planJson);
    exact(value, 'handle,slot,operationId,media,jobId,version,workflowId,callbackScope,mapperNodeId,waitExecutionId,waitName,waitVersion,waitParametersDigest,instanceOrigin,resumeUrl');
    if (value.version !== VERSION || value.workflowId !== this.workflowId || typeof value.handle !== 'string' || typeof value.jobId !== 'string' ||
        value.callbackScope !== await callbackScope(this.captured, this.workflowId) || typeof value.waitExecutionId !== 'string' ||
        !/^[1-9][0-9]{0,18}$/.test(value.waitExecutionId) || typeof value.resumeUrl !== 'string' || typeof value.instanceOrigin !== 'string' ||
        typeof value.mapperNodeId !== 'string' || typeof value.waitName !== 'string' || typeof value.waitParametersDigest !== 'string' ||
        typeof value.waitVersion !== 'number' || await this.slot(value.jobId) !== slot) fail();
    this.target(value.resumeUrl, value.waitExecutionId, value.instanceOrigin);
    if (canonical(await this.original(value.handle)) !== canonical({ handle: value.handle, slot: value.slot, operationId: value.operationId, media: value.media, jobId: value.jobId })) fail();
    return value as unknown as Mapping;
  }

  private async handle(slot: string): Promise<string> {
    return PREFIX + Buffer.from(await this.maps.seal('handle', slot, canonical({ version: VERSION, slot }))).toString('base64url');
  }

  private originalWaitOutput(original: Original, mapping: Mapping, registered: boolean): IDataObject {
    return { object: 'cuddler.callback-wait', operationHandle: original.handle, operationId: original.operationId, jobId: original.jobId,
      registered, disposition: registered ? 'original-wait' : 'existing-original-wait', originalWaitExecutionId: mapping.waitExecutionId,
      recovery: registered ? 'continue-single-wait' : 'read-original-job-with-original-handle' };
  }

  async register(handle: string): Promise<IDataObject> {
    const original = await this.original(handle);
    const slot = await this.slot(original.jobId);
    const existing = await this.readMapping(slot);
    // A retry/new execution must consume the original job, never replace its
    // first durable waiter. The original receiver can still resume that waiter;
    // if it has ended, the unchanged original handle supports a free GET/file.
    if (existing && existing.waitExecutionId !== this.executionId) {
      await this.recheck();
      return this.originalWaitOutput(original, existing, false);
    }
    const wait = this.singleWait('before');
    const raw = this.context.evaluateExpression('{{ $execution.resumeUrl }}', 0);
    if (typeof raw !== 'string' || raw.length > 4096) fail();
    const url = this.target(raw, this.executionId);
    const mapping: Mapping = { ...original, version: VERSION, workflowId: this.workflowId,
      callbackScope: await callbackScope(this.captured, this.workflowId), mapperNodeId: this.nodeId,
      waitExecutionId: this.executionId, waitName: wait.name, waitVersion: wait.typeVersion,
      waitParametersDigest: await digest(canonical(wait.parameters)), instanceOrigin: url.origin, resumeUrl: url.href };
    await this.recheck();
    const saved = await this.maps.saveOnce(slot, canonical(mapping));
    if (saved.entry.planJson !== canonical(mapping)) {
      const winner = await this.readMapping(slot);
      if (!winner || winner.waitExecutionId === this.executionId) fail();
      await this.recheck();
      return this.originalWaitOutput(original, winner, false);
    }
    await this.recheck();
    return this.originalWaitOutput(original, mapping, true);
  }

  private async delivery(value: string): Promise<{ event: SafeEvent; slot: string; lease: Lease; protector: TaskCallbackProtector; inbox: TaskCallbackInbox }> {
    const binding = TaskCallbackProtector.peekBinding(value, 'delivery');
    if (binding.workflowId !== this.workflowId || binding.tableId !== this.captured.tableId || binding.rowId !== this.captured.rowId) fail();
    const protector = await TaskCallbackProtector.create(this.workflowId, binding.triggerNodeId, this.captured, binding.callbackDigest);
    const opened = await protector.open('delivery', value);
    exact(opened, 'event,slot,lease'); exact(opened.lease, 'token,executionId,until');
    // The Inbox validates the complete authenticated lease/state at completion.
    if (!record(opened.event) || typeof opened.slot !== 'string' || typeof opened.lease.executionId !== 'string' ||
        !Number.isSafeInteger(opened.lease.until) || Number(opened.lease.until) <= Date.now()) fail();
    const event = safeEvent(opened.event);
    if (event.callbackScope !== await callbackScope(this.captured, this.workflowId) || event.subject.object === 'balance') fail();
    const inbox = new TaskCallbackInbox(this.bridge.capability(this.captured.tableId), protector, () => this.recheck());
    return { event, slot: opened.slot, lease: opened.lease as unknown as Lease, protector, inbox };
  }

  async deliver(value: string): Promise<IDataObject> {
    const current = await this.delivery(value);
    if (current.lease.executionId !== this.executionId) fail();
    if (await current.inbox.completed(current.event)) return { eventId: current.event.eventId, statusCode: 200, disposition: 'completed' };
    const subject = current.event.subject;
    if (!('id' in subject)) fail();
    const slot = await this.slot(subject.id);
    const mapping = await this.readMapping(slot);
    if (!mapping) return { eventId: current.event.eventId, statusCode: 503, disposition: 'waiting-for-original-job' };
    if (mapping.media !== subject.object || mapping.jobId !== subject.id) fail();
    const proof = { callbackWaitHandle: await this.handle(slot), deliveryHandle: value };
    const target = this.target(mapping.resumeUrl, mapping.waitExecutionId, mapping.instanceOrigin);
    await this.recheck();
    if (canonical(await this.readMapping(slot)) !== canonical(mapping)) fail();
    try {
      // Public helper only, same trusted instance + captured original waiting path/token.
      // No authentication helper, Authorization, Cookie, redirect, workflow URL or TLS override.
      await this.context.helpers.httpRequest({ method: 'POST', url: target.href, body: proof, json: true,
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' }, timeout: 10000,
        disableFollowRedirect: true, returnFullResponse: true, ignoreHttpStatusErrors: true });
    } catch { /* An ambiguous/lost response is pending, never proof of consumption. */ }
    await this.recheck();
    const completed = await current.inbox.completed(current.event);
    return { eventId: current.event.eventId, statusCode: completed ? 200 : 503, disposition: completed ? 'completed' : 'pending' };
  }

  /**
   * Explicit crash recovery: consume the authenticated original terminal job by
   * GET, without pretending to resume the old execution or replace its Wait.
   * The marker is shared across receiver leases; ownership is checked live at
   * every boundary and Inbox completion, never inferred from that marker alone.
   */
  async recover(value: string): Promise<IDataObject> {
    const current = await this.delivery(value);
    const assertLease = async () => await current.inbox.assertCurrentLease(current.event, current.slot, current.lease, this.executionId);
    if (await assertLease()) return { eventId: current.event.eventId, statusCode: 200, disposition: 'completed' };
    const subject = current.event.subject;
    if (!('id' in subject)) fail();
    const mapSlot = await this.slot(subject.id);
    const mapping = await this.readMapping(mapSlot);
    const pending = (): IDataObject => ({ eventId: current.event.eventId, statusCode: 503, disposition: 'original-job-recovery-pending' });
    if (!mapping) return pending();
    if (mapping.media !== subject.object || mapping.jobId !== subject.id) fail();
    const marker = canonical({ version: RECOVERY_VERSION, workflowId: this.workflowId, callbackScope: mapping.callbackScope,
      eventIdentity: identity(current.event), mapSlot, mappingDigest: await digest(canonical(mapping)),
      operationId: mapping.operationId, jobId: mapping.jobId, media: mapping.media, status: subject.status });
    const markerSlot = await digest(canonical({ version: RECOVERY_VERSION, workflowId: this.workflowId, mapSlot, eventId: current.event.eventId }));
    const prior = await this.maps.read(markerSlot);
    if (prior && prior.entry.planJson !== marker) fail();
    if (!prior) {
      await assertLease();
      let observed: Pick<INodeExecutionData, 'json' | 'binary'>;
      try { observed = await this.readOriginal(mapping.handle, mapping.media); }
      catch { await this.recheck(); await assertLease(); return pending(); }
      await this.recheck();
      await assertLease();
      // This is the real protected read capability's safe projection. Plain
      // node parameters/JSON, HTTP409 and a successful resume POST are not read
      // authority. No output URL or provider error is persisted or returned.
      const job = observed.json;
      if (job.jobId !== mapping.jobId || job.operationId !== mapping.operationId || job.operationHandle !== mapping.handle ||
          job.state !== 'original-job' || job.terminal !== true || job.status !== subject.status ||
          !['succeeded', 'failed', 'canceled'].includes(String(job.status))) return pending();
      if (canonical(await this.readMapping(mapSlot)) !== canonical(mapping)) fail();
      await assertLease();
      const saved = await this.maps.saveOnce(markerSlot, marker);
      if (saved.entry.planJson !== marker) fail();
    }
    await this.recheck();
    await assertLease();
    if (canonical(await this.readMapping(mapSlot)) !== canonical(mapping)) fail();
    const saved = await this.maps.read(markerSlot);
    if (!saved || saved.entry.planJson !== marker) fail();
    await assertLease();
    await current.inbox.complete(current.event, current.slot, current.lease, this.executionId);
    if (!await current.inbox.completed(current.event)) fail();
    await this.recheck();
    return { eventId: current.event.eventId, operationId: mapping.operationId, jobId: mapping.jobId, status: subject.status,
      statusCode: 200, disposition: 'recovered-original-job' };
  }

  async complete(proof: unknown): Promise<IDataObject> {
    exact(proof, 'callbackWaitHandle,deliveryHandle');
    if (typeof proof.callbackWaitHandle !== 'string' || !proof.callbackWaitHandle.startsWith(PREFIX) || proof.callbackWaitHandle.length > 16384 || !/^[A-Za-z0-9_-]+$/.test(proof.callbackWaitHandle.slice(PREFIX.length)) || typeof proof.deliveryHandle !== 'string') fail();
    const opened = await this.maps.unseal('handle', Buffer.from(proof.callbackWaitHandle.slice(PREFIX.length), 'base64url').toString('utf8'));
    const pointer: unknown = JSON.parse(opened.plaintext); exact(pointer, 'version,slot');
    if (pointer.version !== VERSION || pointer.slot !== opened.slot) fail();
    const mapping = await this.readMapping(opened.slot);
    const wait = this.singleWait('after');
    if (!mapping || mapping.waitExecutionId !== this.executionId || wait.name !== mapping.waitName || wait.typeVersion !== mapping.waitVersion ||
        await digest(canonical(wait.parameters)) !== mapping.waitParametersDigest) fail();
    const current = await this.delivery(proof.deliveryHandle);
    if (!('id' in current.event.subject) || current.event.subject.id !== mapping.jobId || current.event.subject.object !== mapping.media ||
        await this.slot(current.event.subject.id) !== opened.slot) fail();
    await this.recheck();
    await current.inbox.complete(current.event, current.slot, current.lease, current.lease.executionId);
    if (!await current.inbox.completed(current.event)) fail();
    return { eventId: current.event.eventId, jobId: mapping.jobId, operationId: mapping.operationId, statusCode: 200, disposition: 'completed' };
  }
}
