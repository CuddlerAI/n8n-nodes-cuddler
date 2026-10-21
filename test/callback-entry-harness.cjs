const assert = require('node:assert/strict');
const { createHmac, createHash } = require('node:crypto');
const { CuddlerTaskCallbackTrigger } = require('../dist/nodes/CuddlerTaskCallback/CuddlerTaskCallbackTrigger.node');
const { CuddlerTaskCallbackReceipt } = require('../dist/nodes/CuddlerTaskCallback/CuddlerTaskCallbackReceipt.node');
const { TaskCallbackProtector } = require('../dist/nodes/CuddlerTaskCallback/TaskCallbackProtector');
const { credentials, TYPES, RECEIPT_TTL, LEASE_MS, identity, digest } = require('../dist/nodes/CuddlerTaskCallback/TaskCallbackContract');
const { INBOX_FORMAT } = require('../dist/nodes/CuddlerTaskCallback/TaskCallbackInbox');
const clone = (value) => JSON.parse(JSON.stringify(value));
const SECRET = `whsec_${'a'.repeat(43)}`;
const canonical = (value) => value && typeof value === 'object' ? (Array.isArray(value) ? '[' + value.map(canonical).join(',') + ']' : '{' + Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+canonical(value[k])).join(',') + '}') : JSON.stringify(value);
const sha = value => createHash('sha256').update(value).digest('hex');
const CALLBACK_SCOPE = sha(canonical({version:'cuddler.n8n.per-task-wait/1',workflowId:'workflow_1',callbackUrl:'https://receiver.fixture.example/webhook/fixture',inboxTableId:'events_1',inboxRowId:1,waitTableId:'events_1',waitRowId:2,keyFingerprint:sha('cuddler.n8n.per-task-callback/2/key\nsynthetic-existing-org-key'),secretFingerprint:sha('cuddler.n8n.per-task-callback/2/secret\n'+SECRET)}));
let eventSequence = 0;
function event(type = 'video.succeeded') {
  const [kind, status] = type.split('.');
  const prefix = { video: 'vid', image: 'img', speech: 'aud', music: 'mus' }[kind];
  return { object: 'event', id: `evt_${String(++eventSequence).padStart(20, '0')}`, type, created_at: new Date().toISOString(),
    data: kind === 'balance' ? { object: 'balance', currency: 'usd', total_usd: '1.000000', held_usd: '0.000000', available_usd: '1.000000', daily_limit_usd: '0.000000', spent_today_usd: '0.000000', expires_at: null } :
      { object: kind, id: `${prefix}_${'a'.repeat(kind === 'music' ? 20 : 12)}`, status, prompt: 'PRIVATE_PROMPT_MARKER', output: { url: 'https://private.example/SECRET_SIGNED_URL' }, metadata: { cuddler_callback_scope: CALLBACK_SCOPE, private: 'PRIVATE_METADATA_MARKER' }, error: { message: 'PRIVATE_ERROR_MARKER' } } };
}
function sign(body, secret = SECRET, timestamp = Math.floor(Date.now() / 1000)) {
  return `t=${timestamp},v1=${createHmac('sha256', secret).update(`${timestamp}.`).update(body).digest('hex')}`;
}
class Boundary {
  constructor() {
    this.credential = { apiKey: 'synthetic-existing-org-key', signingSecret: SECRET, ledgerTableId: 'events_1', ledgerRowId: '1', waitTableId:'events_1',waitRowId:'2', callbackUrl: 'https://receiver.fixture.example/webhook/fixture' };
    this.row = { id: 1, etag: 'a'.repeat(64), payload: JSON.stringify({ format: INBOX_FORMAT, slots: {} }) };
    this.workflowId = 'workflow_1'; this.triggerId = 'trigger_1'; this.executionId = 'execution_1';
    this.calls = []; this.credentialsRead = 0; this.credentialHook = undefined; this.childHook = undefined;
  }
  async getCredentials() {
    this.credentialsRead++;
    if (this.credentialHook) await this.credentialHook(this);
    return clone(this.credential);
  }
  async trigger(value = event(), options = {}) {
    const body = options.body ?? Buffer.from(JSON.stringify(value));
    const request = { method: options.method ?? 'POST', rawBody: body, headers: {
      'content-type': 'application/json', 'cuddler-signature': sign(body, options.secret ?? SECRET, options.timestamp), 'cuddler-event-id': value.id, ...options.headers,
    } };
    if (options.noRaw) delete request.rawBody;
    const context = { getRequestObject: () => request, getHeaderData: () => request.headers, getMode: () => options.mode ?? 'webhook', getWorkflowSettings: () => options.settings ?? {}, getWebhookResourceUrl: () => options.served ?? this.credential.callbackUrl, getResponseObject: () => ({status: (code) => ({json: (body) => { this.rejection = {code, body}; }} )}), getCredentials: () => this.getCredentials(),
      getWorkflow: () => ({ id: this.workflowId }), getNodeWebhookUrl: () => 'https://receiver.fixture.example/webhook/fixture', getNode: () => ({ id: this.triggerId, name: 'Fixture Trigger', type: 'cuddlerEventTrigger', typeVersion: 1, parameters: { options: { binaryData: true } } }) };
    const result = await CuddlerTaskCallbackTrigger.prototype.webhook.call(context);
    if (options.fullResponse) return result;
    if (!result.workflowData) { assert.equal(this.rejection.code, 401); assert.deepEqual(result, {noWebhookResponse:true}); throw new Error('CUDDLER_TASK_CALLBACK_REJECTED'); }
    return result.workflowData[0][0].json;
  }
  async child(graph) {
    const p = graph.code.nodes[1].parameters;
    this.calls.push(clone(p));
    if (this.childHook) await this.childHook(p, this);
    const conditions = p.filters.conditions;
    assert.equal(conditions[0].keyName, 'id'); assert.equal(conditions[0].keyValue, 1);
    const before = clone(this.row);
    if (p.operation === 'update') {
      assert.equal(conditions.length, 2); assert.equal(conditions[1].keyName, 'etag');
      if (conditions[1].keyValue === this.row.etag) this.row = { ...this.row, ...clone(p.columns.value) };
    }
    // Deliberately emulate native update's preselected rows, which are not a CAS success receipt.
    return { executionId: `child_${this.calls.length}`, data: [[{ json: p.operation === 'get' ? clone(this.row) : before }]] };
  }
  async action(operation, value, options = {}) {
    const values = Array.isArray(value) ? value : [value];
    const node = { id: 'receipt_1', name: 'Fixture Receipt', type: 'n8n-nodes-cuddler.cuddlerTaskCallbackReceipt', parameters: {}, ...options.node };
    const executionId = options.executionId ?? this.executionId;
    const context = {
      getInputData: () => values.map(() => ({ json: {} })), getNodeParameter: (name, index) => name === 'operation' ? operation : values[index],
      getCredentials: () => this.getCredentials(), getWorkflow: () => ({ id: this.workflowId }), getNode: () => node,
      getExecutionId: () => executionId, getMode: () => 'manual', continueOnFail: () => false,
      executeWorkflow: (graph) => this.child(graph),
    };
    return await CuddlerTaskCallbackReceipt.prototype.execute.call(context);
  }
  async protector(triggerId = this.triggerId) { return await TaskCallbackProtector.create(this.workflowId, triggerId, credentials(this.credential), await digest('https://receiver.fixture.example/webhook/fixture')); }
}

module.exports = { Boundary, event, sign, SECRET, CALLBACK_SCOPE, clone, sha };
