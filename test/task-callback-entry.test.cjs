const test = require('node:test');
const assert = require('node:assert/strict');
const { createHmac, createHash } = require('node:crypto');
const { CuddlerTaskCallbackTrigger } = require('../dist/nodes/CuddlerTaskCallback/CuddlerTaskCallbackTrigger.node');
const { CuddlerTaskCallbackReceipt } = require('../dist/nodes/CuddlerTaskCallback/CuddlerTaskCallbackReceipt.node');
const { TaskCallbackProtector } = require('../dist/nodes/CuddlerTaskCallback/TaskCallbackProtector');
const { credentials, TYPES, RECEIPT_TTL, LEASE_MS, identity, digest } = require('../dist/nodes/CuddlerTaskCallback/TaskCallbackContract');
const { INBOX_FORMAT } = require('../dist/nodes/CuddlerTaskCallback/TaskCallbackInbox');
const { Boundary, event, sign, SECRET, CALLBACK_SCOPE, clone, sha } = require('./callback-entry-harness.cjs');
async function rejected(promise) {
  await assert.rejects(promise, (error) => {
    assert.equal(error.message, 'CUDDLER_TASK_CALLBACK_REJECTED');
    for (const marker of ['PRIVATE_PROMPT', 'SECRET_SIGNED_URL', SECRET, 'synthetic-existing-org-key', 'raw fixture error']) assert.ok(!JSON.stringify(error).includes(marker));
    return true;
  });
}

for (const type of TYPES) test(`actual trigger/claim/complete and completed duplicate: ${type}`, async () => {
  const b = new Boundary(); const original = event(type); const ingress = await b.trigger(original);
  const [business, ack] = await b.action('claimEvent', ingress.eventReceipt);
  assert.equal(business.length, 1); assert.equal(ack.length, 0); assert.deepEqual(business[0].pairedItem, { item: 0 });
  assert.equal(business[0].json.event.type, type); assert.equal(business[0].json.eventId, original.id);
  const complete = await b.action('completeEvent', business[0].json.deliveryHandle);
  assert.equal(complete[0].length, 0); assert.equal(complete[1][0].json.statusCode, 200);
  const duplicate = await b.action('claimEvent', (await b.trigger(original)).eventReceipt, { executionId: 'execution_2' });
  assert.equal(duplicate[0].length, 0); assert.equal(duplicate[1][0].json.disposition, 'duplicate'); assert.equal(duplicate[1][0].json.statusCode, 200);
  const visible = JSON.stringify({ ingress, business, ack, complete, children: b.calls, row: b.row });
  for (const marker of ['PRIVATE_PROMPT', 'SECRET_SIGNED_URL', 'PRIVATE_METADATA', 'PRIVATE_ERROR', SECRET, b.credential.apiKey]) assert.ok(!visible.includes(marker));
  assert.ok(b.calls.some((p) => p.operation === 'update')); assert.ok(b.calls.at(-1).operation === 'get');
});

test('raw whitespace is authenticated without JSON reserialization', async () => {
  const b = new Boundary(); const e = event(); const body = Buffer.from(`\n ${JSON.stringify(e, null, 2)} \n`);
  assert.equal((await b.trigger(e, { body })).event.eventId, e.id);
});
test('complete whsec_ string is the key, not decoded suffix', async () => {
  const b = new Boundary(); const e = event();
  await rejected(b.trigger(e, { secret: Buffer.from(SECRET.slice(6), 'base64url') }));
});
test('multiple v1 candidates can match while duplicate timestamp is rejected', async () => {
  const b = new Boundary(); const e = event(); const body = Buffer.from(JSON.stringify(e)); const signature = sign(body);
  await b.trigger(e, { body, headers: { 'cuddler-signature': `${signature},v1=${'0'.repeat(64)}` } });
  await rejected(b.trigger(e, { body, headers: { 'cuddler-signature': `${signature},t=123` } }));
});
for (const delta of [-301, 301]) test(`timestamp outside tolerance ${delta}`, async () => {
  await rejected(new Boundary().trigger(event(), { timestamp: Math.floor(Date.now() / 1000) + delta }));
});
for (const [label, options] of [
  ['missing raw bytes', { noRaw: true }], ['wrong method', { method: 'GET' }], ['wrong content type', { headers: { 'content-type': 'text/plain' } }],
  ['compressed content', { headers: { 'content-encoding': 'gzip' } }], ['array signature', { headers: { 'cuddler-signature': ['a', 'b'] } }],
  ['mismatched event header', { headers: { 'cuddler-event-id': 'evt_123456789012' } }], ['malformed signature', { headers: { 'cuddler-signature': 't=1,v1=xyz' } }],
]) test(`ingress rejects ${label}`, async () => { await rejected(new Boundary().trigger(event(), options)); });
test('invalid HMAC, changed bytes, invalid UTF8 and overlimit fail before workflow output', async () => {
  const b = new Boundary(); const e = event(); const body = Buffer.from(JSON.stringify(e));
  await rejected(b.trigger(e, { headers: { 'cuddler-signature': sign(Buffer.concat([body, Buffer.from('x')])) } }));
  await rejected(b.trigger(e, { body: Buffer.from([0xff]) }));
  await rejected(b.trigger(e, { body: Buffer.alloc(1024 * 1024 + 1) }));
});
for (const change of [
  (e) => { e.type = 'image.canceled'; e.data.object = 'image'; },
  (e) => { e.data.status = 'running'; }, (e) => { e.data.object = 'music'; }, (e) => { e.data.id = 'wrong-id'; },
  (e) => { e.id = 'evt_short'; }, (e) => { e.created_at = 'yesterday'; }, (e) => { e.extra = 'not-envelope'; },
]) test(`strict authenticated event schema rejects ${String(change)}`, async () => {
  const e = event(); change(e); await rejected(new Boundary().trigger(e));
});
test('ingress raw snapshot survives a caller mutation during credential await', async () => {
  const b = new Boundary(); const e = event(); const body = Buffer.from(JSON.stringify(e));
  b.credentialHook = async () => { body.fill(0); };
  assert.equal((await b.trigger(e, { body })).event.eventId, e.id);
});
test('credential mutation during ingress is rejected', async () => {
  const b = new Boundary(); b.credentialHook = async () => { if (b.credentialsRead === 2) b.credential.apiKey = 'rotated-key'; };
  await rejected(b.trigger());
});
test('same event re-signed with refreshed URL deduplicates; conflicting job identity rejects', async () => {
  const b = new Boundary(); const e = event(); const first = await b.action('claimEvent', (await b.trigger(e)).eventReceipt);
  await b.action('completeEvent', first[0][0].json.deliveryHandle);
  e.data.output.url = 'https://refreshed.example/second-signed-value';
  assert.equal((await b.action('claimEvent', (await b.trigger(e)).eventReceipt))[1][0].json.disposition, 'duplicate');
  e.data.id = 'vid_bbbbbbbbbbbb';
  await rejected(b.action('claimEvent', (await b.trigger(e)).eventReceipt));
});
test('same event conflicting type rejects', async () => {
  const b = new Boundary(); const e = event(); await b.action('claimEvent', (await b.trigger(e)).eventReceipt);
  e.type = 'video.failed'; e.data.status = 'failed'; await rejected(b.action('claimEvent', (await b.trigger(e)).eventReceipt));
});
test('active lease prevents another business output and returns 503', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt;
  await b.action('claimEvent', receipt);
  const second = await b.action('claimEvent', receipt, { executionId: 'execution_2' });
  assert.equal(second[0].length, 0); assert.equal(second[1][0].json.statusCode, 503); assert.equal(second[1][0].json.disposition, 'busy');
});
test('concurrent claims have one CAS winner despite preselected update output', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt;
  const results = await Promise.all(Array.from({ length: 5 }, (_, i) => b.action('claimEvent', receipt, { executionId: `execution_${i}` })));
  assert.equal(results.reduce((sum, result) => sum + result[0].length, 0), 1);
  assert.equal(results.reduce((sum, result) => sum + result[1].length, 0), 4);
  assert.ok(results.filter((r) => r[1].length).every((r) => r[1][0].json.statusCode === 503));
});
test('crash before completion reclaims expired lease; stale or cross-execution completion fails', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt;
  const first = (await b.action('claimEvent', receipt))[0][0].json.deliveryHandle;
  await rejected(b.action('completeEvent', first, { executionId: 'different_execution' }));
  const realNow = Date.now; const later = realNow() + LEASE_MS + 500;
  try {
    Date.now = () => later; // Offline host-clock fixture, never a node parameter or supplied payload field.
    const resumed = await b.action('claimEvent', receipt, { executionId: 'execution_2' });
    assert.equal(resumed[0].length, 1); await rejected(b.action('completeEvent', first));
    await b.action('completeEvent', resumed[0][0].json.deliveryHandle, { executionId: 'execution_2' });
  } finally { Date.now = realNow; }
});
test('lost child response after persistent claim does not acknowledge or redeliver before lease expiry', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt; const child = b.child.bind(b); let lost = false;
  b.child = async (graph) => { const result = await child(graph); if (!lost && graph.code.nodes[1].parameters.operation === 'update') { lost = true; throw new Error('raw fixture error'); } return result; };
  await rejected(b.action('claimEvent', receipt));
  const retried = await b.action('claimEvent', receipt, { executionId: 'execution_2' });
  assert.equal(retried[0].length, 0); assert.equal(retried[1][0].json.statusCode, 503);
});
test('lost completion response is recoverable as completed duplicate without business replay', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt;
  const handle = (await b.action('claimEvent', receipt))[0][0].json.deliveryHandle;
  const child = b.child.bind(b); let lost = false;
  b.child = async (graph) => { const result = await child(graph); if (!lost && graph.code.nodes[1].parameters.operation === 'update') { lost = true; throw new Error('raw fixture error'); } return result; };
  await rejected(b.action('completeEvent', handle));
  const duplicate = await b.action('claimEvent', receipt, { executionId: 'execution_2' });
  assert.equal(duplicate[0].length, 0); assert.equal(duplicate[1][0].json.statusCode, 200);
});
test('only authenticated receipt can claim; only authenticated lease can complete', async () => {
  const b = new Boundary(); const ingress = await b.trigger();
  await rejected(b.action('claimEvent', JSON.stringify({ eventId: ingress.event.eventId, authenticated: true })));
  const box = JSON.parse(ingress.eventReceipt); box.ciphertext = box.ciphertext.slice(0, -3) + 'aaa';
  await rejected(b.action('claimEvent', JSON.stringify(box)));
  const claim = await b.action('claimEvent', ingress.eventReceipt);
  await rejected(b.action('completeEvent', ingress.eventReceipt));
  const leaseBox = JSON.parse(claim[0][0].json.deliveryHandle); leaseBox.binding.triggerNodeId = 'different_trigger';
  await rejected(b.action('completeEvent', JSON.stringify(leaseBox)));
});
for (const [field, value] of [['apiKey', 'new-key'], ['signingSecret', `whsec_${'b'.repeat(43)}`], ['ledgerTableId', 'other_table'], ['ledgerRowId', '2']]) test(`protected ${field} changes fail closed`, async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt; b.credential[field] = value;
  await rejected(b.action('claimEvent', receipt)); assert.equal(b.calls.length, 0);
});
test('workflow change refuses receipt before native child access', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt; b.workflowId = 'other_workflow';
  await rejected(b.action('claimEvent', receipt)); assert.equal(b.calls.length, 0);
});
test('mid-child credential rotation refuses business output and ACK', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt;
  b.childHook = async (p) => { if (p.operation === 'update') b.credential.apiKey = 'midawait-rotation'; };
  await rejected(b.action('claimEvent', receipt));
});
test('reordering input items keeps authenticated event and pairedItem associations', async () => {
  const b = new Boundary(); const one = await b.trigger(); const two = await b.trigger();
  const result = await b.action('claimEvent', [two.eventReceipt, one.eventReceipt]);
  assert.deepEqual(result[0].map((item) => item.json.eventId), [two.event.eventId, one.event.eventId]);
  assert.deepEqual(result[0].map((item) => item.pairedItem.item), [0, 1]);
});
test('tampered persisted ciphertext refuses subsequent claims', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt; await b.action('claimEvent', receipt);
  const ledger = JSON.parse(b.row.payload); const slot = Object.keys(ledger.slots)[0]; const box = JSON.parse(ledger.slots[slot]); box.ciphertext = 'a'.repeat(box.ciphertext.length);
  ledger.slots[slot] = JSON.stringify(box); b.row.payload = JSON.stringify(ledger); await rejected(b.action('claimEvent', receipt));
});
test('native CAS no-op is never treated as saved; bounded contention rejects', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt; const child = b.child.bind(b);
  b.child = async (graph) => { if (graph.code.nodes[1].parameters.operation === 'update') { b.calls.push(clone(graph.code.nodes[1].parameters)); return { executionId: 'no_op', data: [[{ json: clone(b.row) }]] }; } return await child(graph); };
  await rejected(b.action('claimEvent', receipt)); assert.equal(b.calls.filter((p) => p.operation === 'update').length, 5);
});
test('readback mismatch and missing native row never produce business output', async () => {
  for (const missing of [true, false]) {
    const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt; const child = b.child.bind(b);
    b.child = async (graph) => { const result = await child(graph); if (missing) return { ...result, data: [[]] }; if (graph.code.nodes[1].parameters.operation === 'update') b.row.payload = JSON.stringify({ format: INBOX_FORMAT, slots: {} }); return result; };
    await rejected(b.action('claimEvent', receipt));
  }
});
test('receipt expiration and future verification clock reject without native reads', async () => {
  const b = new Boundary(); const ingress = await b.trigger(); const protector = await b.protector(); const value = await protector.open('ingress', ingress.eventReceipt);
  for (const verifiedAt of [Date.now() - RECEIPT_TTL - 1, Date.now() + 300000]) {
    await rejected(b.action('claimEvent', await protector.seal('ingress', { ...value, verifiedAt, expiresAt: verifiedAt + RECEIPT_TTL })));
  }
  assert.equal(b.calls.length, 0);
});
test('completed event slot substitution and lease token substitution are authenticated', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt; const result = await b.action('claimEvent', receipt); const p = await b.protector(); const handle = await p.open('delivery', result[0][0].json.deliveryHandle);
  await rejected(b.action('completeEvent', await p.seal('delivery', { ...handle, slot: '0'.repeat(64) })));
  await rejected(b.action('completeEvent', await p.seal('delivery', { ...handle, lease: { ...handle.lease, token: '0'.repeat(64) } })));
});
test('capacity256 and row byte cap fail closed with no automatic deletion', async () => {
  const b = new Boundary(); const receipt = (await b.trigger()).eventReceipt; const before = {};
  for (let i = 0; i < 256; i++) before[i.toString(16).padStart(64, '0')] = 'nonempty-opaque-placeholder';
  b.row.payload = JSON.stringify({ format: INBOX_FORMAT, slots: before }); await rejected(b.action('claimEvent', receipt));
  assert.equal(b.calls.filter((p) => p.operation === 'update').length, 0); assert.deepEqual(JSON.parse(b.row.payload).slots, before);
  b.row.payload = ' '.repeat(1024 * 1024 + 1); await rejected(b.action('claimEvent', receipt));
});
test('retryOnFail and continueOnFail are rejected before credentials', async () => {
  for (const node of [{ retryOnFail: true }, { continueOnFail: true }, { onError: 'continueRegularOutput' }]) {
    const b = new Boundary(); await rejected(b.action('claimEvent', 'not-even-a-receipt', { node })); assert.equal(b.credentialsRead, 0);
  }
});
test('fixed responseNode503 fallback and explicit ACK channel, no external lifecycle or network helper', () => {
  const trigger = new CuddlerTaskCallbackTrigger(); const action = new CuddlerTaskCallbackReceipt();
  assert.equal(trigger.description.webhooks[0].responseMode, 'responseNode'); assert.equal(trigger.description.webhooks[0].responseCode, '={{503}}');
  assert.deepEqual(Object.keys(trigger.webhookMethods.default).sort(),['checkExists','create','delete']); assert.equal(trigger.description.usableAsTool, undefined);
  assert.deepEqual(action.description.outputNames, ['Business Action', 'Acknowledgement']);
});
test('local credential test makes its non-runtime scope explicit', async () => {
  const trigger = new CuddlerTaskCallbackTrigger(); const b = new Boundary(); const check = trigger.methods.credentialTest.cuddlerTaskCallbackCredentialTest;
  const ok = await check({ data: b.credential }); assert.equal(ok.status, 'OK'); assert.match(ok.message, /Local format only/);
  assert.equal((await check({ data: { ...b.credential, signingSecret: 'wrong' } })).status, 'Error');
});

for (const options of [{mode:'manual'}, {settings:{engineType:'v2'}}, {served:'https://receiver.fixture.example/webhook-test/fixture'}]) test('unsupported test/engine refuses before credentials '+JSON.stringify(options), async()=>{const b=new Boundary(); await rejected(b.trigger(event(),options)); assert.equal(b.credentialsRead,0);});
test('production rejection returns fixed401 and no execution seed rather than throw',async()=>{const b=new Boundary(); const result=await b.trigger(event(),{secret:'wrong',fullResponse:true});assert.deepEqual(result,{noWebhookResponse:true});assert.deepEqual(b.rejection,{code:401,body:{error:'CUDDLER_TASK_CALLBACK_REJECTED'}});});
test('callback credential change refuses old receipt before native access',async()=>{const b=new Boundary();const ingress=await b.trigger();b.credential.callbackUrl='https://other.fixture.example/webhook/fixture';await rejected(b.action('claimEvent',ingress.eventReceipt));assert.equal(b.calls.length,0);});
test('tool alias refuses before credentials or native child',async()=>{const b=new Boundary();await rejected(b.action('claimEvent','not-receipt',{node:{type:'n8n-nodes-cuddler.cuddlerTaskCallbackReceiptTool'}}));assert.equal(b.credentialsRead,0);});

test('manual per-task lifecycle only validates local callback and never registers or deletes remote state',async()=>{const b=new Boundary();const hooks=new CuddlerTaskCallbackTrigger().webhookMethods.default;const ctx={getCredentials:()=>b.getCredentials(),getNodeWebhookUrl:()=>b.credential.callbackUrl,helpers:new Proxy({},{get:()=>{throw new Error('HTTP forbidden');}})};assert.equal(await hooks.checkExists.call(ctx),true);assert.equal(await hooks.delete.call(ctx),true);await assert.rejects(hooks.create.call(ctx),/CUDDLER_TASK_CALLBACK_REJECTED/);ctx.getNodeWebhookUrl=()=> 'https://wrong.example/events';assert.equal(await hooks.checkExists.call(ctx),false);});
