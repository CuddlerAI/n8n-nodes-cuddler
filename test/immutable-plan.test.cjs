'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { prepare, verify, canonical, generationUnavailable } = require('../dist/nodes/Cuddler/recovery/ImmutablePlan.js');
// These are synthetic fixture strings, never real organization credentials.
const key = 'synthetic-credential-fixture-a';
const scope = { workflowId: 'workflow_fixture', nodeId: 'node_fixture', businessBatchId: 'batch_fixture', stableItemId: 'item_fixture' };
const body = { model: 'synthetic-model', prompt: 'synthetic-prompt', max_usd: '0.100000', references: ['synthetic-a', 'synthetic-b'] };
const clone = (value) => JSON.parse(JSON.stringify(value));

for (const media of ['video', 'image', 'speech', 'music']) {
  test(`${media} prepares and authenticates the complete detached request`, async () => {
    const input = clone(body);
    const result = await prepare(media, input, scope, key, 'cli', 'new');
    const verified = await verify(result, media, body, scope, key);
    assert.equal(verified.payload.method, 'POST');
    assert.equal(verified.payload.origin, 'https://api.cuddler.ai');
    assert.equal(verified.payload.path, { video: '/v1/videos', image: '/v1/images', speech: '/v1/speech', music: '/v1/music' }[media]);
    assert.equal(verified.payload.budget.decimal, '0.100000');
    assert.equal(verified.payload.requestJson, canonical(body));
    input.references.reverse();
    input.max_usd = '1';
    assert.equal(result.payload.requestJson, canonical(body));
    assert.notStrictEqual(verified.payload, result.payload);
    assert.ok(!JSON.stringify(result).includes(key));
  });
}

test('same stable slot chooses storage, independent prepares retain unique operation keys', async () => {
  const a = await prepare('image', body, scope, key, 'cli', 'new');
  const b = await prepare('image', body, scope, key, 'cli', 'new');
  assert.equal(a.payload.slot, b.payload.slot);
  assert.notEqual(a.payload.operationId, b.payload.operationId);
  assert.notEqual(a.payload.idempotencyKey, b.payload.idempotencyKey);
  const reordered = { references: [...body.references], max_usd: body.max_usd, prompt: body.prompt, model: body.model };
  assert.equal((await verify(a, 'image', reordered, scope, key)).payload.slot, a.payload.slot);
});

test('distinct stable items and deliberate batches are never collapsed', async () => {
  const original = await prepare('image', body, scope, key, 'cli', 'new');
  for (const changed of [{ stableItemId: 'another_item' }, { businessBatchId: 'another_batch' }, { nodeId: 'another_node' }, { workflowId: 'another_workflow' }]) {
    const other = await prepare('image', body, { ...scope, ...changed }, key, 'cli', 'new');
    assert.notEqual(other.payload.slot, original.payload.slot);
    await assert.rejects(verify(original, 'image', body, { ...scope, ...changed }, key), /binding mismatch/);
  }
});

test('request mutation cannot redirect an existing slot to a fresh operation', async () => {
  const original = await prepare('video', body, scope, key, 'cli', 'new');
  const changedBody = { ...body, model: 'another-model' };
  const other = await prepare('video', changedBody, scope, key, 'cli', 'new');
  assert.equal(other.payload.slot, original.payload.slot);
  await assert.rejects(verify(original, 'video', changedBody, scope, key), /binding mismatch/);
});

test('unknown versions, origin, endpoints and every authenticated field reject tampering', async () => {
  const envelope = await prepare('video', body, scope, key, 'cli', 'new');
  const changes = [
    ['contract', 'unknown/2'], ['envelopeVersion', 'unknown/2'], ['origin', 'https://attacker.invalid'],
    ['method', 'GET'], ['path', '/v1/images'], ['media', 'image'], ['slot', '0'.repeat(64)],
    ['operationId', '0'.repeat(36)], ['idempotencyKey', 'another'], ['credentialBinding', '0'.repeat(64)],
    ['requestJson', '{}'], ['budget', { present: false }], ['scope', { ...scope, nodeId: 'another' }],
  ];
  for (const [field, value] of changes) {
    const tampered = clone(envelope);
    tampered.payload[field] = value;
    await assert.rejects(verify(tampered, 'video', body, scope, key), /authenticated plan/);
  }
  await assert.rejects(verify({ ...envelope, extra: true }, 'video', body, scope, key));
  const badTag = clone(envelope);
  badTag.tag = '0'.repeat(64);
  await assert.rejects(verify(badTag, 'video', body, scope, key));
});

test('same organization new key and credential switch reject automatic recovery', async () => {
  const envelope = await prepare('music', body, scope, key, 'cli', 'new');
  const second = await prepare('music', body, scope, 'synthetic-credential-fixture-b', 'cli', 'new');
  assert.notEqual(envelope.payload.credentialBinding, second.payload.credentialBinding);
  assert.equal(envelope.payload.slot, second.payload.slot);
  await assert.rejects(verify(envelope, 'music', body, scope, 'synthetic-credential-fixture-b'));
});

test('changing media cannot escape the original business slot', async () => {
  const image = await prepare('image', body, scope, key, 'cli', 'new');
  const video = await prepare('video', body, scope, key, 'cli', 'new');
  assert.equal(image.payload.slot, video.payload.slot);
  await assert.rejects(verify(image, 'video', body, scope, key), /binding mismatch/);
});

test('exact budget absence and decimal spelling remain frozen', async () => {
  const envelope = await prepare('speech', body, scope, key, 'cli', 'new');
  for (const replacement of ['0.1', '0.100001', '1', undefined]) {
    const changed = clone(body);
    if (replacement === undefined) delete changed.max_usd;
    else changed.max_usd = replacement;
    await assert.rejects(verify(envelope, 'speech', changed, scope, key));
  }
  const withoutBudget = clone(body);
  delete withoutBudget.max_usd;
  const absent = await prepare('speech', withoutBudget, scope, key, 'cli', 'new');
  assert.deepEqual(absent.payload.budget, { present: false });
});

test('invalid budget values reject before a plan exists', async () => {
  for (const value of [null, 0.1, '-1', '+1', '01', '1e-3', '0.1234567', 'NaN', ' 1']) {
    await assert.rejects(prepare('image', { ...body, max_usd: value }, scope, key, 'cli', 'new'), /decimal string/);
  }
});

test('reference order and absent versus explicit values are bound', async () => {
  const envelope = await prepare('video', body, scope, key, 'cli', 'new');
  await assert.rejects(verify(envelope, 'video', { ...body, references: [...body.references].reverse() }, scope, key));
  await assert.rejects(verify(envelope, 'video', { ...body, mode: null }, scope, key));
});

test('retry mode or resume intent cannot create a fresh random key', async () => {
  await assert.rejects(prepare('image', body, scope, key, 'retry', 'new'), /original persistent plan/);
  await assert.rejects(prepare('image', body, scope, key, 'cli', 'resume'), /original persistent plan/);
});

test('missing stable identity rejects; execution id and item index are not substituted', async () => {
  for (const field of Object.keys(scope)) {
    await assert.rejects(prepare('image', body, { ...scope, [field]: '' }, key, 'cli', 'new'), /Stable operation identity/);
  }
  await assert.rejects(prepare('image', body, { ...scope, itemIndex: 0 }, key, 'cli', 'new'));
});

test('non-JSON, sparse, prototype and oversized requests reject safely', async () => {
  const cycle = {}; cycle.self = cycle;
  const sparse = new Array(2); sparse[1] = 'x'; sparse.foo = 'x';
  for (const value of [NaN, Infinity, -0, undefined, BigInt(1), new Date(), cycle, sparse, JSON.parse('{"__proto__":1}'), 'x'.repeat(65537)]) {
    assert.throws(() => canonical(value));
  }
  await assert.rejects(prepare('image', { model: 'synthetic', parameter: undefined }, scope, key, 'cli', 'new'));
});

test('credential errors and integrity failures never expose credential material', async () => {
  const envelope = await prepare('image', body, scope, key, 'cli', 'new');
  try { await verify(envelope, 'image', body, scope, 'synthetic-secret-that-must-not-leak'); assert.fail(); }
  catch (error) { assert.ok(!String(error).includes('synthetic-secret-that-must-not-leak')); }
  for (const badKey of ['', ' synthetic', 'synthetic\nvalue']) await assert.rejects(prepare('image', body, scope, badKey, 'cli', 'new'));
});

test('generation stays disabled independently of a valid envelope', async () => {
  const envelope = await prepare('image', body, scope, key, 'cli', 'new');
  await verify(envelope, 'image', body, scope, key);
  assert.throws(generationUnavailable, /Generation remains disabled/);
});

test('prepare snapshots scope and request before asynchronous cryptography', async () => {
  const mutableScope = clone(scope);
  const mutableBody = clone(body);
  const pending = prepare('image', mutableBody, mutableScope, key, 'cli', 'new');
  mutableScope.stableItemId = 'later_item';
  mutableBody.max_usd = '999';
  const envelope = await pending;
  const verified = await verify(envelope, 'image', body, scope, key);
  assert.deepEqual(verified.payload.scope, scope);
  assert.equal(verified.payload.budget.decimal, body.max_usd);
});

test('verify returns the exact authenticated snapshot despite concurrent caller mutation', async () => {
  const envelope = await prepare('image', body, scope, key, 'cli', 'new');
  const original = clone(envelope);
  const mutableBody = clone(body);
  const mutableScope = clone(scope);
  const pending = verify(envelope, 'image', mutableBody, mutableScope, key);
  envelope.payload.operationId = '0'.repeat(36);
  envelope.payload.idempotencyKey = 'n8n-' + envelope.payload.operationId;
  envelope.payload.requestJson = '{}';
  envelope.tag = '0'.repeat(64);
  mutableBody.max_usd = '999';
  mutableScope.businessBatchId = 'later_batch';
  assert.deepEqual(await pending, original);
});
