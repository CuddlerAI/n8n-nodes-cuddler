'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { RecoveryBarrier } = require('../dist/nodes/Cuddler/recovery/RecoveryBarrier.js');
const { prepare, canonical } = require('../dist/nodes/Cuddler/recovery/ImmutablePlan.js');
const key = 'synthetic-barrier-credential';
const scope = { workflowId: 'workflow_fixture', nodeId: 'node_fixture', businessBatchId: 'batch_fixture', stableItemId: 'item_fixture' };
const body = { model: 'synthetic-model', prompt: 'synthetic-prompt', max_usd: '0.010000' };
// Synthetic capability stubs exercise the barrier only. They are not native,
// durable or host proof; the separate actual host CAS suite owns those claims.
const receipt = (planJson, knownJobId = null) => ({ entry: { planJson, knownJobId }, durableReadback: true });
const approved = (media = 'image') => ({ media, body, scope });

for (const media of ['video', 'image', 'speech', 'music']) {
  test(`${media} requires saveOnce followed by an independent complete readback`, async () => {
    const calls = [];
    let saved;
    const barrier = new RecoveryBarrier({
      async saveOnce(slot, planJson) { calls.push('save'); saved = receipt(planJson); return saved; },
      async read(slot) { calls.push('read'); return saved; },
    });
    const opened = await barrier.prepareAndPersist(approved(media), key, 'cli');
    assert.deepEqual(calls, ['save', 'read']);
    assert.equal(opened.envelope.payload.requestJson, canonical(body));
  });
}

test('another candidate in the same slot uses the authenticated original winner/key', async () => {
  const winner = await prepare('image', body, scope, key, 'cli', 'new');
  let candidate;
  const barrier = new RecoveryBarrier({
    async saveOnce(slot, planJson) { candidate = JSON.parse(planJson); return receipt(canonical(winner)); },
    async read() { return receipt(canonical(winner)); },
  });
  const opened = await barrier.prepareAndPersist(approved(), key, 'cli');
  assert.equal(opened.envelope.payload.idempotencyKey, winner.payload.idempotencyKey);
  assert.notEqual(opened.envelope.payload.idempotencyKey, candidate.payload.idempotencyKey);
});

test('same slot with a different approved request or budget refuses the winner', async () => {
  const winner = await prepare('image', { ...body, max_usd: '0.020000' }, scope, key, 'cli', 'new');
  let reads = 0;
  const barrier = new RecoveryBarrier({
    async saveOnce() { return receipt(canonical(winner)); }, async read() { reads++; return receipt(canonical(winner)); },
  });
  await assert.rejects(barrier.prepareAndPersist(approved(), key, 'cli'), /binding mismatch/);
  assert.equal(reads, 0);
});

test('save failure and absent/failed/mismatched readback all fail closed', async () => {
  const winner = await prepare('image', body, scope, key, 'cli', 'new');
  const another = await prepare('image', body, scope, key, 'cli', 'new');
  for (const scenario of ['save-fail', 'read-fail', 'absent', 'mismatch', 'not-durable']) {
    const barrier = new RecoveryBarrier({
      async saveOnce() { if (scenario === 'save-fail') throw new Error('synthetic store failure'); return receipt(canonical(winner)); },
      async read() {
        if (scenario === 'read-fail') throw new Error('synthetic read failure');
        if (scenario === 'absent') return undefined;
        if (scenario === 'mismatch') return receipt(canonical(another));
        return { ...receipt(canonical(winner)), durableReadback: false };
      },
    });
    await assert.rejects(barrier.prepareAndPersist(approved(), key, 'cli'));
  }
});

test('explicit resume reads without preparing, randomizing, saving or recreating', async () => {
  const winner = await prepare('image', body, scope, key, 'cli', 'new');
  const calls = [];
  const barrier = new RecoveryBarrier({
    async read(slot) { calls.push(slot); return receipt(canonical(winner), 'job_fixture_a'); },
    async saveOnce() { assert.fail('Resume must never create a replacement plan'); },
  });
  const opened = await barrier.resumeExplicit(approved(), key, 'cli');
  assert.equal(opened.knownJobId, 'job_fixture_a');
  assert.equal(opened.envelope.payload.idempotencyKey, winner.payload.idempotencyKey);
  assert.deepEqual(calls, [winner.payload.slot]);
});

test('retry can only read an existing plan and cannot prepare a new plan', async () => {
  let reads = 0;
  const barrier = new RecoveryBarrier({ async read() { reads++; return undefined; }, async saveOnce() { assert.fail(); } });
  await assert.rejects(barrier.resumeExplicit(approved(), key, 'retry'), /persistent plan readback/);
  await assert.rejects(barrier.prepareAndPersist(approved(), key, 'retry'), /original persistent plan/);
  assert.equal(reads, 1);
});

test('rotated same-org key finds the old slot and cannot prepare a replacement operation', async () => {
  const winner = await prepare('image', body, scope, key, 'cli', 'new');
  const oldBytes = canonical(winner);
  const observed = [];
  const barrier = new RecoveryBarrier({
    async read(slot) { observed.push(['read', slot]); return receipt(oldBytes, 'job_fixture_a'); },
    async saveOnce(slot, ignoredCandidate) { observed.push(['save', slot]); return receipt(oldBytes, 'job_fixture_a'); },
  });
  const replacementKey = 'synthetic-same-org-rotated-key';
  await assert.rejects(barrier.resumeExplicit(approved(), replacementKey, 'cli'), /authenticated plan/);
  await assert.rejects(barrier.prepareAndPersist(approved(), replacementKey, 'cli'), /authenticated plan/);
  assert.deepEqual(observed, [['read', winner.payload.slot], ['save', winner.payload.slot]]);
  assert.equal(oldBytes, canonical(winner));
});

test('job ID update needs original plan and verified independent readback', async () => {
  const winner = await prepare('image', body, scope, key, 'cli', 'new');
  let current = receipt(canonical(winner));
  const barrier = new RecoveryBarrier({
    async read() { return current; },
    async recordKnownJob(slot, planJson, jobId) {
      assert.equal(slot, winner.payload.slot);
      assert.equal(planJson, canonical(winner));
      current = receipt(planJson, jobId); return current;
    },
  });
  const result = await barrier.recordOriginalJob(approved(), key, 'cli', 'job_fixture_a');
  assert.equal(result.knownJobId, 'job_fixture_a');
  await assert.rejects(barrier.recordOriginalJob(approved(), key, 'cli', 'job_fixture_b'), /cannot change/);
});

test('lying job update result cannot substitute for persistent readback', async () => {
  const winner = await prepare('image', body, scope, key, 'cli', 'new');
  const barrier = new RecoveryBarrier({
    async read() { return receipt(canonical(winner)); },
    async recordKnownJob() { return receipt(canonical(winner), 'job_fixture_a'); },
  });
  await assert.rejects(barrier.recordOriginalJob(approved(), key, 'cli', 'job_fixture_a'), /readback mismatch/);
});
