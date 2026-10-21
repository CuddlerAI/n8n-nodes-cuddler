'use strict';
// Synthetic SDK boundary, actual built Cuddler.execute entry. Fixture bytes do
// not prove media decoding, native host persistence, Cloud or paid networking.
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { SyntheticBoundary, keyA, keyB, clone, sha } = require('./file-entry-harness.cjs');

const HOST = 'assets.fixture.example';
const MIME = { video: 'video/mp4', image: 'image/png', speech: 'audio/mpeg', music: 'audio/mpeg' };
const EXT = { video: 'mp4', image: 'png', speech: 'mp3', music: 'mp3' };
const MEDIA = Object.keys(MIME);
const body = media => ({ model: `synthetic-${media}`, prompt: 'private-fixture-prompt', max_usd: '0.50' });
const future = () => new Date(Date.now() + 3600000).toISOString();
const past = () => new Date(Date.now() - 3600000).toISOString();
const failure = /Cuddler request failed/;

async function prepared(boundary, media = 'image', item = 'item-one', execute = true) {
  const [[result]] = await boundary.invoke({ resource: media, operation: 'prepareMedia',
    requestBody: body(media), businessBatchId: 'prepared-file-fixture-batch', stableItemId: item });
  const params = { resource: media, operationHandle: result.json.operationHandle, stableItemId: item };
  let original;
  if (execute) {
    const [[executed]] = await boundary.invoke({ ...params, operation: 'executePrepared' });
    original = executed.json;
  }
  boundary.events.length = 0;
  return { params, original };
}

function job(request, overrides = {}) {
  return { object: request.media, id: request.jobId, status: 'succeeded', output: {
    url: `https://${HOST}/${request.jobId}?signature=fixture-only`, url_expires_at: future(),
    retained_until: future(), content_type: MIME[request.media],
  }, ...overrides };
}

function reader(boundary, media = 'image', overrides = {}) {
  const bytes = Buffer.from(`synthetic-file-bytes-${media}`);
  const transport = {
    async getOriginal(request) {
      boundary.events.push({ kind: 'original-get', request: clone(request) });
      assert.equal(request.origin, 'https://api.cuddler.ai');
      assert.equal(request.method, 'GET');
      assert.equal(Object.isFrozen(request), true);
      assert.equal(JSON.stringify(request).includes(keyA), false);
      assert.equal(JSON.stringify(request).includes(keyB), false);
      return overrides.getOriginal ? await overrides.getOriginal(request) : { statusCode: 200, job: job(request) };
    },
    async readAsset(request) {
      boundary.events.push({ kind: 'asset-get', request: clone(request) });
      assert.equal(request.method, 'GET');
      assert.equal(request.credentials, 'omit');
      assert.equal(request.followRedirects, false);
      assert.deepEqual(request.headers, {});
      assert.equal(Object.isFrozen(request), true);
      assert.equal(Object.isFrozen(request.headers), true);
      return overrides.readAsset ? await overrides.readAsset(request) : {
        statusCode: 200, headers: { 'Content-Type': MIME[media], 'Content-Length': String(bytes.length) }, body: bytes,
      };
    },
  };
  return { bytes, preparedRead: { transport, trustedAssetHosts: [HOST] } };
}

function noCreate(boundary) {
  assert.equal(boundary.count('create'), 0, 'read path created a paid operation');
  assert.equal(boundary.count('get'), 0, 'read path bypassed the prepared-read capability');
  assert.equal(boundary.count('store-write'), 0, 'read path mutated the original ledger');
}

function safeOutput(value) {
  const serialized = JSON.stringify(value);
  for (const forbidden of [keyA, keyB, 'private-fixture-prompt', 'signature=fixture-only', HOST, 'requestJson', 'idempotencyKey']) {
    assert.equal(serialized.includes(forbidden), false, `sensitive output: ${forbidden}`);
  }
}

for (const media of MEDIA) {
  test(`${media}: poll original queued and terminal jobs without file reads or creates`, async () => {
    const boundary = new SyntheticBoundary();
    const { params, original } = await prepared(boundary, media);
    let calls = 0;
    const { preparedRead } = reader(boundary, media, { getOriginal: async request => ({ statusCode: 200,
      job: job(request, ++calls === 1 ? { status: 'queued', stage: 'preparing_references', poll_after_seconds: 2 } : {}) }) });
    const [[queued]] = await boundary.invoke({ ...params, operation: 'pollPrepared' }, { preparedRead });
    assert.equal(queued.json.jobId, original.jobId);
    assert.equal(queued.json.status, 'queued');
    assert.equal(queued.json.terminal, false);
    assert.equal(queued.json.nextPollAfterSeconds, 10);
    assert.equal(queued.json.stage, 'preparing_references');
    const [[done]] = await boundary.invoke({ ...params, operation: 'pollPrepared' }, { preparedRead });
    assert.equal(done.json.status, 'succeeded');
    assert.equal(done.json.terminal, true);
    assert.equal(done.json.nextPollAfterSeconds, undefined);
    assert.deepEqual(done.pairedItem, { item: 0 });
    assert.equal(boundary.count('original-get'), 2);
    assert.equal(boundary.count('asset-get'), 0);
    assert.equal(boundary.count('binary-publish'), 0);
    safeOutput([queued, done]); noCreate(boundary);
  });

  test(`${media}: original file becomes host binary with safe metadata and item link`, async () => {
    const boundary = new SyntheticBoundary();
    const { params, original } = await prepared(boundary, media);
    const { preparedRead, bytes } = reader(boundary, media);
    const [[result]] = await boundary.invoke({ ...params, operation: 'downloadPreparedFile',
      outputBinaryProperty: 'originalFile', maxFileBytes: 4096 }, { preparedRead, nodeId: 'download-consumer' });
    assert.equal(result.json.jobId, original.jobId);
    assert.equal(result.json.state, 'file-delivered');
    assert.equal(result.json.bytes, bytes.length);
    assert.equal(result.json.sha256, sha(bytes));
    assert.equal(result.json.contentType, MIME[media]);
    assert.equal(result.json.filename, `${original.jobId}.${EXT[media]}`);
    assert.deepEqual(Buffer.from(result.binary.originalFile.data, 'base64'), bytes);
    assert.deepEqual(Object.keys(result.binary), ['originalFile']);
    assert.deepEqual(result.pairedItem, { item: 0 });
    assert.equal(boundary.count('original-get'), 1);
    assert.equal(boundary.count('asset-get'), 1);
    assert.equal(boundary.count('binary-publish'), 1);
    assert.equal(boundary.events.find(event => event.kind === 'asset-get').request.maxBytes, 4096);
    safeOutput(result); noCreate(boundary);
  });
}

test('expired URL refresh queries the same original job without reading the old asset', async () => {
  const boundary = new SyntheticBoundary();
  const { params, original } = await prepared(boundary);
  let calls = 0;
  const { preparedRead } = reader(boundary, 'image', { getOriginal: async request => {
    const current = job(request); current.output.url += `&version=${++calls}`;
    if (calls === 1) current.output.url_expires_at = past();
    return { statusCode: 200, job: current };
  } });
  const [[result]] = await boundary.invoke({ ...params, operation: 'downloadPreparedFile' }, { preparedRead });
  assert.equal(result.json.jobId, original.jobId);
  assert.equal(boundary.count('original-get'), 2);
  assert.equal(boundary.count('asset-get'), 1);
  assert.match(boundary.events.find(event => event.kind === 'asset-get').request.url, /version=2$/);
  assert.ok(boundary.events.filter(event => event.kind === 'original-get').every(event => event.request.jobId === original.jobId));
  noCreate(boundary);
});

for (const statusCode of [401, 403, 404, 410]) {
  test(`asset ${statusCode} refreshes original URL once with no replacement job`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
    let reads = 0; let queries = 0;
    const { preparedRead } = reader(boundary, 'image', {
      getOriginal: async request => { const current = job(request); current.output.url += `&version=${++queries}`; return { statusCode: 200, job: current }; },
      readAsset: async () => ++reads === 1 ? { statusCode, headers: {}, body: Buffer.alloc(0) } :
        { statusCode: 200, headers: { 'content-type': 'image/png' }, body: Buffer.from('refreshed-fixture') },
    });
    await boundary.invoke({ ...params, operation: 'downloadPreparedFile' }, { preparedRead });
    assert.equal(queries, 2); assert.equal(reads, 2);
    const urls = boundary.events.filter(event => event.kind === 'asset-get').map(event => event.request.url);
    assert.notEqual(urls[0], urls[1]);
    assert.equal(boundary.count('binary-publish'), 1); noCreate(boundary);
  });
}

for (const [statusCode, retryAfter, expected] of [[429, '120', 120], [503, '2', 10], [429, undefined, 30]]) {
  test(`poll ${statusCode} with Retry-After ${retryAfter} emits a bounded next step`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
    const { preparedRead } = reader(boundary, 'image', { getOriginal: async () => ({ statusCode, headers: retryAfter ? { 'Retry-After': retryAfter } : {} }) });
    const [[result]] = await boundary.invoke({ ...params, operation: 'pollPrepared' }, { preparedRead });
    assert.equal(result.json.state, 'retry-later'); assert.equal(result.json.terminal, false);
    assert.equal(result.json.nextPollAfterSeconds, expected);
    assert.equal(boundary.count('original-get'), 1); assert.equal(boundary.count('asset-get'), 0); noCreate(boundary);
  });
}

for (const retryAfter of ['0', '86401', 'not-a-date']) {
  test(`invalid retry hint ${retryAfter} fails without a spin or asset request`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
    const { preparedRead } = reader(boundary, 'image', { getOriginal: async () => ({ statusCode: 429, headers: { 'Retry-After': retryAfter } }) });
    await assert.rejects(() => boundary.invoke({ ...params, operation: 'pollPrepared' }, { preparedRead }), failure);
    assert.equal(boundary.count('original-get'), 1); assert.equal(boundary.count('asset-get'), 0); noCreate(boundary);
  });
}

for (const status of ['failed', 'canceled']) {
  test(`${status} is terminal polling metadata and never a downloadable replacement`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
    const { preparedRead } = reader(boundary, 'image', { getOriginal: async request => ({ statusCode: 200, job: job(request, { status }) }) });
    const [[result]] = await boundary.invoke({ ...params, operation: 'pollPrepared' }, { preparedRead });
    assert.equal(result.json.status, status); assert.equal(result.json.terminal, true);
    await assert.rejects(() => boundary.invoke({ ...params, operation: 'downloadPreparedFile' }, { preparedRead }), failure);
    assert.equal(boundary.count('asset-get'), 0); noCreate(boundary);
  });
}

const rejectedJobs = [
  ['HTTP failure', () => ({ statusCode: 500 })],
  ['wrong original ID', request => ({ statusCode: 200, job: job(request, { id: 'img_different_original_1234' }) })],
  ['wrong media object', request => ({ statusCode: 200, job: job(request, { object: 'video' }) })],
  ['unknown status', request => ({ statusCode: 200, job: job(request, { status: 'unknown' }) })],
  ['provider error', () => { throw new Error(`${keyA} private-fixture-prompt https://${HOST}/signed-secret`); }],
];
for (const [name, getOriginal] of rejectedJobs) {
  test(`${name} is masked at the actual node entry`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
    const { preparedRead } = reader(boundary, 'image', { getOriginal });
    const [[result]] = await boundary.invoke({ ...params, operation: 'downloadPreparedFile' }, { preparedRead, continueOnFail: true });
    assert.match(result.json.error, failure); safeOutput(result);
    assert.deepEqual(result.pairedItem, { item: 0 });
    assert.equal(result.binary, undefined); assert.equal(boundary.count('asset-get'), 0); noCreate(boundary);
  });
}

const rejectedOutput = [
  ['retention expired', output => { output.retained_until = past(); }],
  ['retention missing', output => { delete output.retained_until; }],
  ['expiry malformed', output => { output.url_expires_at = 'invalid'; }],
  ['insecure URL', output => { output.url = `http://${HOST}/file`; }],
  ['untrusted URL', output => { output.url = 'https://other.fixture.example/file'; }],
  ['API URL', output => { output.url = 'https://api.cuddler.ai/v1/balance'; }],
  ['credential URL', output => { output.url = `https://user:pass@${HOST}/file`; }],
  ['nonstandard port', output => { output.url = `https://${HOST}:8443/file`; }],
  ['fragment URL', output => { output.url = `https://${HOST}/file#fragment`; }],
];
for (const [name, mutate] of rejectedOutput) {
  test(`${name} fails before any byte request`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
    const { preparedRead } = reader(boundary, 'image', { getOriginal: async request => {
      const current = job(request); mutate(current.output); return { statusCode: 200, job: current };
    } });
    await assert.rejects(() => boundary.invoke({ ...params, operation: 'downloadPreparedFile' }, { preparedRead }), failure);
    assert.equal(boundary.count('asset-get'), 0); assert.equal(boundary.count('binary-publish'), 0); noCreate(boundary);
  });
}

test('two expired original links stop after two GETs without create or download', async () => {
  const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
  const { preparedRead } = reader(boundary, 'image', { getOriginal: async request => {
    const current = job(request); current.output.url_expires_at = past(); return { statusCode: 200, job: current };
  } });
  await assert.rejects(() => boundary.invoke({ ...params, operation: 'downloadPreparedFile' }, { preparedRead }), failure);
  assert.equal(boundary.count('original-get'), 2); assert.equal(boundary.count('asset-get'), 0); noCreate(boundary);
});

const rejectedAssets = [
  ['oversize', { statusCode: 200, headers: { 'content-type': 'image/png' }, body: Buffer.alloc(65) }],
  ['redirect', { statusCode: 302, headers: { location: 'https://other.fixture.example/file' }, body: Buffer.from('redirect') }],
  ['wrong MIME', { statusCode: 200, headers: { 'content-type': 'text/html' }, body: Buffer.from('wrong') }],
  ['missing MIME', { statusCode: 200, headers: {}, body: Buffer.from('wrong') }],
  ['output MIME mismatch', { statusCode: 200, headers: { 'content-type': 'image/jpeg' }, body: Buffer.from('wrong') }],
  ['length mismatch', { statusCode: 200, headers: { 'content-type': 'image/png', 'content-length': '99' }, body: Buffer.from('short') }],
  ['invalid length', { statusCode: 200, headers: { 'content-type': 'image/png', 'content-length': '-1' }, body: Buffer.from('bad') }],
  ['empty bytes', { statusCode: 200, headers: { 'content-type': 'image/png' }, body: Buffer.alloc(0) }],
  ['non-buffer body', { statusCode: 200, headers: { 'content-type': 'image/png' }, body: new Uint8Array([1, 2]) }],
];
for (const [name, response] of rejectedAssets) {
  test(`${name} never publishes partial or untrusted binary`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
    const { preparedRead } = reader(boundary, 'image', { readAsset: async () => response });
    await assert.rejects(() => boundary.invoke({ ...params, operation: 'downloadPreparedFile', maxFileBytes: 64 }, { preparedRead }), failure);
    assert.equal(boundary.count('asset-get'), 1); assert.equal(boundary.count('binary-publish'), 0); noCreate(boundary);
  });
}

function tamperHandle(opaque) {
  const prefix = 'cuddler-op1.';
  const box = JSON.parse(Buffer.from(opaque.slice(prefix.length), 'base64url').toString('utf8'));
  box.ciphertext = (box.ciphertext[0] === 'A' ? 'B' : 'A') + box.ciphertext.slice(1);
  return prefix + Buffer.from(JSON.stringify(box)).toString('base64url');
}

for (const change of ['handle', 'key', 'table', 'row', 'workflow', 'media', 'item', 'body', 'job-tag']) {
  test(`${change} tampering refuses before original GET or asset access`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
    const input = { ...params, operation: 'downloadPreparedFile' };
    if (change === 'handle') input.operationHandle = tamperHandle(input.operationHandle);
    if (change === 'key') boundary.key = keyB;
    if (change === 'table') boundary.ledgerTableId = 'DifferentFixtureTable';
    if (change === 'row') boundary.ledgerRowId = 2;
    if (change === 'workflow') boundary.workflowId = 'DifferentFixtureWorkflow';
    if (change === 'media') input.resource = 'video';
    if (change === 'item') input.stableItemId = 'different-item';
    if (change === 'body') input.requestBody = { ...body('image'), prompt: 'changed' };
    if (change === 'job-tag') {
      const ledger = JSON.parse(boundary.row.payload); const entry = Object.values(ledger.slots)[0];
      entry.knownJobId = entry.knownJobId.slice(0, -1) + (entry.knownJobId.endsWith('a') ? 'b' : 'a');
      boundary.row.payload = JSON.stringify(ledger);
    }
    const { preparedRead } = reader(boundary);
    await assert.rejects(() => boundary.invoke(input, { preparedRead }), failure);
    assert.equal(boundary.count('original-get'), 0); assert.equal(boundary.count('asset-get'), 0);
    assert.equal(boundary.count('binary-publish'), 0); noCreate(boundary);
  });
}

for (const phase of ['job-read', 'asset-read']) {
  test(`protected key changes during ${phase} fail before binary publication`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
    const { preparedRead } = reader(boundary, 'image', {
      getOriginal: async request => { if (phase === 'job-read') boundary.key = keyB; return { statusCode: 200, job: job(request) }; },
      readAsset: async () => { boundary.key = keyB; return { statusCode: 200, headers: { 'content-type': 'image/png' }, body: Buffer.from('fixture') }; },
    });
    await assert.rejects(() => boundary.invoke({ ...params, operation: 'downloadPreparedFile' }, { preparedRead }), failure);
    assert.equal(boundary.count('binary-publish'), 0);
    assert.equal(boundary.count('asset-get'), phase === 'job-read' ? 0 : 1); noCreate(boundary);
  });
}

test('key change during asset 403 prevents a second original GET', async () => {
  const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
  const { preparedRead } = reader(boundary, 'image', { readAsset: async () => {
    boundary.key = keyB;
    return { statusCode: 403, headers: {}, body: Buffer.alloc(0) };
  } });
  await assert.rejects(() => boundary.invoke({ ...params, operation: 'downloadPreparedFile' }, { preparedRead }), failure);
  assert.equal(boundary.count('original-get'), 1);
  assert.equal(boundary.count('asset-get'), 1);
  assert.equal(boundary.count('binary-publish'), 0);
  noCreate(boundary);
});

for (const operation of ['pollPrepared', 'downloadPreparedFile']) {
  test(`${operation} missing known job fails with no create fallback`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary, 'image', 'prepared-only', false);
    const { preparedRead } = reader(boundary);
    await assert.rejects(() => boundary.invoke({ ...params, operation }, { preparedRead }), failure);
    assert.equal(boundary.count('original-get'), 0); assert.equal(boundary.count('asset-get'), 0); noCreate(boundary);
  });

  test(`${operation} retry without retained handle rejects before protected access`, async () => {
    const boundary = new SyntheticBoundary(); const { params } = await prepared(boundary);
    boundary.mode = 'retry'; const { preparedRead } = reader(boundary);
    await assert.rejects(() => boundary.invoke({ ...params, operation, operationHandle: '' }, { preparedRead }), /preflight failed/);
    assert.deepEqual(boundary.events, []);
  });
}

test('reordered original handles download their own jobs and preserve pairedItem', async () => {
  const boundary = new SyntheticBoundary();
  const first = await prepared(boundary, 'image', 'first');
  const second = await prepared(boundary, 'image', 'second');
  boundary.events.length = 0;
  const { preparedRead } = reader(boundary);
  const inputs = [second.params, first.params];
  const [results] = await boundary.invoke(index => ({ ...inputs[index], operation: 'downloadPreparedFile' }),
    { preparedRead, nodeId: 'downstream-consumer', items: [{ json: { input: 'second' } }, { json: { input: 'first' } }] });
  assert.deepEqual(results.map(item => item.json.jobId), [second.original.jobId, first.original.jobId]);
  assert.deepEqual(results.map(item => item.json.stableItemId), ['second', 'first']);
  assert.deepEqual(results.map(item => item.pairedItem), [{ item: 0 }, { item: 1 }]);
  assert.equal(boundary.count('binary-publish'), 2); noCreate(boundary);
});

test('all download inputs detach before the first credential await', async () => {
  const boundary = new SyntheticBoundary();
  const first = await prepared(boundary, 'image', 'snapshot-first');
  const second = await prepared(boundary, 'image', 'snapshot-second');
  boundary.events.length = 0;
  const parameters = [first, second].map(value => ({ ...value.params, operation: 'downloadPreparedFile',
    outputBinaryProperty: 'originalFile', maxFileBytes: 4096 }));
  const { preparedRead } = reader(boundary);
  let changed = false;
  const [results] = await boundary.invoke(index => parameters[index], { preparedRead, items: [{ json: {} }, { json: {} }],
    onCredentials: async () => {
      if (changed) return; changed = true;
      parameters[1].resource = 'video'; parameters[1].operationHandle = 'tampered-after-snapshot';
      parameters[1].stableItemId = 'changed'; parameters[1].outputBinaryProperty = 'changed'; parameters[1].maxFileBytes = 1;
    } });
  assert.equal(results[1].json.jobId, second.original.jobId);
  assert.deepEqual(Object.keys(results[1].binary), ['originalFile']);
  assert.ok(boundary.events.filter(event => event.kind === 'asset-get').every(event => event.request.maxBytes === 4096));
  noCreate(boundary);
});
