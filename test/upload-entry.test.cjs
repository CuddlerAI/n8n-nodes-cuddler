'use strict';
// Synthetic public SDK/API boundary, actual built Cuddler.execute entry. These
// fixture bytes do not prove decoding, native blob retention, Cloud or billing.
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { UploadBoundary, binaryItem, clone, sha } = require('./upload-entry-harness.cjs');
const { keyA, keyB } = require('./file-entry-harness.cjs');
const { ProtectedPlanStore } = require('../dist/nodes/Cuddler/recovery/ProtectedPlanStore.js');
const { assertUploadMetadata } = require('../dist/nodes/Cuddler/recovery/UploadPlan.js');

const HOST = 'assets.fixture.example';
const MIME = { image: 'image/png', video: 'video/mp4', audio: 'audio/mpeg' };
const BYTES = Buffer.from('private-upload-fixture-bytes-0123456789-not-production-media');
const failure = /Cuddler upload refused:/;
const future = () => new Date(Date.now() + 3600000).toISOString();
const past = () => new Date(Date.now() - 3600000).toISOString();

function server(boundary, options = {}) {
  const byKey = new Map(); const byId = new Map();
  let lostCreate = options.lostCreate ? 1 : 0; let lostPut = options.lostPut ? 1 : 0;
  function response(entry, stage, request) {
    const value = { object: 'upload', id: entry.id, kind: entry.kind, content_type: entry.content_type,
      bytes: entry.bytes, status: entry.status, expires_at: entry.expires_at,
      upload_url: `https://${HOST}/${entry.id}?version=${entry.version}&signature=fixture-upload-only`,
      upload_headers: { 'content-type': entry.content_type, 'content-length': String(entry.bytes) } };
    if (options.mutateUpload) options.mutateUpload(value, stage, request, entry);
    return value;
  }
  const transport = {
    async api(request) {
      const stage = request.path === '/v1/uploads' ? 'create' : request.path.endsWith('/complete') ? 'complete' : 'get';
      boundary.events.push({ kind: 'upload-api', stage, request: clone(request) });
      assert.equal(request.origin, 'https://api.cuddler.ai');
      assert.equal(Object.isFrozen(request), true);
      assert.equal(JSON.stringify(request).includes(keyA), false);
      assert.equal(JSON.stringify(request).includes(keyB), false);
      assert.equal(boundary.count('store-write') > 0 || options.allowPriorWrites, true, 'API before persistent winner');
      if (options.beforeApi) await options.beforeApi(request, stage);
      if (options.apiStatus) return { statusCode: options.apiStatus };
      let entry;
      if (stage === 'create') {
        assert.equal(request.method, 'POST'); assert.ok(Object.isFrozen(request.body));
        assert.match(request.idempotencyKey, /^n8n-upload-[a-f0-9]{64}$/);
        assert.deepEqual(Object.keys(request.body).sort(), ['bytes', 'content_type', 'kind']);
        const key = request.credentialBinding + ':' + request.idempotencyKey;
        entry = byKey.get(key);
        if (!entry) {
          entry = { ...clone(request.body), id: 'upl_' + sha(key).slice(0, 24), status: 'pending',
            expires_at: future(), version: 0, stored: false };
          byKey.set(key, entry); byId.set(entry.id, entry);
        }
        entry.version++;
        if (lostCreate-- > 0) throw new Error(`${keyA} fixture lost create response`);
      } else {
        const match = /^\/v1\/uploads\/(upl_[A-Za-z0-9]+)(?:\/complete)?$/.exec(request.path);
        assert.ok(match, 'arbitrary upload API path'); entry = byId.get(match[1]);
        assert.ok(entry, 'original upload ID not found');
        assert.equal(request.body, undefined); assert.equal(request.idempotencyKey, undefined);
        assert.equal(request.method, stage === 'get' ? 'GET' : 'POST');
        if (stage === 'complete') {
          assert.equal(entry.stored, true, 'complete without original bytes');
          entry.status = options.completePending ? 'pending' : 'ready'; entry.expires_at = future();
        }
      }
      return { statusCode: stage === 'create' ? 201 : 200, upload: response(entry, stage, request) };
    },
    async put(request) {
      boundary.events.push({ kind: 'upload-put', url: request.url, headers: clone(request.headers),
        bytes: request.body.length, sha256: sha(request.body), credentials: request.credentials,
        followRedirects: request.followRedirects, maxBytes: request.maxBytes });
      assert.equal(request.method, 'PUT'); assert.equal(request.credentials, 'omit'); assert.equal(request.followRedirects, false);
      assert.equal(Object.isFrozen(request), true); assert.equal(Object.isFrozen(request.headers), true);
      assert.deepEqual(Object.keys(request.headers).sort(), ['content-length', 'content-type']);
      assert.equal(request.maxBytes, request.body.length);
      const id = new URL(request.url).pathname.slice(1); const entry = byId.get(id); assert.ok(entry);
      assert.equal(request.body.length, entry.bytes); entry.stored = true; entry.sha256 = sha(request.body);
      if (options.beforePut) await options.beforePut(request, entry);
      if (lostPut-- > 0) throw new Error(`${keyA} fixture lost PUT response`);
      return { statusCode: options.putStatus ?? 204 };
    },
  };
  return { upload: { transport, trustedAssetHosts: [HOST] }, byKey, byId,
    api: () => boundary.events.filter(event => event.kind === 'upload-api'),
    posts: () => boundary.events.filter(event => event.kind === 'upload-api' && event.request.method === 'POST'),
  };
}

async function prepare(boundary, fixture, kind = 'image', bytes = BYTES, options = {}) {
  const [[output]] = await boundary.invoke({ resource: 'upload', operation: 'prepareUpload', uploadKind: kind,
    businessBatchId: 'upload-fixture-batch', stableItemId: options.stableItemId ?? 'item-one', inputBinaryProperty: 'data' },
  { upload: fixture.upload, ...options, items: options.items ?? [binaryItem(bytes, MIME[kind])] });
  return { resource: 'upload', operationHandle: output.json.operationHandle, stableItemId: output.json.stableItemId,
    operationId: output.json.operationId, output };
}

function execute(boundary, fixture, pointer, options = {}) {
  return boundary.invoke({ resource: 'upload', operation: 'executePreparedUpload', operationHandle: pointer.operationHandle,
    stableItemId: pointer.stableItemId }, { upload: fixture.upload, ...options });
}

function reset(boundary) { boundary.events.length = 0; }
function noLegacyCreate(boundary) { assert.equal(boundary.count('create'), 0); assert.equal(boundary.count('get'), 0); }
function safeOutput(value) {
  const serialized = JSON.stringify(value);
  for (const secret of [keyA, keyB, HOST, 'fixture-upload-only', BYTES.toString('base64'), 'credentialBinding', 'planJson']) {
    assert.equal(serialized.includes(secret), false, `private output: ${secret}`);
  }
}

async function protectedEntry(boundary, slot) {
  const ledger = JSON.parse(boundary.row.payload);
  const selected = slot ?? Object.keys(ledger.slots)[0]; const entry = ledger.slots[selected]; assert.ok(entry);
  const binding = { workflowId: boundary.workflowId, tableId: boundary.ledgerTableId, rowId: boundary.ledgerRowId };
  const store = await ProtectedPlanStore.open({}, binding, boundary.key);
  const opened = await store.unseal('plan', entry.planJson, selected);
  return { ledger, slot: selected, entry, store, plan: JSON.parse(opened.plaintext) };
}

for (const kind of Object.keys(MIME)) {
  test(`${kind}: prepare encrypts a complete native snapshot before create/PUT/complete`, async () => {
    const boundary = new UploadBoundary(); const fixture = server(boundary);
    const pointer = await prepare(boundary, fixture, kind);
    assert.equal(pointer.output.json.state, 'prepared'); assert.equal(pointer.output.json.ready, false);
    assert.equal(pointer.output.json.uploadId, undefined); assert.equal(fixture.api().length, 0);
    const saved = await protectedEntry(boundary);
    assert.equal(saved.plan.source.descriptor.data, '');
    assert.equal(saved.plan.source.contentType, MIME[kind]); assert.equal(saved.plan.source.kind, kind);
    assert.equal(saved.plan.source.bytes, BYTES.length); assert.equal(saved.plan.source.sha256, sha(BYTES));
    assert.equal(saved.plan.idempotencyKey, 'n8n-upload-' + saved.plan.operationId);
    assert.equal(boundary.blobs.has(saved.plan.source.descriptor.id), true);
    assert.equal(boundary.row.payload.includes(BYTES.toString('base64')), false);
    assert.equal(boundary.row.payload.includes(BYTES.toString('utf8')), false);
    assert.equal(boundary.row.payload.includes(saved.plan.source.descriptor.id), false);
    const write = boundary.events.findIndex(event => event.kind === 'store-write');
    assert.ok(write >= 0); assert.ok(boundary.events.slice(write + 1).some(event => event.kind === 'store-read'));
    const [[ready]] = await execute(boundary, fixture, pointer, { nodeId: 'upload-consumer' });
    assert.equal(ready.json.ready, true); assert.equal(ready.json.state, 'ready'); assert.match(ready.json.uploadId, /^upl_/);
    assert.equal(fixture.byKey.size, 1); assert.equal(boundary.count('upload-put'), 1);
    assert.deepEqual(fixture.api().map(event => event.stage), ['create', 'complete']);
    assert.equal(fixture.byId.get(ready.json.uploadId).sha256, sha(BYTES));
    assert.deepEqual(ready.pairedItem, { item: 0 }); safeOutput([pointer.output, ready]); noLegacyCreate(boundary);
  });
}

test('lost create response replays the exact original key and resolves one upload ID', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary, { lostCreate: true });
  const pointer = await prepare(boundary, fixture);
  await assert.rejects(() => execute(boundary, fixture, pointer), /UPLOAD_RECOVERY_FAILED/);
  assert.equal(fixture.byKey.size, 1); assert.equal((await protectedEntry(boundary)).entry.knownJobId, null);
  const [[ready]] = await execute(boundary, fixture, pointer);
  const creates = fixture.api().filter(event => event.stage === 'create');
  assert.equal(creates.length, 2); assert.deepEqual(creates[0].request, creates[1].request);
  assert.equal(fixture.byKey.size, 1); assert.equal(ready.json.uploadId, [...fixture.byId.keys()][0]);
  noLegacyCreate(boundary);
});

test('lost PUT response retains the original ID/key and only replays the same source', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary, { lostPut: true });
  const pointer = await prepare(boundary, fixture);
  await assert.rejects(() => execute(boundary, fixture, pointer), /UPLOAD_RECOVERY_FAILED/);
  assert.ok((await protectedEntry(boundary)).entry.knownJobId);
  const [[ready]] = await execute(boundary, fixture, pointer);
  assert.equal(fixture.byKey.size, 1); assert.equal(fixture.api().filter(event => event.stage === 'get').length, 1);
  const creates = fixture.api().filter(event => event.stage === 'create'); assert.equal(creates.length, 2);
  assert.deepEqual(creates[0].request, creates[1].request);
  const puts = boundary.events.filter(event => event.kind === 'upload-put');
  assert.equal(puts.length, 2); assert.equal(puts[0].sha256, puts[1].sha256);
  assert.equal(ready.json.uploadId, [...fixture.byId.keys()][0]); noLegacyCreate(boundary);
});

test('known ready upload uses only original GET and does not require incoming binary', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary, { allowPriorWrites: true });
  const pointer = await prepare(boundary, fixture); const [[first]] = await execute(boundary, fixture, pointer);
  reset(boundary);
  const [[second]] = await execute(boundary, fixture, pointer);
  assert.equal(second.json.uploadId, first.json.uploadId);
  assert.deepEqual(fixture.api().map(event => event.stage), ['get']);
  assert.equal(boundary.count('upload-put'), 0); assert.equal(boundary.count('native-blob-read'), 0);
  assert.equal(boundary.count('store-write'), 0); noLegacyCreate(boundary);
});

test('expired presign on the persisted upload only completes that original ID', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary, { lostPut: true, allowPriorWrites: true });
  const pointer = await prepare(boundary, fixture);
  await assert.rejects(() => execute(boundary, fixture, pointer), failure);
  const entry = [...fixture.byId.values()][0]; entry.expires_at = past(); reset(boundary);
  const [[ready]] = await execute(boundary, fixture, pointer);
  assert.equal(ready.json.uploadId, entry.id);
  assert.deepEqual(fixture.api().map(event => event.stage), ['get', 'complete']);
  assert.equal(boundary.count('upload-put'), 0); assert.equal(boundary.count('native-blob-read'), 0);
  assert.equal(fixture.byKey.size, 1); noLegacyCreate(boundary);
});

for (const change of ['deleted', 'changed']) {
  test(`original native blob ${change} refuses before any create POST`, async () => {
    const boundary = new UploadBoundary(); const fixture = server(boundary, { allowPriorWrites: true });
    const pointer = await prepare(boundary, fixture); const saved = await protectedEntry(boundary);
    if (change === 'deleted') boundary.blobs.delete(saved.plan.source.descriptor.id);
    else { const changed = Buffer.from(BYTES); changed[0] ^= 1;
      boundary.blobs.set(saved.plan.source.descriptor.id, changed); }
    reset(boundary);
    await assert.rejects(() => execute(boundary, fixture, pointer), failure);
    assert.equal(fixture.api().length, 0); assert.equal(boundary.count('upload-put'), 0); noLegacyCreate(boundary);
  });
}

test('inline storage refuses explicitly instead of embedding bytes in a durable plan', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary);
  await assert.rejects(() => prepare(boundary, fixture, 'image', BYTES, { inlineStorage: true }), /DURABLE_BINARY_STORAGE_REQUIRED/);
  assert.equal(boundary.count('store-write'), 0); assert.equal(fixture.api().length, 0);
  assert.deepEqual(JSON.parse(boundary.row.payload).slots, {});
});

test('a host descriptor with an empty ID refuses durable storage explicitly', async () => {
  class MissingIdBoundary extends UploadBoundary {
    context(parameters, options) { const context = super.context(parameters, options);
      context.helpers.prepareBinaryData = async (_bytes, fileName, mimeType) => ({ data: 'filesystem-v2', id: '', mimeType, fileName });
      return context; }
  }
  const boundary = new MissingIdBoundary(); const fixture = server(boundary);
  await assert.rejects(() => prepare(boundary, fixture), /DURABLE_BINARY_STORAGE_REQUIRED/);
  assert.equal(boundary.count('store-write'), 0); assert.equal(fixture.api().length, 0);
});

function tamperHandle(value) {
  const prefix = 'cuddler-up1.'; const box = JSON.parse(Buffer.from(value.slice(prefix.length), 'base64url').toString('utf8'));
  box.ciphertext = (box.ciphertext[0] === 'A' ? 'B' : 'A') + box.ciphertext.slice(1);
  return prefix + Buffer.from(JSON.stringify(box)).toString('base64url');
}

for (const change of ['key', 'table', 'row', 'workflow', 'item', 'handle', 'plan', 'idempotency-relation']) {
  test(`${change} change rejects the prepared pointer before upload networking`, async () => {
    const boundary = new UploadBoundary(); const fixture = server(boundary, { allowPriorWrites: true });
    const pointer = await prepare(boundary, fixture);
    if (change === 'key') boundary.key = keyB;
    if (change === 'table') boundary.ledgerTableId = 'DifferentUploadLedger';
    if (change === 'row') boundary.ledgerRowId = 2;
    if (change === 'workflow') boundary.workflowId = 'DifferentUploadWorkflow';
    if (change === 'item') pointer.stableItemId = 'different-item';
    if (change === 'handle') pointer.operationHandle = tamperHandle(pointer.operationHandle);
    if (change === 'plan' || change === 'idempotency-relation') {
      const saved = await protectedEntry(boundary);
      if (change === 'plan') saved.entry.planJson += 'corrupt';
      else { saved.plan.idempotencyKey = 'n8n-upload-' + 'a'.repeat(64);
        saved.entry.planJson = await saved.store.seal('plan', saved.slot, JSON.stringify(saved.plan)); }
      boundary.row.payload = JSON.stringify(saved.ledger);
    }
    reset(boundary);
    await assert.rejects(() => execute(boundary, fixture, pointer), failure);
    assert.equal(fixture.api().length, 0); assert.equal(boundary.count('upload-put'), 0);
    assert.equal(boundary.count('store-write'), 0); noLegacyCreate(boundary);
  });
}

test('tampered persisted upload ID tag fails before original GET', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary, { allowPriorWrites: true });
  const pointer = await prepare(boundary, fixture); await execute(boundary, fixture, pointer);
  const ledger = JSON.parse(boundary.row.payload); const entry = Object.values(ledger.slots)[0];
  entry.knownJobId = entry.knownJobId.slice(0, -1) + (entry.knownJobId.endsWith('a') ? 'b' : 'a');
  boundary.row.payload = JSON.stringify(ledger); reset(boundary);
  await assert.rejects(() => execute(boundary, fixture, pointer), failure);
  assert.equal(fixture.api().length, 0); assert.equal(boundary.count('upload-put'), 0); noLegacyCreate(boundary);
});

test('reordered handles read original descriptors rather than replacement input bytes', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary, { allowPriorWrites: true });
  const firstBytes = Buffer.from('original-first-upload'); const secondBytes = Buffer.from('original-second-upload');
  const first = await prepare(boundary, fixture, 'image', firstBytes, { stableItemId: 'first' });
  const second = await prepare(boundary, fixture, 'image', secondBytes, { stableItemId: 'second' });
  const pointers = [second, first]; reset(boundary);
  const [results] = await boundary.invoke(index => ({ resource: 'upload', operation: 'executePreparedUpload',
    operationHandle: pointers[index].operationHandle, stableItemId: pointers[index].stableItemId }),
  { upload: fixture.upload, nodeId: 'upload-consumer', items: [binaryItem(Buffer.from('replacement')), binaryItem(Buffer.from('replacement'))] });
  assert.deepEqual(results.map(item => item.json.stableItemId), ['second', 'first']);
  assert.deepEqual(results.map(item => item.pairedItem), [{ item: 0 }, { item: 1 }]);
  assert.deepEqual(boundary.events.filter(event => event.kind === 'upload-put').map(event => event.sha256), [sha(secondBytes), sha(firstBytes)]);
  assert.equal(boundary.count('native-blob-write'), 0); noLegacyCreate(boundary);
});

test('concurrent same-item same-source prepare returns the single persistent winner', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary);
  const [first, second] = await Promise.all([prepare(boundary, fixture), prepare(boundary, fixture)]);
  assert.equal(first.operationId, second.operationId);
  assert.equal(Object.keys(JSON.parse(boundary.row.payload).slots).length, 1);
  const saved = await protectedEntry(boundary);
  assert.deepEqual(boundary.blobs.get(saved.plan.source.descriptor.id), BYTES);
  assert.equal(fixture.api().length, 0); assert.equal(boundary.count('upload-put'), 0);
});

test('same business slot with changed bytes fails without replacing the winner', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary);
  const first = await prepare(boundary, fixture); const before = boundary.row.payload; reset(boundary);
  await assert.rejects(() => prepare(boundary, fixture, 'image', Buffer.from('different-upload-source')), /UPLOAD_SOURCE_CONFLICT/);
  assert.equal(boundary.row.payload, before); assert.equal((await protectedEntry(boundary)).plan.operationId, first.operationId);
  assert.equal(boundary.count('native-blob-write'), 0); assert.equal(boundary.count('store-write'), 0); assert.equal(fixture.api().length, 0);
});

const badPresigns = [
  ['HTTP asset', value => { value.upload_url = `http://${HOST}/object`; }],
  ['untrusted asset', value => { value.upload_url = 'https://other.fixture.example/object'; }],
  ['API asset', value => { value.upload_url = 'https://api.cuddler.ai/v1/balance'; }],
  ['URL credentials', value => { value.upload_url = `https://user:secret@${HOST}/object`; }],
  ['URL fragment', value => { value.upload_url += '#fragment'; }],
  ['asset port', value => { value.upload_url = `https://${HOST}:8443/object`; }],
  ['Authorization header', value => { value.upload_headers.Authorization = 'Bearer fixture-should-never-be-sent'; }],
  ['cookie header', value => { value.upload_headers.Cookie = 'fixture-cookie'; }],
  ['wrong header length', value => { value.upload_headers['content-length'] = '999'; }],
  ['wrong header MIME', value => { value.upload_headers['content-type'] = 'text/html'; }],
  ['invalid upload status', value => { value.status = 'working'; }],
  ['wrong metadata bytes', value => { value.bytes++; }],
  ['wrong metadata MIME', value => { value.content_type = 'text/html'; }],
];
for (const [name, mutate] of badPresigns) {
  test(`${name} refuses before PUT and does not create a replacement`, async () => {
    const boundary = new UploadBoundary(); const fixture = server(boundary, { mutateUpload: value => mutate(value) });
    const pointer = await prepare(boundary, fixture);
    await assert.rejects(() => execute(boundary, fixture, pointer), failure);
    assert.equal(boundary.count('upload-put'), 0); assert.equal(fixture.byKey.size, 1);
    assert.equal(fixture.api().filter(event => event.stage === 'create').length, 1); noLegacyCreate(boundary);
  });
}

for (const statusCode of [302, 429, 500]) {
  test(`API HTTP ${statusCode} refuses without PUT or hidden retries`, async () => {
    const boundary = new UploadBoundary(); const fixture = server(boundary, { apiStatus: statusCode });
    const pointer = await prepare(boundary, fixture);
    await assert.rejects(() => execute(boundary, fixture, pointer), /UPLOAD_API_FAILED/);
    assert.equal(fixture.api().length, 1); assert.equal(boundary.count('upload-put'), 0);
  });
}

for (const statusCode of [302, 403, 500]) {
  test(`PUT HTTP ${statusCode} does not follow redirects or complete the upload`, async () => {
    const boundary = new UploadBoundary(); const fixture = server(boundary, { putStatus: statusCode });
    const pointer = await prepare(boundary, fixture);
    await assert.rejects(() => execute(boundary, fixture, pointer), /UPLOAD_PUT_FAILED/);
    assert.equal(boundary.count('upload-put'), 1); assert.equal(fixture.api().some(event => event.stage === 'complete'), false);
    assert.equal(fixture.byKey.size, 1); noLegacyCreate(boundary);
  });
}

for (const [kind, mimeType] of [['image', 'text/html'], ['video', 'image/png'], ['audio', 'audio/ogg']]) {
  test(`${kind} disallowed MIME refuses before durable snapshot or networking`, async () => {
    const boundary = new UploadBoundary(); const fixture = server(boundary);
    await assert.rejects(() => prepare(boundary, fixture, kind, BYTES, { items: [binaryItem(BYTES, mimeType)] }), failure);
    assert.equal(boundary.count('native-blob-write'), 0); assert.equal(boundary.count('store-write'), 0); assert.equal(fixture.api().length, 0);
  });
}

test('image above the public cap refuses before persisting a native blob', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary);
  await assert.rejects(() => prepare(boundary, fixture, 'image', Buffer.alloc(30 * 1024 * 1024 + 1)), failure);
  assert.equal(boundary.count('native-blob-write'), 0); assert.equal(boundary.count('store-write'), 0); assert.equal(fixture.api().length, 0);
});

test('the public metadata gate accepts exact three-kind caps and rejects one byte over', () => {
  for (const [kind, mimeType, cap] of [['image', 'image/png', 30], ['video', 'video/mp4', 200], ['audio', 'audio/wav', 50]]) {
    assert.doesNotThrow(() => assertUploadMetadata(kind, mimeType, cap * 1024 * 1024));
    assert.throws(() => assertUploadMetadata(kind, mimeType, cap * 1024 * 1024 + 1));
    assert.throws(() => assertUploadMetadata(kind, mimeType, 0));
  }
});

test('only ready results expose uploadId; pending read remains metadata-only', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary, { completePending: true, allowPriorWrites: true });
  const pointer = await prepare(boundary, fixture); const [[pending]] = await execute(boundary, fixture, pointer);
  assert.equal(pending.json.state, 'pending'); assert.equal(pending.json.ready, false); assert.equal(pending.json.uploadId, undefined);
  reset(boundary);
  const [[read]] = await boundary.invoke({ resource: 'upload', operation: 'readPreparedUpload', operationHandle: pointer.operationHandle }, { upload: fixture.upload });
  assert.equal(read.json.ready, false); assert.equal(read.json.uploadId, undefined);
  assert.deepEqual(fixture.api().map(event => event.stage), ['get']); assert.equal(boundary.count('upload-put'), 0); safeOutput(read);
});

test('read without a persisted ID returns prepared and never creates an upload', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary);
  const pointer = await prepare(boundary, fixture); reset(boundary);
  const [[result]] = await boundary.invoke({ resource: 'upload', operation: 'readPreparedUpload', operationHandle: pointer.operationHandle }, { upload: fixture.upload });
  assert.equal(result.json.state, 'prepared'); assert.equal(result.json.ready, false); assert.equal(result.json.uploadId, undefined);
  assert.equal(fixture.api().length, 0); assert.equal(boundary.count('upload-put'), 0); assert.equal(boundary.count('native-blob-read'), 0); noLegacyCreate(boundary);
});

for (const operation of ['prepareUpload', 'executePreparedUpload', 'readPreparedUpload']) {
  test(`${operation} retry prepare or missing retained handle rejects before protected access`, async () => {
    const boundary = new UploadBoundary(); const fixture = server(boundary);
    const pointer = await prepare(boundary, fixture); boundary.mode = 'retry'; reset(boundary);
    await assert.rejects(() => boundary.invoke({ resource: 'upload', operation, operationHandle: operation === 'prepareUpload' ? pointer.operationHandle : '',
      businessBatchId: 'upload-fixture-batch', stableItemId: 'item-one', uploadKind: 'image', inputBinaryProperty: 'data' },
    { upload: fixture.upload, items: [binaryItem(BYTES)] }), /preflight failed/);
    assert.deepEqual(boundary.events, []);
  });
}

test('workflow lookalike upload transport is ignored in favor of the ordinary installed SDK boundary', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary);
  const pointer = await prepare(boundary, fixture); reset(boundary);
  const originalContext = boundary.context.bind(boundary); let nativeCalls = 0;
  boundary.context = (...args) => { const ctx = originalContext(...args); ctx.getExecutionCancelSignal = () => undefined;
    ctx.helpers.httpRequest = request => { nativeCalls++; assert.equal(request.url, 'https://api.cuddler.ai/v1/uploads'); assert.equal(request.method, 'POST'); throw Error('synthetic SDK boundary unavailable'); }; return ctx; };
  await assert.rejects(() => boundary.invoke({ resource: 'upload', operation: 'executePreparedUpload',
    operationHandle: pointer.operationHandle, transport: fixture.upload.transport, trustedAssetHosts: [HOST] }, { defaultTransport: true }), /UPLOAD_RECOVERY_FAILED/);
  assert.equal(nativeCalls, 1); assert.equal(fixture.api().length, 0); assert.equal(boundary.count('upload-put'), 0);
  assert.equal(boundary.count('store-write'), 0); noLegacyCreate(boundary);
});

test('provider errors remain redacted with continueOnFail and item pairing', async () => {
  const boundary = new UploadBoundary(); const fixture = server(boundary, { beforeApi: async () => {
    throw new Error(`${keyA} ${HOST} fixture-upload-only ${BYTES.toString('base64')}`);
  } });
  const pointer = await prepare(boundary, fixture);
  const [[result]] = await execute(boundary, fixture, pointer, { continueOnFail: true });
  assert.match(result.json.error, /UPLOAD_RECOVERY_FAILED/); assert.deepEqual(result.pairedItem, { item: 0 });
  safeOutput(result); assert.equal(boundary.count('upload-put'), 0);
});
