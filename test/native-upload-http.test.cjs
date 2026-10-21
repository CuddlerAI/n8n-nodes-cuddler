'use strict';
// Factory tests use synthetic credentials, public SDK shapes and local streams.
// No helper here dispatches HTTP, resolves DNS, starts a host or accesses an account.
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { createHash, createHmac } = require('node:crypto');
const { Readable } = require('node:stream');
const path = require('node:path');
const { createNativeUploadHttpTransport } = require('../dist/nodes/Cuddler/recovery/NativeUploadHttpTransport.js');
const KEY = 'fixture-only-upload-protected-key-A';
const OTHER_KEY = 'fixture-only-upload-protected-key-B';
const HOST = 'upload-assets.synthetic-regression.net'; // Synthetic allowlist, never resolved.
const ID = 'upl_0123456789abcdef01234567';
const URL = `https://${HOST}/${ID}?signature=fixture-only`;
const BYTES = Buffer.from('synthetic-original-upload-bytes');
const MIME = { image: 'image/png', video: 'video/mp4', audio: 'audio/mpeg' };
const binding = key => createHash('sha256').update('cuddler.n8n.upload-plan/1\n' + key).digest('hex');
const FUTURE = () => new Date(Date.now() + 3600000).toISOString();
const FAILURE = /Cuddler upload transport request was rejected or could not complete\./;

function createRequest(kind = 'image') {
  return { origin: 'https://api.cuddler.ai', method: 'POST', path: '/v1/uploads', credentialBinding: binding(KEY),
    body: { kind, content_type: MIME[kind], bytes: BYTES.length }, idempotencyKey: 'n8n-upload-' + '1'.repeat(64) };
}
function readRequest(complete = false) { return { origin: 'https://api.cuddler.ai', method: complete ? 'POST' : 'GET',
  path: `/v1/uploads/${ID}${complete ? '/complete' : ''}`, credentialBinding: binding(KEY) }; }
function putRequest() { return { method: 'PUT', url: URL, headers: { 'content-type': 'image/png', 'content-length': String(BYTES.length) },
  body: Buffer.from(BYTES), maxBytes: BYTES.length, credentials: 'omit', followRedirects: false }; }
function upload(source = createRequest().body, status = 'pending') {
  return { object: 'upload', id: ID, ...source, status, expires_at: FUTURE(), created_at: FUTURE(),
    ...(status === 'pending' ? { upload_url: URL, upload_headers: { 'content-type': source.content_type, 'content-length': String(source.bytes) } } : {}),
    ignored_provider_extra: 'must-not-return-provider-extra' };
}
function full(statusCode, body, headers = {}) {
  const bytes = Buffer.isBuffer(body) ? body : Buffer.from(JSON.stringify(body));
  return { statusCode, headers: { 'content-type': 'application/json', 'content-length': String(bytes.length), ...headers }, body: Readable.from([bytes]) };
}
function boundary(options = {}) {
  const state = { key: KEY, calls: [], credentialReads: 0, parent: new AbortController(), metadata: createRequest().body };
  const context = {
    getExecutionCancelSignal: () => state.parent.signal,
    getCredentials: async (name, index) => { state.credentialReads++; assert.equal(name, 'cuddlerApi'); assert.equal(index, 2);
      if (options.onCredentials) await options.onCredentials(state);
      return { apiKey: state.key }; },
    helpers: { httpRequest: async request => {
      state.calls.push(request);
      assert.equal(Object.isFrozen(request.headers), false);
      request.headers.traceparent = 'synthetic-trace-header';
      assert.equal(request.disableFollowRedirect, true); assert.equal(request.maxRedirects, 0);
      assert.equal(request.sendCredentialsOnCrossOriginRedirect, false); assert.equal(request.skipSslCertificateValidation, false);
      assert.equal(request.encoding, 'stream'); assert.equal(request.returnFullResponse, true); assert.equal(request.ignoreHttpStatusErrors, true);
      assert.ok(request.abortSignal instanceof AbortSignal);
      if (options.handler) return await options.handler(request, state);
      if (request.url.startsWith('https://api.cuddler.ai/')) {
        if (request.body) state.metadata = JSON.parse(request.body.toString('utf8'));
        const object = upload(state.metadata, request.url.endsWith('/complete') ? 'ready' : 'pending');
        if (request.method === 'GET') { delete object.upload_url; delete object.upload_headers; }
        return full(request.url.endsWith('/complete') ? 200 : request.method === 'POST' ? 201 : 200,
          object);
      }
      return full(204, Buffer.alloc(0), { 'content-length': '0' });
    } },
  };
  const factory = createNativeUploadHttpTransport(context, 2, { trustedAssetHosts: [HOST], timeoutMs: options.timeoutMs ?? 1000 });
  return { context, factory, state };
}
async function grant(value) { const result = await value.factory.transport.api(createRequest()); assert.equal(result.upload.id, ID); return result; }
function noSensitiveError(error) {
  assert.match(error.message, FAILURE);
  for (const word of [KEY, OTHER_KEY, HOST, 'fixture-only-secret', 'provider-extra']) assert.equal(error.message.includes(word), false);
  assert.equal(error.cause, undefined); return true;
}

for (const kind of Object.keys(MIME)) {
  test(`${kind}: original create metadata/key and signed PUT are bound to one API grant`, async () => {
    const value = boundary(); const input = createRequest(kind); const result = await value.factory.transport.api(input);
    assert.equal(result.statusCode, 201); assert.equal(result.upload.kind, kind); assert.equal(result.upload.ignored_provider_extra, undefined);
    const request = value.state.calls[0];
    assert.equal(request.url, 'https://api.cuddler.ai/v1/uploads'); assert.equal(request.method, 'POST');
    assert.equal(request.headers.Authorization, `Bearer ${KEY}`); assert.equal(request.headers.Cookie, '');
    assert.equal(request.headers['Idempotency-Key'], input.idempotencyKey);
    assert.deepEqual(JSON.parse(request.body.toString('utf8')), input.body);
    assert.equal(request.allowedDomains, 'api.cuddler.ai');
    const put = putRequest(); put.headers['content-type'] = MIME[kind];
    assert.deepEqual(await value.factory.transport.put(put), { statusCode: 204 });
    const sent = value.state.calls[1];
    assert.equal(sent.url, URL); assert.equal(sent.method, 'PUT'); assert.equal(sent.allowedDomains, HOST);
    assert.equal(sent.headers.Authorization, ''); assert.equal(sent.headers.Cookie, '');
    assert.equal(JSON.stringify(sent.headers).includes(KEY), false); assert.deepEqual(sent.body, BYTES);
    assert.equal(value.state.credentialReads, 4);
  });
}

test('the upload binding domain rejects the media HMAC before calling HTTP', async () => {
  const value = boundary(); const input = createRequest();
  input.credentialBinding = createHmac('sha256', KEY).update('cuddler.n8n.hmac-json/1/credential-binding').digest('hex');
  await assert.rejects(() => value.factory.transport.api(input), noSensitiveError); assert.equal(value.state.calls.length, 0);
});

test('known original GET and complete preserve exact ID/path and omit create body/key', async () => {
  const value = boundary(); await grant(value);
  await value.factory.transport.api(readRequest()); await value.factory.transport.api(readRequest(true));
  for (const [index, request] of value.state.calls.slice(1).entries()) {
    assert.equal(request.url, 'https://api.cuddler.ai' + readRequest(index === 1).path);
    assert.equal(request.method, index === 1 ? 'POST' : 'GET');
    assert.equal(request.body, undefined); assert.equal(request.headers['Idempotency-Key'], undefined);
    assert.equal(request.headers.Authorization, `Bearer ${KEY}`);
  }
  const count = value.state.calls.length;
  await assert.rejects(() => value.factory.transport.put(putRequest()), noSensitiveError);
  assert.equal(value.state.calls.length, count, 'terminal completion must revoke old PUT grant');
});

for (const phase of ['before-api', 'during-api', 'before-put', 'during-put']) {
  test(`key rotation ${phase} rejects without sending the replacement key`, async () => {
    const value = boundary({ handler: async (request, state) => {
      if (request.url.startsWith('https://api.cuddler.ai/')) {
        if (phase === 'during-api') state.key = OTHER_KEY;
        return full(201, upload());
      }
      if (phase === 'during-put') state.key = OTHER_KEY;
      return full(204, Buffer.alloc(0));
    } });
    if (phase === 'before-api') value.state.key = OTHER_KEY;
    if (phase.includes('put')) {
      await grant(value); if (phase === 'before-put') value.state.key = OTHER_KEY;
      await assert.rejects(() => value.factory.transport.put(putRequest()), noSensitiveError);
    } else await assert.rejects(() => value.factory.transport.api(createRequest()), noSensitiveError);
    for (const request of value.state.calls) assert.equal(JSON.stringify(request.headers).includes(OTHER_KEY), false);
    assert.equal(value.state.calls.length, phase === 'before-api' ? 0 : phase === 'during-put' ? 2 : 1);
  });
}

test('create request metadata and original key detach before the credential await', async () => {
  const input = createRequest(); const original = structuredClone(input);
  const value = boundary({ onCredentials: async state => {
    if (state.credentialReads === 1) { input.body.kind = 'audio'; input.body.bytes = 999; input.idempotencyKey = 'changed'; input.path = '/v1/balance'; }
  } });
  await value.factory.transport.api(input);
  const sent = value.state.calls[0]; assert.equal(sent.url, 'https://api.cuddler.ai/v1/uploads');
  assert.deepEqual(JSON.parse(sent.body.toString('utf8')), original.body); assert.equal(sent.headers['Idempotency-Key'], original.idempotencyKey);
});

test('PUT body and headers detach before credential await and accept header-order differences', async () => {
  const input = putRequest(); input.headers = { 'content-length': String(BYTES.length), 'content-type': 'image/png' };
  let mutate = false;
  const value = boundary({ onCredentials: async () => { if (mutate) { input.body.fill(0); input.headers['content-type'] = 'text/html'; } } });
  await grant(value); mutate = true;
  await value.factory.transport.put(input);
  const sent = value.state.calls[1]; assert.deepEqual(sent.body, BYTES); assert.equal(sent.headers['content-type'], 'image/png');
});

const badRequests = [
  ['origin', input => { input.origin = 'https://other.synthetic-regression.net'; }],
  ['path', input => { input.path = '/v1/videos'; }],
  ['query', input => { input.path += '?anything=1'; }],
  ['wrong key format', input => { input.idempotencyKey = 'replacement-key'; }],
  ['extra auth', input => { input.Authorization = 'Bearer fixture-only-secret'; }],
  ['body extras', input => { input.body.filename = 'not-in-approved-contract'; }],
  ['unsupported MIME', input => { input.body.content_type = 'image/gif'; }],
  ['oversize metadata', input => { input.body.bytes = 30 * 1024 * 1024 + 1; }],
  ['zero bytes', input => { input.body.bytes = 0; }],
  ['noninteger bytes', input => { input.body.bytes = 1.5; }],
];
for (const [name, change] of badRequests) {
  test(`invalid ${name} rejects before HTTP`, async () => {
    const value = boundary(); const input = createRequest(); change(input);
    await assert.rejects(() => value.factory.transport.api(input), noSensitiveError); assert.equal(value.state.calls.length, 0);
  });
}

for (const [name, change] of [
  ['wrong original ID', object => { object.id = 'upl_otheroriginal0123456789'; }],
  ['wrong kind', object => { object.kind = 'audio'; object.content_type = 'audio/mpeg'; }],
  ['wrong MIME', object => { object.content_type = 'image/jpeg'; }],
  ['wrong bytes', object => { object.bytes++; }],
  ['unknown status', object => { object.status = 'unknown'; }],
  ['bad expiry', object => { object.expires_at = 'not-a-date'; }],
]) {
  test(`API ${name} cannot grant a signed PUT`, async () => {
    const value = boundary({ handler: async () => { const object = upload(); change(object); return full(200, object); } });
    await assert.rejects(() => value.factory.transport.api(name === 'wrong original ID' ? readRequest() : createRequest()), noSensitiveError);
    const count = value.state.calls.length;
    await assert.rejects(() => value.factory.transport.put(putRequest()), noSensitiveError); assert.equal(value.state.calls.length, count);
  });
}

for (const [name, change] of [
  ['HTTP', object => { object.upload_url = `http://${HOST}/object`; }],
  ['unapproved host', object => { object.upload_url = 'https://other.synthetic-regression.net/object'; }],
  ['API host', object => { object.upload_url = 'https://api.cuddler.ai/v1/balance'; }],
  ['IP literal', object => { object.upload_url = 'https://127.0.0.1/object'; }],
  ['URL userinfo', object => { object.upload_url = `https://user:secret@${HOST}/object`; }],
  ['URL fragment', object => { object.upload_url += '#fragment'; }],
  ['port', object => { object.upload_url = `https://${HOST}:8443/object`; }],
  ['Authorization header', object => { object.upload_headers.Authorization = 'Bearer fixture-only-secret'; }],
  ['Cookie header', object => { object.upload_headers.Cookie = 'fixture-only-secret'; }],
  ['length header', object => { object.upload_headers['content-length'] = '1'; }],
  ['MIME header', object => { object.upload_headers['content-type'] = 'text/html'; }],
]) {
  test(`API ${name} presign is refused without an asset request`, async () => {
    const value = boundary({ handler: async () => { const object = upload(); change(object); return full(201, object); } });
    await assert.rejects(() => value.factory.transport.api(createRequest()), noSensitiveError); assert.equal(value.state.calls.length, 1);
  });
}

for (const [name, change] of [
  ['different URL', input => { input.url += '&changed=1'; }],
  ['Authorization', input => { input.headers.Authorization = 'Bearer fixture-only-secret'; }],
  ['Cookie', input => { input.headers.Cookie = 'fixture-only-secret'; }],
  ['redirect', input => { input.followRedirects = true; }],
  ['credentials', input => { input.credentials = 'include'; }],
  ['cap', input => { input.maxBytes++; }],
  ['body length', input => { input.body = Buffer.concat([input.body, Buffer.from('x')]); }],
]) {
  test(`mutated PUT ${name} is refused before helper dispatch`, async () => {
    const value = boundary(); await grant(value); const input = putRequest(); change(input);
    await assert.rejects(() => value.factory.transport.put(input), noSensitiveError); assert.equal(value.state.calls.length, 1);
  });
}

test('PUT without a same-instance original API grant never dispatches', async () => {
  const value = boundary(); await assert.rejects(() => value.factory.transport.put(putRequest()), noSensitiveError); assert.equal(value.state.calls.length, 0);
});

test('GET cannot invent a presign refresh capability absent from its public contract', async () => {
  const value = boundary({ handler: async () => full(200, upload()) });
  await assert.rejects(() => value.factory.transport.api(readRequest()), noSensitiveError);
  await assert.rejects(() => value.factory.transport.put(putRequest()), noSensitiveError);
  assert.equal(value.state.calls.length, 1);
});

test('default asset allowlist is empty and caller cannot grant a host through the request', async () => {
  const value = boundary(); const factory = createNativeUploadHttpTransport(value.context, 2);
  assert.deepEqual(factory.trustedAssetHosts, []);
  await assert.rejects(() => factory.transport.api(createRequest()), noSensitiveError);
  await assert.rejects(() => factory.transport.put({ ...putRequest(), trustedAssetHosts: [HOST] }), noSensitiveError);
  assert.equal(value.state.calls.length, 1);
});

test('expired pending response cannot grant PUT and does not invent another URL', async () => {
  const value = boundary({ handler: async () => full(201, { ...upload(), expires_at: new Date(0).toISOString() }) });
  const result = await value.factory.transport.api(createRequest()); assert.equal(result.upload.upload_url, undefined);
  await assert.rejects(() => value.factory.transport.put(putRequest()), noSensitiveError); assert.equal(value.state.calls.length, 1);
});

for (const statusCode of [302, 307, 500]) {
  test(`API HTTP ${statusCode} never follows or exposes provider errors`, async () => {
    const value = boundary({ handler: async () => full(statusCode, { error: `${KEY} fixture-only-secret` }, { location: 'https://other.synthetic-regression.net' }) });
    if (statusCode < 400) await assert.rejects(() => value.factory.transport.api(createRequest()), noSensitiveError);
    else assert.deepEqual(await value.factory.transport.api(createRequest()), { statusCode });
    assert.equal(value.state.calls.length, 1);
  });
}

for (const statusCode of [302, 307, 403]) {
  test(`PUT HTTP ${statusCode} never follows a new target or leaks response content`, async () => {
    const value = boundary({ handler: async request => request.method === 'PUT'
      ? full(statusCode, { error: KEY }, { location: 'https://other.synthetic-regression.net' }) : full(201, upload()) });
    await grant(value);
    if (statusCode < 400) await assert.rejects(() => value.factory.transport.put(putRequest()), noSensitiveError);
    else assert.deepEqual(await value.factory.transport.put(putRequest()), { statusCode });
    assert.equal(value.state.calls.length, 2);
  });
}

for (const type of ['advertised oversize', 'stream oversize', 'length mismatch', 'invalid UTF8', 'compression']) {
  test(`API ${type} is bounded and destroys its response stream`, async () => {
    let stream;
    const value = boundary({ handler: async () => {
      const body = type === 'stream oversize' ? Buffer.alloc(1024 * 1024 + 1) : type === 'invalid UTF8' ? Buffer.from([0xff]) : Buffer.from('{}');
      stream = Readable.from([body]); const headers = { 'content-type': 'application/json' };
      if (type === 'advertised oversize') headers['content-length'] = String(1024 * 1024 + 1);
      if (type === 'length mismatch') headers['content-length'] = '99';
      if (type === 'compression') headers['content-encoding'] = 'gzip';
      return { statusCode: 201, headers, body: stream };
    } });
    await assert.rejects(() => value.factory.transport.api(createRequest()), noSensitiveError); assert.equal(stream.destroyed, true);
  });
}

test('PUT response is capped independently of original upload bytes', async () => {
  let stream;
  const value = boundary({ handler: async request => request.method === 'PUT'
    ? { statusCode: 200, headers: { 'content-length': '65537' }, body: stream = Readable.from([Buffer.alloc(65537)]) }
    : full(201, upload()) });
  await grant(value); await assert.rejects(() => value.factory.transport.put(putRequest()), noSensitiveError);
  assert.equal(stream.destroyed, true);
});

test('pre-canceled execution rejects before credentials or helper calls', async () => {
  const value = boundary(); value.state.parent.abort();
  await assert.rejects(() => value.factory.transport.api(createRequest()), noSensitiveError);
  assert.equal(value.state.credentialReads, 0); assert.equal(value.state.calls.length, 0);
});

test('cancellation during helper wait rejects and destroys a late response', async () => {
  let finish; let stream;
  const value = boundary({ handler: async () => await new Promise(resolve => { finish = resolve; }) });
  const pending = value.factory.transport.api(createRequest());
  while (!finish) await new Promise(resolve => setImmediate(resolve));
  value.state.parent.abort(); await assert.rejects(() => pending, noSensitiveError);
  stream = Readable.from([Buffer.from('{}')]); finish({ statusCode: 201, headers: {}, body: stream });
  await new Promise(resolve => setImmediate(resolve)); assert.equal(stream.destroyed, true);
});

test('wall-clock timeout rejects a stalled stream and aborts the helper signal', async () => {
  let stream; const keepAlive = setTimeout(() => {}, 100);
  const value = boundary({ timeoutMs: 5, handler: async () => ({ statusCode: 201, headers: { 'content-type': 'application/json' }, body: stream = new Readable({ read() {} }) }) });
  try { await assert.rejects(() => value.factory.transport.api(createRequest()), noSensitiveError);
    assert.equal(stream.destroyed, true); assert.equal(value.state.calls[0].abortSignal.aborted, true); }
  finally { clearTimeout(keepAlive); }
});

test('arbitrary helper error text and causes are always replaced', async () => {
  const value = boundary({ handler: async () => { throw new Error(`${KEY} ${HOST} fixture-only-secret`, { cause: { raw: KEY } }); } });
  await assert.rejects(() => value.factory.transport.api(createRequest()), noSensitiveError);
});
