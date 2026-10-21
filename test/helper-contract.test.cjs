const assert = require('node:assert/strict');
const { test } = require('node:test');
const { Cuddler } = require('../dist/nodes/Cuddler/Cuddler.node.js');
const { CuddlerApi } = require('../dist/credentials/CuddlerApi.credentials.js');
const { readOperation } = require('../dist/nodes/Cuddler/transport.js');

function context(operations, options = {}) {
  const calls = [];
  const ctx = {
    getInputData: () => operations.map((operation) => ({ json: { operation } })),
    getMode: () => options.mode ?? 'manual',
    getNodeParameter(name, itemIndex, fallback) {
      if (name === 'operation') return operations[itemIndex];
      if (name === 'id') return options.ids?.[itemIndex] ?? 'vid_fixture';
      if (name === 'estimateBody') return options.body ?? '{}';
      return fallback;
    },
    getNode: () => ({ id: 'fixture', name: 'Cuddler', type: 'cuddler', typeVersion: 1, position: [0, 0], parameters: {} }),
    continueOnFail: () => options.continueOnFail ?? false,
    helpers: {
      async httpRequest(request) {
        calls.push({ kind: 'anonymous', request });
        if (options.error) throw options.error;
        return options.result ?? { object: 'list', data: [{ id: 'fixture-model' }] };
      },
      async httpRequestWithAuthentication(credentialType, request) {
        assert.equal(this, ctx, 'authenticated n8n helper must bind execute context');
        calls.push({ kind: 'authenticated', credentialType, request });
        if (options.error) throw options.error;
        return options.result ?? { id: request.url.split('/').at(-1), status: 'succeeded' };
      },
    },
  };
  return { ctx, calls };
}

test('public catalog uses anonymous helper and fixed origin without credentials', async () => {
  const { ctx, calls } = context(['publicCatalog']);
  const result = await new Cuddler().execute.call(ctx);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].kind, 'anonymous');
  assert.equal(calls[0].request.url, 'https://api.cuddler.ai/v1/catalog');
  assert.equal(calls[0].request.method, 'GET');
  assert.equal(calls[0].request.disableFollowRedirect, true);
  assert.equal(calls[0].request.headers, undefined);
  assert.deepEqual(result[0][0].pairedItem, { item: 0 });
});

test('authenticated operations use the n8n credential helper and no raw header', async () => {
  const { ctx, calls } = context(['listModels', 'getBalance']);
  await new Cuddler().execute.call(ctx);
  assert.deepEqual(calls.map((call) => call.request.url), [
    'https://api.cuddler.ai/v1/models', 'https://api.cuddler.ai/v1/balance',
  ]);
  for (const call of calls) {
    assert.equal(call.kind, 'authenticated');
    assert.equal(call.credentialType, 'cuddlerApi');
    assert.equal(call.request.headers, undefined);
    assert.equal(call.request.disableFollowRedirect, true);
  }
});

for (const [operation, path] of Object.entries({
  getVideo: 'videos', getImage: 'images', getSpeech: 'speech', getMusic: 'music', getUpload: 'uploads',
})) {
  test(`${operation} reads the saved ID exactly once and makes zero create POSTs`, async () => {
    const { ctx, calls } = context([operation], { ids: ['saved_original_42'] });
    await new Cuddler().execute.call(ctx);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].request.url, `https://api.cuddler.ai/v1/${path}/saved_original_42`);
    assert.equal(calls[0].request.method, 'GET');
  });
}

for (const [operation, path] of Object.entries({
  estimateVideo: 'videos', estimateImage: 'images', estimateSpeech: 'speech', estimateMusic: 'music',
})) {
  test(`${operation} can only POST to the estimate endpoint with exact body`, async () => {
    const body = { model: 'fixture-model', duration: 5 };
    const { ctx, calls } = context([operation], { body: JSON.stringify(body) });
    await new Cuddler().execute.call(ctx);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].request.url, `https://api.cuddler.ai/v1/${path}/estimate`);
    assert.equal(calls[0].request.method, 'POST');
    assert.deepEqual(calls[0].request.body, body);
  });
}

test('reordered saved IDs preserve output item linking; no retryOf storage claim', async () => {
  const { ctx, calls } = context(['getVideo', 'getVideo'], { ids: ['vid_second', 'vid_first'] });
  const [results] = await new Cuddler().execute.call(ctx);
  assert.deepEqual(results.map((item) => item.json.id), ['vid_second', 'vid_first']);
  assert.deepEqual(results.map((item) => item.pairedItem), [{ item: 0 }, { item: 1 }]);
  assert.ok(calls.every((call) => call.request.method === 'GET'));
});

test('same read inputs produce separate outputs without mutating input items', async () => {
  const { ctx, calls } = context(['getImage', 'getImage'], { ids: ['img_same', 'img_same'] });
  const input = ctx.getInputData();
  ctx.getInputData = () => input;
  const before = structuredClone(input);
  const [results] = await new Cuddler().execute.call(ctx);
  assert.deepEqual(input, before);
  assert.equal(results.length, 2);
  assert.equal(calls.length, 2);
});

test('generation, prototype keys and arbitrary paths fail before any helper call', async () => {
  for (const operation of ['createVideo', 'createImage', 'createSpeech', 'createMusic', '__proto__', 'constructor', 'https://other.invalid']) {
    const { ctx, calls } = context([operation]);
    await assert.rejects(() => readOperation(ctx, operation, '', {}), /Generation is unavailable/);
    assert.equal(calls.length, 0);
  }
});

test('path and query injection in IDs fails before any helper call', async () => {
  for (const id of ['../balance', 'vid_one?key=secret', '//other.invalid', 'vid_one#fragment', '', 'a'.repeat(129)]) {
    const { ctx, calls } = context(['getVideo']);
    await assert.rejects(() => readOperation(ctx, 'getVideo', id, {}), /valid model, job or upload ID/);
    assert.equal(calls.length, 0);
  }
});

test('request errors do not expose credentials, prompt, signed URL or original input', async () => {
  const secret = 'fixture-secret-not-a-real-key';
  const { ctx, calls } = context(['getVideo'], { continueOnFail: true, error: new Error(`Authorization: Bearer ${secret} signed-url prompt`) });
  const result = await new Cuddler().execute.call(ctx);
  assert.equal(calls.length, 1);
  const serialized = JSON.stringify(result);
  assert.equal(serialized.includes(secret), false);
  assert.equal(serialized.includes('signed-url'), false);
  assert.equal(serialized.includes('prompt'), false);
  assert.deepEqual(result[0][0].pairedItem, { item: 0 });
});

test('malformed estimate parameters and invalid responses fail closed', async () => {
  for (const body of ['[]', 'null', '"string"', '{']) {
    const { ctx, calls } = context(['estimateImage'], { body });
    await assert.rejects(() => new Cuddler().execute.call(ctx), /Cuddler request failed/);
    assert.equal(calls.length, 0);
  }
  for (const result of [[], 'not-json', 3]) {
    const { ctx } = context(['publicCatalog'], { result });
    await assert.rejects(() => new Cuddler().execute.call(ctx), /Cuddler request failed/);
  }
});

test('credentials use password field, header-only Bearer and a fixed test origin', () => {
  const credential = new CuddlerApi();
  assert.equal(credential.properties[0].typeOptions.password, true);
  assert.deepEqual(credential.authenticate.properties, { headers: { Authorization: '=Bearer {{$credentials.apiKey}}' } });
  assert.equal(credential.test.request.baseURL, 'https://api.cuddler.ai');
  assert.equal(credential.test.request.url, '/v1/balance');
});
