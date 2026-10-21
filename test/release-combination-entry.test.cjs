'use strict';
// Actual built node/factories, synthetic SDK and HTTP streams only.
const test=require('node:test');
const assert=require('node:assert/strict');
const {Readable}=require('node:stream');
const {createHash}=require('node:crypto');
const {Cuddler}=require('../dist/nodes/Cuddler/Cuddler.node.js');
const {createNativeCuddlerHttpTransports}=require('../dist/nodes/Cuddler/recovery/NativeCuddlerHttpTransports.js');
const {UploadBoundary,binaryItem}=require('./upload-entry-harness.cjs');
const {SyntheticBoundary,keyA}=require('./file-entry-harness.cjs');
const HOST='uploads.public-cdn.com';
const future=()=>new Date(Date.now()+3600000).toISOString();
const uploadBinding=key=>createHash('sha256').update('cuddler.n8n.upload-plan/1\n'+key).digest('hex');
const full=(statusCode,value)=>{const b=Buffer.isBuffer(value)?value:Buffer.from(JSON.stringify(value));return {statusCode,headers:{'content-type':'application/json','content-length':String(b.length)},body:Readable.from([b])}};
function context(b,params,options={}){const c=b.context(params,options);c.getExecutionCancelSignal=()=>undefined;return c}
function factory(c,i){return createNativeCuddlerHttpTransports(c,i,{upload:{trustedAssetHosts:[HOST]}})}
for(const [kind,mime] of [['image','image/png'],['video','video/mp4'],['audio','audio/mpeg']])test(`combined typed factory: ${kind} upload persists original plan then POST/PUT/complete`,async()=>{
 const b=new UploadBoundary(),bytes=Buffer.from('synthetic-combined-'+kind),calls=[];let source,uploadId='upl_'+kind+'a'.repeat(20),puts=0;const metadata=()=>({object:'upload',id:uploadId,...source,status:puts?'ready':'pending',expires_at:future()});
 const node=new Cuddler({trustedTransportFactory:factory});
 const [[prepared]]=await node.execute.call(context(b,{resource:'upload',operation:'prepareUpload',businessBatchId:'combined-batch',stableItemId:kind,inputBinaryProperty:'data',uploadKind:kind},{items:[binaryItem(bytes,mime)]}));
 const ctx=context(b,{resource:'upload',operation:'executePreparedUpload',operationHandle:prepared.json.operationHandle,stableItemId:kind});
 ctx.helpers.httpRequest=async request=>{
  calls.push(request);assert.equal(request.disableFollowRedirect,true);assert.equal(request.maxRedirects,0);
  if(request.url==='https://api.cuddler.ai/v1/uploads'){
   assert.equal(b.count('store-write')>0,true);assert.equal(request.headers.Authorization,'Bearer '+keyA);
   assert.match(request.headers['Idempotency-Key'],/^n8n-upload-[a-f0-9]{64}$/);source=JSON.parse(request.body.toString());
   return full(201,{...metadata(),upload_url:`https://${HOST}/${uploadId}`,upload_headers:{'content-type':mime,'content-length':String(bytes.length)}});
  }
  if(request.url.startsWith(`https://${HOST}/`)){
   assert.equal(request.method,'PUT');assert.equal(request.headers.Authorization,'');assert.equal(request.headers.Cookie,'');assert.deepEqual(request.body,bytes);puts++;
   assert.equal(b.count('store-write')>=2,true,'original ID must persist before PUT');return full(204,Buffer.alloc(0));
  }
  assert.equal(request.url,`https://api.cuddler.ai/v1/uploads/${uploadId}/complete`);assert.equal(request.method,'POST');assert.equal(request.headers.Authorization,'Bearer '+keyA);return full(200,metadata());
 };
 const [[ready]]=await node.execute.call(ctx);assert.equal(ready.json.ready,true);assert.equal(ready.json.uploadId,uploadId);assert.equal(calls.length,3);assert.equal(puts,1);
 assert.equal(JSON.stringify(ready).includes(HOST),false);assert.equal(JSON.stringify(ready).includes(keyA),false);
});

test('combined factory preserves explicit offline upload override precedence',async()=>{
 const b=new UploadBoundary(),bytes=Buffer.from('offline-override');const [[prepared]]=await b.invoke({resource:'upload',operation:'prepareUpload',businessBatchId:'combined-batch',stableItemId:'override',inputBinaryProperty:'data',uploadKind:'image'},{items:[binaryItem(bytes)]});
 let calls=0;const upload={transport:{api:async request=>{calls++;return {statusCode:201,upload:{object:'upload',id:'upl_aaaaaaaaaaaa',kind:'image',content_type:'image/png',bytes:bytes.length,status:'ready',expires_at:future()}}},put:()=>assert.fail('ready upload cannot PUT')}};
 const c=context(b,{resource:'upload',operation:'executePreparedUpload',operationHandle:prepared.json.operationHandle});c.helpers.httpRequest=()=>assert.fail('explicit offline override must win');
 const [[out]]=await new Cuddler({trustedTransportFactory:factory,upload}).execute.call(c);assert.equal(out.json.ready,true);assert.equal(calls,1);
});

test('combined factory preserves explicit offline media and prepared-read overrides',async()=>{
 const b=new SyntheticBoundary();const [[p]]=await b.invoke({resource:'image',operation:'prepareMedia',businessBatchId:'combined-batch',stableItemId:'media-override',requestBody:{model:'synthetic',prompt:'synthetic',max_usd:'0.50'}});
 const params={resource:'image',operationHandle:p.json.operationHandle};const c=context(b,{...params,operation:'executePrepared'});c.helpers.httpRequest=()=>assert.fail('media override must win');
 const [[original]]=await new Cuddler({trustedTransportFactory:factory,recoveryTransport:b.transport}).execute.call(c);
 const preparedRead={transport:{getOriginal:async request=>({statusCode:200,job:{object:'image',id:request.jobId,status:'succeeded'}}),readAsset:()=>assert.fail('poll must not read assets')}};
 const poll=context(b,{...params,operation:'pollPrepared'});poll.helpers.httpRequest=()=>assert.fail('read override must win');
 const [[out]]=await new Cuddler({trustedTransportFactory:factory,preparedRead}).execute.call(poll);assert.equal(out.json.jobId,original.json.jobId);assert.equal(out.json.terminal,true);
});

test('combined per-item factories cannot share upload URL grants',async()=>{
 const b=new UploadBoundary(),c=context(b,{}),calls=[];let source;
 c.helpers.httpRequest=async request=>{calls.push(request);source=JSON.parse(request.body.toString());return full(201,{object:'upload',id:'upl_aaaaaaaaaaaa',...source,status:'pending',expires_at:future(),upload_url:`https://${HOST}/original`,upload_headers:{'content-type':'image/png','content-length':'3'}})};
 const a=factory(c,0),z=factory(c,1);assert.notEqual(a.upload.transport,z.upload.transport);assert.notEqual(a.recoveryTransport,z.recoveryTransport);
 await a.upload.transport.api({origin:'https://api.cuddler.ai',method:'POST',path:'/v1/uploads',body:{kind:'image',content_type:'image/png',bytes:3},idempotencyKey:'n8n-upload-'+'1'.repeat(64),credentialBinding:uploadBinding(keyA)});
 await assert.rejects(()=>z.upload.transport.put({method:'PUT',url:`https://${HOST}/original`,headers:{'content-type':'image/png','content-length':'3'},body:Buffer.from('abc'),maxBytes:3,credentials:'omit',followRedirects:false}));assert.equal(calls.length,1);
});

test('new composite export does not activate a default node from workflow parameters',async()=>{
 const b=new UploadBoundary(),bytes=Buffer.from('default-closed');const [[p]]=await b.invoke({resource:'upload',operation:'prepareUpload',businessBatchId:'combined-batch',stableItemId:'default',inputBinaryProperty:'data',uploadKind:'image'},{items:[binaryItem(bytes)]});
 const c=context(b,{resource:'upload',operation:'executePreparedUpload',operationHandle:p.json.operationHandle,enableNative:true,trustedTransportFactory:'createNativeCuddlerHttpTransports',trustedAssetHosts:[HOST]});c.helpers.httpRequest=()=>assert.fail('workflow fields cannot activate transport');
 await assert.rejects(()=>new Cuddler().execute.call(c),/UPLOAD_RECOVERY_FAILED/);
});

const releaseGuard = import('../scripts/release-guard.mjs');
const releaseHead = 'a'.repeat(40);
function releaseFixture() {
 return {
  environment: {
   GITHUB_ACTIONS: 'true',
   GITHUB_EVENT_NAME: 'workflow_dispatch',
   GITHUB_REPOSITORY: 'CuddlerAI/n8n-nodes-cuddler',
   GITHUB_REF: 'refs/heads/main',
   GITHUB_WORKFLOW_REF: 'CuddlerAI/n8n-nodes-cuddler/.github/workflows/publish.yml@refs/heads/main',
   GITHUB_SHA: releaseHead,
   RELEASE_REPOSITORY_VISIBILITY: 'public',
   RELEASE_RUNNER_ENVIRONMENT: 'github-hosted',
   RELEASE_EXPECTED_SHA: releaseHead,
   RELEASE_EXPECTED_VERSION: '0.1.0',
   RELEASE_ACCEPTANCE_URL: 'https://github.com/CuddlerAI/Cuddler/issues/6454#issuecomment-123',
   RELEASE_ACCEPTANCE_SHA256: 'b'.repeat(64),
  },
  manifest: structuredClone(require('../package.json')),
  head: releaseHead,
 };
}

test('release guard accepts the reviewed canonical public main context', async () => {
 const { validateReleaseContext } = await releaseGuard;
 const { environment, manifest, head } = releaseFixture();
 assert.doesNotThrow(() => validateReleaseContext(environment, manifest, head));
});

for (const [title, differences] of [
 ['fork or private source', [['GITHUB_REPOSITORY', 'ChenShan-2026/n8n-nodes-cuddler'], ['RELEASE_REPOSITORY_VISIBILITY', 'private']]],
 ['non-main ref or different workflow', [['GITHUB_REF', 'refs/tags/v0.1.0'], ['GITHUB_WORKFLOW_REF', 'other/publish.yml@refs/heads/main']]],
 ['automatic event or non-Actions execution', [['GITHUB_EVENT_NAME', 'push'], ['GITHUB_EVENT_NAME', 'pull_request'], ['GITHUB_ACTIONS', 'false']]],
 ['malformed or different dispatch SHA', [['RELEASE_EXPECTED_SHA', 'main'], ['RELEASE_EXPECTED_SHA', 'A'.repeat(40)], ['GITHUB_SHA', 'c'.repeat(40)]]],
 ['different version or self-hosted runner', [['RELEASE_EXPECTED_VERSION', '0.2.0'], ['RELEASE_RUNNER_ENVIRONMENT', 'self-hosted']]],
 ['missing or unsafe acceptance evidence', [['RELEASE_ACCEPTANCE_URL', ''], ['RELEASE_ACCEPTANCE_URL', 'https://example.test/receipt'], ['RELEASE_ACCEPTANCE_URL', 'https://github.com/CuddlerAI/Cuddler/issues/6454?secret=x'], ['RELEASE_ACCEPTANCE_SHA256', '']]],
 ['npm token or password fallback', [['NODE_AUTH_TOKEN', 'synthetic'], ['NPM_TOKEN', 'synthetic'], ['NPM_CONFIG__AUTH', 'synthetic'], ['NPM_CONFIG_PASSWORD', 'synthetic']]],
]) {
 test(`release guard rejects ${title}`, async () => {
  const { validateReleaseContext } = await releaseGuard;
  for (const [field, value] of differences) {
   const { environment, manifest, head } = releaseFixture();
   assert.throws(() => validateReleaseContext({ ...environment, [field]: value }, manifest, head));
  }
 });
}

test('release guard rejects a checkout different from the reviewed dispatch', async () => {
 const { validateReleaseContext } = await releaseGuard;
 const { environment, manifest } = releaseFixture();
 assert.throws(() => validateReleaseContext(environment, manifest, 'c'.repeat(40)));
});

test('release guard rejects package identity, private access and disabled provenance', async () => {
 const { validateReleaseContext } = await releaseGuard;
 for (const changes of [
  { name: 'another-package' },
  { private: true },
  { repository: { url: 'git+https://github.com/other/package.git' } },
  { publishConfig: { access: 'restricted' } },
  { publishConfig: { access: 'public', provenance: false } },
 ]) {
  const { environment, manifest, head } = releaseFixture();
  assert.throws(() => validateReleaseContext(environment, { ...manifest, ...changes }, head));
 }
});

test('release guard rejects missing, unprotected or empty-reviewer environments', async () => {
 const { validateReleaseEnvironment } = await releaseGuard;
 for (const environment of [
  {},
  { name: 'other' },
  { name: 'npm-release', protection_rules: [] },
  { name: 'npm-release', protection_rules: [{ type: 'wait_timer', wait_timer: 30 }] },
  { name: 'npm-release', protection_rules: [{ type: 'required_reviewers', reviewers: [] }] },
  { name: 'npm-release', protection_rules: [{ type: 'required_reviewers', reviewers: [{ type: 'User', reviewer: { id: 0 } }] }] },
 ]) assert.throws(() => validateReleaseEnvironment(environment));
});

test('release guard accepts configured user or team required reviewers', async () => {
 const { validateReleaseEnvironment } = await releaseGuard;
 for (const type of ['User', 'Team']) {
  assert.doesNotThrow(() => validateReleaseEnvironment({
   name: 'npm-release',
   protection_rules: [{ type: 'required_reviewers', reviewers: [{ type, reviewer: { id: 1 } }] }],
  }));
 }
});
