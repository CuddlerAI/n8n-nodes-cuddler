"use strict";
// Real ordinary zero-argument compiled entries and installed factory. Only public
// SDK credentials, child persistence, binary and HTTP stream edges are synthetic.
// This is not native-host, real API/provider, asset service or billing evidence.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Readable } = require('node:stream');
const { Cuddler } = require('../dist/nodes/Cuddler/Cuddler.node.js');
const { installedProductionTransportFactory } = require('../dist/nodes/Cuddler/recovery/InstalledProductionTransport.js');
const { SyntheticBoundary, keyA, keyB } = require('./file-entry-harness.cjs');
const { UploadBoundary, binaryItem } = require('./upload-entry-harness.cjs');
const HOST='4e8dd0f84c1d428e392189a299a9fa2c.r2.cloudflarestorage.com';
const ORIGIN='https://api.cuddler.ai';
const paths={video:'videos',image:'images',speech:'speech',music:'music'};
const ids={video:'vid_'+'v'.repeat(24),image:'img_'+'i'.repeat(24),speech:'aud_'+'a'.repeat(24),music:'mus_'+'m'.repeat(20)};
const contentTypes={video:'video/mp4',image:'image/png',speech:'audio/mpeg',music:'audio/mpeg'};
const bytes=Buffer.from('synthetic-original-media-bytes');
const body=media=>({model:'synthetic-'+media,prompt:'PRIVATE_ORIGINAL_PROMPT',max_usd:'0.50'});
const noOptions=()=>new Cuddler();
const full=(value,statusCode=200,headers={})=>{const data=Buffer.isBuffer(value)?value:Buffer.from(JSON.stringify(value));return {statusCode,headers:{'content-type':Buffer.isBuffer(value)?'application/octet-stream':'application/json','content-length':String(data.length),...headers},body:Readable.from([data])};};
function sdk(b,parameters,server,options={}){
 const ctx=b.context(parameters,options);ctx.getExecutionCancelSignal=()=>undefined;
 ctx.helpers.httpRequest=async request=>server(request);
 ctx.helpers.httpRequestWithAuthentication=()=>assert.fail('protected adapter must bind captured key directly');
 return ctx;
}
async function invoke(b,parameters,server,options){return await noOptions().execute.call(sdk(b,parameters,server,options));}
async function prepare(b,media,item,server){
 const [[out]]=await invoke(b,{resource:media,operation:'prepareMedia',businessBatchId:'installed-full-batch',stableItemId:item,requestBody:body(media)},server);
 assert.equal(out.json.state,'prepared');return {resource:media,operationHandle:out.json.operationHandle,stableItemId:item};
}
function failure(error){
 assert.match(error.message,/Cuddler (?:request failed|cancel refused|upload refused)/);
 for(const value of [keyA,keyB,'PRIVATE_ORIGINAL_PROMPT','PRIVATE_PROVIDER_ERROR'])assert.equal(error.message.includes(value),false);
 return true;
}
function mediaServer(media){
 const calls=[];let status='queued',createCount=0;let wrongJob=false;
 const url=`https://${HOST}/cuddler-media-private/fixture/${ids[media]}?synthetic=private`;
 const job=()=>({object:media,id:wrongJob?ids[media].slice(0,-1)+'z':ids[media],status,prompt:'PRIVATE_ORIGINAL_PROMPT',provider_error:'PRIVATE_PROVIDER_ERROR',...(status==='succeeded'?{output:{url,url_expires_at:new Date(Date.now()+60000).toISOString(),retained_until:new Date(Date.now()+7*86400000).toISOString(),...(media==='video'?{}:{content_type:contentTypes[media]})}}:{})});
 const server=async request=>{
  calls.push(request);
  if(request.url===url){
   assert.equal(request.method,'GET');assert.equal(request.headers.Authorization,'');assert.equal(request.headers.Cookie,'');
   assert.equal(request.allowedDomains,HOST);assert.equal(request.disableFollowRedirect,true);assert.equal(request.skipSslCertificateValidation,false);
   return full(Buffer.from(bytes),200,{'content-type':contentTypes[media]});
  }
  assert.equal(request.headers.Authorization,'Bearer '+keyA);assert.equal(request.headers.Cookie,'');assert.equal(request.allowedDomains,'api.cuddler.ai');
  assert.equal(request.disableFollowRedirect,true);assert.equal(request.maxRedirects,0);assert.equal(request.skipSslCertificateValidation,false);
  if(request.url===ORIGIN+'/v1/'+paths[media]&&request.method==='POST'){
   assert.deepEqual(JSON.parse(request.body.toString()),body(media));assert.match(request.headers['Idempotency-Key'],/^n8n-/);createCount++;
   return full(job(),202);
  }
  if(request.url===ORIGIN+'/v1/'+paths[media]+'/'+ids[media]&&request.method==='GET')return full(job());
  if(request.url===ORIGIN+'/v1/'+paths[media]+'/'+ids[media]+'/cancel'&&request.method==='POST'){status='canceled';return full(job());}
  assert.fail('unexpected protected request');
 };
 return {server,calls,url,get creates(){return createCount;},set status(value){status=value;},set wrongJob(value){wrongJob=value;}};
}

test('ordinary installed factory selects complete fixed-origin transports and exact private-storage hosts',()=>{
 const context=new Proxy({}, {get(){assert.fail('factory construction must not touch context');}});
 const selected=installedProductionTransportFactory(context,0);
 assert.deepEqual(Object.keys(selected).sort(),['preparedCancel','preparedRead','recoveryTransport','upload']);
 assert.deepEqual(selected.preparedRead.trustedAssetHosts,[HOST]);assert.deepEqual(selected.upload.trustedAssetHosts,[HOST]);
 assert.deepEqual(Object.keys(require('../dist/nodes/Cuddler/recovery/InstalledProductionTransport.js')),['installedProductionTransportFactory']);
});

for(const media of Object.keys(paths)){
 test(`ordinary zero-argument ${media} executes original plan once, then reads and polls same job`,async()=>{
  const b=new SyntheticBoundary(),s=mediaServer(media),params=await prepare(b,media,media+'-original',s.server);assert.equal(s.calls.length,0);
  const [[created]]=await invoke(b,{...params,operation:'executePrepared'},s.server);
  assert.equal(created.json.jobId,ids[media]);assert.equal(created.json.operationHandle,params.operationHandle);assert.equal(s.creates,1);
  const [[replayed]]=await invoke(b,{...params,operation:'executePrepared'},s.server);
  const [[read]]=await invoke(b,{...params,operation:'readPrepared'},s.server);
  assert.equal(replayed.json.jobId,ids[media]);assert.equal(read.json.jobId,ids[media]);assert.equal(s.creates,1);
  s.status='succeeded';const [[poll]]=await invoke(b,{...params,operation:'pollPrepared'},s.server);
  assert.equal(poll.json.jobId,ids[media]);assert.equal(poll.json.operationHandle,params.operationHandle);assert.equal(poll.json.terminal,true);
  for(const secret of [keyA,'PRIVATE_ORIGINAL_PROMPT','PRIVATE_PROVIDER_ERROR',s.url])assert.equal(JSON.stringify([created,replayed,read,poll]).includes(secret),false);
  assert.equal(s.calls.filter(x=>x.method==='POST').length,1);assert.equal(b.sideEffects().length,0);
 });
 test(`ordinary zero-argument ${media} downloads authenticated original bytes anonymously to native binary`,async()=>{
  const b=new SyntheticBoundary(),s=mediaServer(media),params=await prepare(b,media,media+'-file',s.server);await invoke(b,{...params,operation:'executePrepared'},s.server);s.status='succeeded';
  const [[file]]=await invoke(b,{...params,operation:'downloadPreparedFile',outputBinaryProperty:'data',maxFileBytes:1024},s.server);
  assert.equal(file.json.state,'file-delivered');assert.equal(file.json.jobId,ids[media]);assert.equal(file.json.bytes,bytes.length);assert.equal(file.json.contentType,contentTypes[media]);
  assert.deepEqual(Buffer.from(file.binary.data.data,'base64'),bytes);assert.equal(b.count('binary-publish'),1);assert.equal(s.creates,1);
  assert.equal(s.calls.at(-1).url,s.url);assert.equal(s.calls.at(-1).headers.Authorization,'');
  for(const secret of [keyA,'PRIVATE_ORIGINAL_PROMPT','PRIVATE_PROVIDER_ERROR',s.url])assert.equal(JSON.stringify(file).includes(secret),false);
 });
}
for(const media of ['video','speech','music'])test(`ordinary zero-argument ${media} cancel reads original queued job, then never cancels it twice`,async()=>{
 const b=new SyntheticBoundary(),s=mediaServer(media),params=await prepare(b,media,media+'-cancel',s.server);await invoke(b,{...params,operation:'executePrepared'},s.server);
 const [[first]]=await invoke(b,{...params,operation:'cancelPrepared'},s.server);assert.equal(first.json.status,'canceled');assert.equal(first.json.cancelRequested,true);
 const [[again]]=await invoke(b,{...params,operation:'cancelPrepared'},s.server);assert.equal(again.json.cancelRequested,false);assert.equal(s.creates,1);
 assert.equal(s.calls.filter(x=>x.url.endsWith('/cancel')).length,1);
});
for(const wrong of ['key','job'])test(`ordinary protected API refuses wrong original ${wrong}`,async()=>{
 const b=new SyntheticBoundary(),s=mediaServer('video'),params=await prepare(b,'video','wrong-'+wrong,s.server);await invoke(b,{...params,operation:'executePrepared'},s.server);
 const count=s.calls.length;if(wrong==='key')b.key=keyB;else s.wrongJob=true;
 await assert.rejects(()=>invoke(b,{...params,operation:'pollPrepared'},s.server),failure);assert.equal(s.creates,1);assert.equal(s.calls.length,count+(wrong==='key'?0:1));
});
test('lost initial SDK HTTP response replays identical original body/key before known-job GET',async()=>{
 const b=new SyntheticBoundary(),s=mediaServer('image'),params=await prepare(b,'image','lost-response',s.server);let lost=true;
 const server=async request=>{const result=await s.server(request);if(lost&&request.method==='POST'){lost=false;throw Error('PRIVATE_PROVIDER_ERROR '+keyA);}return result;};
 await assert.rejects(()=>invoke(b,{...params,operation:'executePrepared'},server),failure);
 const [[out]]=await invoke(b,{...params,operation:'executePrepared'},server);assert.equal(out.json.jobId,ids.image);
 const posts=s.calls.filter(x=>x.method==='POST');assert.equal(posts.length,2);assert.equal(posts[0].headers['Idempotency-Key'],posts[1].headers['Idempotency-Key']);assert.deepEqual(posts[0].body,posts[1].body);
 await invoke(b,{...params,operation:'executePrepared'},server);assert.equal(s.calls.at(-1).method,'GET');
});
test('workflow lookalike host/factory fields cannot authorize an untrusted output URL',async()=>{
 const b=new SyntheticBoundary(),s=mediaServer('image'),params=await prepare(b,'image','untrusted-output',s.server);await invoke(b,{...params,operation:'executePrepared'},s.server);
 let calls=0;const server=async request=>{calls++;assert.ok(request.url.startsWith(ORIGIN));return full({object:'image',id:ids.image,status:'succeeded',output:{url:'https://attacker.com/private',url_expires_at:new Date(Date.now()+60000).toISOString(),retained_until:new Date(Date.now()+60000).toISOString(),content_type:'image/png'}});};
 await assert.rejects(()=>invoke(b,{...params,operation:'downloadPreparedFile',outputBinaryProperty:'data',maxFileBytes:1024,enableNative:true,trustedAssetHosts:['attacker.com'],trustedTransportFactory:'native'},server),failure);
 assert.equal(calls,1);assert.equal(b.count('binary-publish'),0);
});

test('ordinary zero-argument upload persists binary, uses original allocation, anonymous PUT and original complete/read',async()=>{
 const b=new UploadBoundary(),input=Buffer.from('synthetic-image-input'),calls=[];const id='upl_'+'u'.repeat(24),url=`https://${HOST}/cuddler-media-private/platform-uploads/fixture/${id}.png?synthetic=private`;let status='pending';
 const projection=()=>({object:'upload',id,kind:'image',content_type:'image/png',bytes:input.length,status,expires_at:new Date(Date.now()+(status==='ready'?30*86400000:900000)).toISOString()});
 const server=async request=>{
  calls.push(request);
  if(request.url===url){assert.equal(request.method,'PUT');assert.equal(request.headers.Authorization,'');assert.equal(request.headers.Cookie,'');assert.equal(request.headers['content-type'],'image/png');assert.equal(request.headers['content-length'],String(input.length));assert.deepEqual(request.body,input);return full(Buffer.alloc(0),200);}
  assert.equal(request.headers.Authorization,'Bearer '+keyA);assert.equal(request.headers.Cookie,'');assert.equal(request.disableFollowRedirect,true);assert.equal(request.skipSslCertificateValidation,false);
  if(request.method==='POST'&&request.url===ORIGIN+'/v1/uploads'){assert.deepEqual(JSON.parse(request.body),{kind:'image',content_type:'image/png',bytes:input.length});assert.match(request.headers['Idempotency-Key'],/^n8n-upload-/);return full({...projection(),upload_url:url,upload_headers:{'content-type':'image/png','content-length':String(input.length)}},201);}
  if(request.method==='POST'&&request.url===ORIGIN+'/v1/uploads/'+id+'/complete'){status='ready';return full(projection());}
  if(request.method==='GET'&&request.url===ORIGIN+'/v1/uploads/'+id)return full(projection());
  assert.fail('unexpected upload request');
 };
 const [[prepared]]=await invoke(b,{resource:'upload',operation:'prepareUpload',businessBatchId:'upload-installed',stableItemId:'input',uploadKind:'image',inputBinaryProperty:'data'},server,{items:[binaryItem(input)]});assert.equal(prepared.json.state,'prepared');assert.equal(calls.length,0);assert.ok(b.blobs.size>0);
 const params={resource:'upload',operationHandle:prepared.json.operationHandle,stableItemId:'input'};
 const [[uploaded]]=await invoke(b,{...params,operation:'executePreparedUpload'},server);assert.equal(uploaded.json.uploadId,id);assert.equal(uploaded.json.ready,true);
 const [[read]]=await invoke(b,{...params,operation:'readPreparedUpload'},server);assert.equal(read.json.uploadId,id);
 const [[resumed]]=await invoke(b,{...params,operation:'executePreparedUpload'},server);assert.equal(resumed.json.uploadId,id);
 assert.equal(calls.filter(x=>x.method==='PUT').length,1);assert.equal(calls.filter(x=>x.url===ORIGIN+'/v1/uploads').length,1);assert.equal(calls.at(-1).method,'GET');
 for(const secret of [keyA,url,input.toString()])assert.equal(JSON.stringify([uploaded,read,resumed]).includes(secret),false);
});

const ordinary = [
  ['publicCatalog', '/catalog', 'GET'], ['listModels', '/models', 'GET'], ['getModel', '/models/existing-id', 'GET'], ['getBalance', '/balance', 'GET'],
  ['getVideo', '/videos/existing-id', 'GET'], ['getImage', '/images/existing-id', 'GET'], ['getSpeech', '/speech/existing-id', 'GET'], ['getMusic', '/music/existing-id', 'GET'], ['getUpload', '/uploads/existing-id', 'GET'],
  ['estimateVideo', '/videos/estimate', 'POST'], ['estimateImage', '/images/estimate', 'POST'], ['estimateSpeech', '/speech/estimate', 'POST'], ['estimateMusic', '/music/estimate', 'POST'],
];
for (const [operation, path, method] of ordinary) test(`zero-argument ordinary ${operation} retains its exact official helper dispatch`, async () => {
  const b = new SyntheticBoundary(); const estimateBody = { model: 'fixture-only', seconds: 1 };
  const ctx = b.context({ resource: 'catalog', operation, id: 'existing-id', estimateBody: JSON.stringify(estimateBody) }); const calls = [];
  ctx.helpers.httpRequest = async request => { assert.equal(operation, 'publicCatalog'); calls.push(request); return { object: 'fixture' }; };
  ctx.helpers.httpRequestWithAuthentication = async function (name, request) { assert.equal(this, ctx); assert.equal(name, 'cuddlerApi'); assert.notEqual(operation, 'publicCatalog'); calls.push(request); return { object: 'fixture' }; };
  const [[out]] = await noOptions().execute.call(ctx);
  assert.equal(calls.length, 1); assert.equal(calls[0].url, 'https://api.cuddler.ai/v1' + path); assert.equal(calls[0].method, method); assert.equal(calls[0].disableFollowRedirect, true);
  assert.deepEqual(calls[0].body, method === 'POST' ? estimateBody : undefined); assert.equal(b.count('credentials'), 0); assert.deepEqual(out.pairedItem, { item: 0 });
});
