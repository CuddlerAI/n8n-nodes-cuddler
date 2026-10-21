'use strict';
// Actual built node dispatch; entirely synthetic SDK/storage/HTTP boundaries.
// No actual retryOf proof: official REST/normal-host execution is a later gate.
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {Readable}=require('node:stream');
const {Cuddler}=require('../dist/nodes/Cuddler/Cuddler.node.js');
const {createNativeCuddlerHttpTransports}=require('../dist/nodes/Cuddler/recovery/NativeCuddlerHttpTransports.js');
const {SyntheticBoundary,keyB,clone,sha}=require('./file-entry-harness.cjs');
const {UploadBoundary,binaryItem}=require('./upload-entry-harness.cjs');
const body=media=>({model:'synthetic-retry-'+media,prompt:'synthetic-private-retry-body',max_usd:'0.010000'});
const failure=/Cuddler request failed|Cuddler upload refused|preflight failed/;
const events=b=>b.events.filter(e=>['create','get','upload-api','upload-put'].includes(e.kind));
function prohibitHistory(b){const original=b.context.bind(b);b.context=(...args)=>{const c=original(...args);c.getExecutionDataById=()=>assert.fail('historical execution lookup is forbidden');return c}}
async function prepared(b,media='image',item='original-item'){
 const [[p]]=await b.invoke({resource:media,operation:'prepareMedia',businessBatchId:'original-business-batch',stableItemId:item,requestBody:body(media)});
 return {resource:media,operationHandle:p.json.operationHandle,stableItemId:item,operationId:p.json.operationId};
}
function params(p,operation='executePrepared'){const {operationId,...rest}=p;return {...rest,operation}}

for(const media of ['video','image','speech','music'])test(`${media}: retained handle retry creates only the original request then known ID GET-only`,async()=>{
 const b=new SyntheticBoundary();prohibitHistory(b);const p=await prepared(b,media);b.mode='retry';b.events=[];
 const [[created]]=await b.invoke(params(p));const original=events(b)[0].request;
 assert.equal(created.json.operationId,p.operationId);assert.equal(events(b).length,1);assert.equal(original.bodyJson,JSON.stringify(body(media),Object.keys(body(media)).sort()));
 const [[read]]=await b.invoke(params(p,'readPrepared'));const [[executeAgain]]=await b.invoke(params(p));
 assert.deepEqual(events(b).map(e=>e.kind),['create','get','get']);assert.equal(read.json.jobId,created.json.jobId);assert.equal(executeAgain.json.jobId,created.json.jobId);
 assert.equal(b.jobs.size,1);assert.equal(created.json.businessBatchId,'original-business-batch');assert.deepEqual(created.pairedItem,{item:0});
});

test('lost response retry reuses exact original body/key and never persists a new plan',async()=>{
 const b=new SyntheticBoundary();const p=await prepared(b);b.fault='lost-create-response';await assert.rejects(()=>b.invoke(params(p)),failure);
 const first=clone(events(b)[0].request);b.mode='retry';b.events=[];prohibitHistory(b);
 const [[out]]=await b.invoke(params(p));const second=events(b)[0].request;
 assert.deepEqual(second,first);assert.equal(b.jobs.size,1);assert.equal(out.json.operationId,p.operationId);
 const old=events(b).length;await b.invoke(params(p));assert.equal(events(b)[old].kind,'get');assert.equal(events(b).filter(e=>e.kind==='create').length,1);
});

for(const operation of ['prepareMedia','prepareUpload'])test(`${operation} retry rejects the whole batch synchronously before first await or factory`,async()=>{
 const b=operation==='prepareUpload'?new UploadBoundary():new SyntheticBoundary();b.mode='retry';let factories=0,parameterReads=[];
 const c=b.context({resource:operation==='prepareUpload'?'upload':'image',operation,requestBody:body('image'),businessBatchId:'must-not-exist',stableItemId:'one',uploadKind:'image',inputBinaryProperty:'data'},{items:[binaryItem(Buffer.from('synthetic'))]});
 const original=c.getNodeParameter;c.getNodeParameter=(...args)=>{parameterReads.push(args[0]);return original(...args)};
 c.getCredentials=()=>assert.fail('prepare retry touched credentials');c.helpers.assertBinaryData=()=>assert.fail('prepare retry touched binary');c.executeWorkflow=()=>assert.fail('prepare retry touched ledger');
 await assert.rejects(()=>new Cuddler({trustedTransportFactory:()=>{factories++;return {}}}).execute.call(c),/preflight failed/);
 assert.equal(factories,0);assert.deepEqual(b.events,[]);assert.deepEqual(parameterReads,['operation']);
});

test('a later prepare item rejects before any earlier valid resume is dispatched',async()=>{
 const b=new SyntheticBoundary();const p=await prepared(b);b.mode='retry';b.events=[];
 const c=b.context(i=>i===0?params(p):{resource:'image',operation:'prepareMedia',requestBody:body('image'),businessBatchId:'new',stableItemId:'new'},{items:[{json:{}},{json:{}}]});let calls=0;
 await assert.rejects(()=>new Cuddler({trustedTransportFactory:()=>{calls++;return {recoveryTransport:b.transport}}}).execute.call(c),/preflight failed/);
 assert.equal(calls,0);assert.deepEqual(b.events,[]);
});

for(const operation of ['executePrepared','readPrepared','pollPrepared','downloadPreparedFile','executePreparedUpload','readPreparedUpload'])for(const handle of ['', 'caller-json-retryOf-123', 'cuddler-op1.!'])test(`${operation}: retry malformed retained handle ${handle.length} refuses before any await`,async()=>{
 const b=new SyntheticBoundary();b.mode='retry';const resource=operation.endsWith('Upload')?'upload':'image';
 await assert.rejects(()=>b.invoke({resource,operation,operationHandle:handle,stableItemId:''}),/preflight failed/);assert.deepEqual(b.events,[]);
});

for(const [name,change] of [
 ['ciphertext', (b,p)=>{p.operationHandle=p.operationHandle.slice(0,-8)+'AAAAAAAA'}],
 ['credential',b=>{b.key=keyB}], ['workflow',b=>{b.workflowId='another-workflow'}],
 ['ledger-table',b=>{b.ledgerTableId='AnotherLedger'}],['ledger-row',b=>{b.ledgerRowId=2}],
 ['media',(b,p)=>{p.resource='video'}],['stable-item',(b,p)=>{p.stableItemId='replacement'}],
 ['request',(b,p)=>{p.requestBody={...body('image'),prompt:'replacement'}}],
 ['budget',(b,p)=>{p.requestBody={...body('image'),max_usd:'0.020000'}}],
 ['missing-plan',b=>{b.row.payload=JSON.stringify({format:'cuddler.n8n.cas-ledger/1',slots:{}})}],
])test(`retry ${name} change cannot authorize a transport call or replacement plan`,async()=>{
 const b=new SyntheticBoundary();const p=params(await prepared(b));change(b,p);b.mode='retry';b.events=[];const before=b.row.payload;
 await assert.rejects(()=>b.invoke(p),failure);assert.equal(events(b).length,0);assert.equal(b.row.payload,before);assert.equal(b.count('store-write'),0);
});

test('JSON retryOf/run ID/batch claims confer no authority and do not rebind a real handle',async()=>{
 const b=new SyntheticBoundary();prohibitHistory(b);const p=await prepared(b);b.mode='retry';b.events=[];
 const [[out]]=await b.invoke({...params(p),retryOf:'invented-root',businessBatchId:'invented-batch'},{items:[{json:{retryOf:'invented-root',executionId:'invented-root',businessBatchId:'invented-batch'}}],executionId:'new-trusted-sdk-root'});
 assert.equal(out.json.operationId,p.operationId);assert.equal(out.json.businessBatchId,'original-business-batch');
 b.events=[];await assert.rejects(()=>b.invoke({resource:'image',operation:'executePrepared',operationHandle:'',retryOf:'invented-root'}),/preflight failed/);assert.deepEqual(b.events,[]);
});

test('retry snapshots retained handle/request/item before the first credential await',async()=>{
 const b=new SyntheticBoundary();const p=await prepared(b);b.mode='retry';b.events=[];
 const input={...params(p),requestBody:body('image')};let changed=false;
 const [[out]]=await b.invoke(input,{onCredentials:async()=>{if(!changed){changed=true;input.operationHandle='replaced';input.requestBody.prompt='replaced';input.stableItemId='replaced'}}});
 assert.equal(out.json.operationId,p.operationId);assert.equal(events(b).length,1);assert.equal(JSON.parse(events(b)[0].request.bodyJson).prompt,'synthetic-private-retry-body');
});

test('retry poll/file retain original known ID and never create',async()=>{
 const b=new SyntheticBoundary();const p=await prepared(b);const [[original]]=await b.invoke(params(p));b.mode='retry';b.events=[];
 let reads=0,assets=0;const bytes=Buffer.from('synthetic-file');const preparedRead={trustedAssetHosts:['asset.fixture.example'],transport:{
 getOriginal:async r=>{reads++;assert.equal(r.jobId,original.json.jobId);return {statusCode:200,job:{object:'image',id:r.jobId,status:'succeeded',output:{url:'https://asset.fixture.example/original',url_expires_at:new Date(Date.now()+60000).toISOString(),retained_until:new Date(Date.now()+60000).toISOString(),content_type:'image/png'}}}},
 readAsset:async r=>{assets++;assert.equal(r.credentials,'omit');return {statusCode:200,body:bytes,headers:{'content-type':'image/png','content-length':String(bytes.length)}}},
 }};
 const [[poll]]=await b.invoke(params(p,'pollPrepared'),{preparedRead});const [[file]]=await b.invoke({...params(p,'downloadPreparedFile'),outputBinaryProperty:'data',maxFileBytes:1024},{preparedRead});
 assert.equal(reads,2);assert.equal(assets,1);assert.equal(poll.json.jobId,original.json.jobId);assert.equal(file.json.jobId,original.json.jobId);assert.equal(events(b).length,0);assert.equal(file.json.sha256,sha(bytes));
});

test('trusted native factory dispatch stays per item in retry, never selected by JSON',async()=>{
 const b=new SyntheticBoundary();const a=await prepared(b,'image','a'),z=await prepared(b,'image','z');await b.invoke(params(a));await b.invoke(params(z));b.mode='retry';b.events=[];
 const order=[z,a],instances=[],calls=[];const c=b.context(i=>params(order[i]),{items:[{json:{enableNative:true}},{json:{}}]});c.getExecutionCancelSignal=()=>undefined;
 c.helpers.httpRequest=async r=>{calls.push(r);const bytes=Buffer.from(JSON.stringify({object:'image',id:r.url.split('/').pop(),status:'succeeded'}));return {statusCode:200,headers:{'content-type':'application/json','content-length':String(bytes.length)},body:Readable.from([bytes])}};
 const node=new Cuddler({trustedTransportFactory:(context,index)=>{const transport=createNativeCuddlerHttpTransports(context,index);instances.push({index,transport});return transport}});
 const [out]=await node.execute.call(c);assert.deepEqual(instances.map(x=>x.index),[0,1]);assert.notEqual(instances[0].transport.recoveryTransport,instances[1].transport.recoveryTransport);assert.deepEqual(out.map(x=>x.json.operationId),[z.operationId,a.operationId]);assert.deepEqual(out.map(x=>x.pairedItem),[{item:0},{item:1}]);assert.equal(calls.length,2);assert.ok(calls.every(r=>r.method==='GET'));
 const off=new SyntheticBoundary(),unknown=await prepared(off);off.mode='retry';off.events=[];
 const offContext=off.context({...params(unknown),enableNative:true,trustedTransportFactory:'native'});offContext.getExecutionCancelSignal=()=>undefined;let installedCalls=0;
 offContext.helpers.httpRequest=async request=>{installedCalls++;assert.equal(request.url,'https://api.cuddler.ai/v1/images');assert.equal(request.method,'POST');const bytes=Buffer.from(JSON.stringify({object:'image',id:'img_'+'i'.repeat(24),status:'queued'}));return {statusCode:202,headers:{'content-type':'application/json'},body:Readable.from([bytes])};};
 const [[installed]]=await new Cuddler().execute.call(offContext);assert.equal(installed.json.jobId,'img_'+'i'.repeat(24));assert.equal(installedCalls,1);assert.equal(events(off).length,0);assert.ok(off.count('store-write')>0);
});

const BYTES=Buffer.from('synthetic-original-upload-bytes');
function uploadServer(b){
 const byKey=new Map(),byId=new Map();let fault;
 const value=e=>({object:'upload',id:e.id,kind:'image',content_type:'image/png',bytes:BYTES.length,status:e.ready?'ready':'pending',expires_at:new Date(Date.now()+60000).toISOString(),...(!e.ready?{upload_url:'https://asset.fixture.example/'+e.id,upload_headers:{'content-type':'image/png','content-length':String(BYTES.length)}}:{})});
 const transport={
 api:async r=>{b.events.push({kind:'upload-api',request:clone(r)});let e;
 if(r.path==='/v1/uploads'){const key=r.credentialBinding+':'+r.idempotencyKey;if(!byKey.has(key)){e={id:'upl_'+sha(key).slice(0,24),ready:false};byKey.set(key,e);byId.set(e.id,e)}else e=byKey.get(key);if(fault==='create'){fault=undefined;throw Error('synthetic lost upload create response')}return {statusCode:201,upload:value(e)}}
 const id=r.path.split('/')[3];e=byId.get(id);assert.ok(e,'original upload ID');if(r.path.endsWith('/complete')){assert.ok(e.stored);e.ready=true}const v=value(e);delete v.upload_url;delete v.upload_headers;return {statusCode:200,upload:v};
 },put:async r=>{b.events.push({kind:'upload-put',request:{...clone(r),body:Buffer.from(r.body)}});assert.deepEqual(r.body,BYTES);assert.equal(r.credentials,'omit');const e=byId.get(r.url.split('/').pop());assert.ok(e);e.stored=true;if(fault==='put'){fault=undefined;throw Error('synthetic lost upload PUT response')}return {statusCode:204}},
 };
 return {upload:{transport,trustedAssetHosts:['asset.fixture.example']},byKey,byId,lose:kind=>{fault=kind}};
}
async function preparedUpload(b,s){const [[p]]=await b.invoke({resource:'upload',operation:'prepareUpload',businessBatchId:'original-upload-batch',stableItemId:'original-upload-item',uploadKind:'image',inputBinaryProperty:'data'},{upload:s.upload,items:[binaryItem(BYTES)]});return p.json}
const uploadParams=(p,operation='executePreparedUpload')=>({resource:'upload',operation,operationHandle:p.operationHandle,stableItemId:'original-upload-item'});
for(const fault of ['none','create','put'])test(`upload retry ${fault}: exact original source/key/ID and ready GET-only`,async()=>{
 const b=new UploadBoundary(),s=uploadServer(b),p=await preparedUpload(b,s);if(fault!=='none'){s.lose(fault);await assert.rejects(()=>b.invoke(uploadParams(p),{upload:s.upload}),failure)}
 const previous=clone(events(b));b.mode='retry';b.events=[];prohibitHistory(b);const [[out]]=await b.invoke(uploadParams(p),{upload:s.upload,items:[binaryItem(Buffer.from('replacement-input'))]});
 assert.equal(out.json.operationId,p.operationId);assert.equal(out.json.ready,true);assert.equal(s.byKey.size,1);assert.equal(s.byId.size,1);
 if(fault!=='none'){const before=previous.find(e=>e.kind==='upload-api'&&e.request.path==='/v1/uploads'),after=events(b).find(e=>e.kind==='upload-api'&&e.request.path==='/v1/uploads');assert.deepEqual(before.request,after.request)}
 if(fault==='put'){assert.equal(events(b)[0].request.method,'GET');assert.equal(events(b)[0].request.path,'/v1/uploads/'+out.json.uploadId)}
 b.events=[];const [[read]]=await b.invoke(uploadParams(p,'readPreparedUpload'),{upload:s.upload});await b.invoke(uploadParams(p),{upload:s.upload});assert.equal(read.json.uploadId,out.json.uploadId);assert.equal(events(b).length,2);assert.ok(events(b).every(e=>e.kind==='upload-api'&&e.request.method==='GET'));
});

for(const change of ['key','workflow','ledger','item','handle','original-blob'])test(`upload retry ${change} cannot use replacement input to create`,async()=>{
 const b=new UploadBoundary(),s=uploadServer(b),p=await preparedUpload(b,s),input=uploadParams(p);b.mode='retry';b.events=[];
 if(change==='key')b.key=keyB;if(change==='workflow')b.workflowId='wrong-workflow';if(change==='ledger')b.ledgerRowId=2;if(change==='item')input.stableItemId='wrong-item';if(change==='handle')input.operationHandle=input.operationHandle.slice(0,-8)+'AAAAAAAA';if(change==='original-blob')b.blobs.clear();
 await assert.rejects(()=>b.invoke(input,{upload:s.upload,items:[binaryItem(BYTES)]}),failure);assert.equal(events(b).length,0);assert.equal(b.count('store-write'),0);assert.equal(s.byKey.size,0);
});
