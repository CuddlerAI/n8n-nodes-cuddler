#!/usr/bin/env node
'use strict';
// Runs the built candidate at its node entry. Only SDK persistence, provider and
// HTTP boundaries are synthetic. Never load live credentials or contact an API.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { AsyncLocalStorage } = require('node:async_hooks');
const { execFileSync } = require('node:child_process');
const { pathToFileURL } = require('node:url');
const { SyntheticBoundary, keyA, keyB, sha, clone } = require('../test/file-entry-harness.cjs');
const { UploadBoundary, binaryItem } = require('../test/upload-entry-harness.cjs');
const { Boundary, event } = require('../test/callback-entry-harness.cjs');
const { Cuddler } = require('../dist/nodes/Cuddler/Cuddler.node.js');
const { ProtectedPlanStore } = require('../dist/nodes/Cuddler/recovery/ProtectedPlanStore.js');
const { canonical } = require('../dist/nodes/Cuddler/recovery/ImmutablePlan.js');
const als = new AsyncLocalStorage();
const future = () => new Date(Date.now()+3600000).toISOString();
const mime = {video:'video/mp4',image:'image/png',speech:'audio/mpeg',music:'audio/mpeg'};
const originals = {};
for (const method of ['saveOnce','read']) {
 originals[method]=ProtectedPlanStore.prototype[method];
 ProtectedPlanStore.prototype[method]=async function(slot,...args) {
  const ctx=als.getStore();
  let receipt;
  try { receipt=await originals[method].call(this,slot,...args); }
  catch(error) {
   if(ctx && method==='saveOnce' && ctx.def.persistFailed)ctx.emit({type:'checkpoint_failed',slot});
   if(ctx && method==='saveOnce' && ctx.def.readbackMismatch && error.message==='Persistent ledger readback mismatch') {
    const plan=JSON.parse(args[0]).payload;
    assert.equal(typeof ctx.lastReadback,'string');
    ctx.emit({type:'checkpoint',execution:ctx.execution,slot,key:plan.idempotencyKey,plan_sha256:sha(args[0]),request_sha256:sha(plan.requestJson),credential_binding:plan.credentialBinding,receipt:'failed-'+sha(args[0]),durable:false,readback_sha256:sha(ctx.lastReadback)});
   }
   throw error;
  }
  if(!ctx || ctx.def.upload)return receipt;
  if(!receipt) {
   if(method==='read' && ctx.def.readbackMismatch && ctx.saved.has(slot)) {
    const plan=JSON.parse(ctx.saved.get(slot)).payload;
    ctx.emit({type:'checkpoint',execution:ctx.execution,slot,key:plan.idempotencyKey,plan_sha256:sha(ctx.saved.get(slot)),request_sha256:sha(plan.requestJson),credential_binding:plan.credentialBinding,receipt:'receipt-'+sha(ctx.saved.get(slot)),durable:true,readback_sha256:sha('missing-readback')});
   }
   return receipt;
  }
  const plan=JSON.parse(receipt.entry.planJson);
  if(method==='saveOnce')ctx.saved.set(slot,receipt.entry.planJson);
  if(method==='read' && ctx.saved.has(slot)) {
   const p=plan.payload;
   const cp={type:'checkpoint',execution:ctx.execution,slot,key:p.idempotencyKey,plan_sha256:sha(ctx.saved.get(slot)),request_sha256:sha(p.requestJson),credential_binding:p.credentialBinding,receipt:'receipt-'+sha(ctx.saved.get(slot)),durable:true,readback_sha256:sha(receipt.entry.planJson)};
   ctx.checkpoints.set(p.idempotencyKey,cp);ctx.emit(cp);
  }
  if(ctx.alterPlan && method==='read') {
   const changed=clone(receipt);const p=JSON.parse(changed.entry.planJson);
   if(ctx.alterPlan==='version')p.payload.envelopeVersion='unsupported-v99';else p.payload.requestJson='{}';
   changed.entry.planJson=canonical(p);return changed;
  }
  return receipt;
 };
}
class Case {
 constructor(def,vectors) {this.def=def;this.vectors=vectors;this.events=[];this.saved=new Map();this.checkpoints=new Map();this.execution='execution-1';this.record=true;this.output=undefined;this.error=undefined;}
 emit(e){if(this.record)this.events.push(e)}
 async invoke(b,params,options={}) {return als.run(this,()=>b.invoke(params,{...options,executionId:this.execution}));}
 async reject(action,code){let error;try{await action()}catch(e){error=e}assert.ok(error,'candidate must reject');this.error=code;this.emit({type:'rejected',code});}
 result(outcome){const e=this.events;const jobs=new Set(e.filter(x=>['create','known_job'].includes(x.type)).map(x=>x.job_id));const led=e.filter(x=>x.type==='ledger');const held=led.reduce((n,x)=>n+(x.action==='hold'?x.amount_micros:-x.amount_micros),0);return {outcome,...(this.error?{code:this.error}:{}),...(jobs.size?{job_id:[...jobs][0]}:{}),logical_jobs:jobs.size,holds:led.filter(x=>x.action==='hold').length,captures:led.filter(x=>x.action==='capture').length,releases:led.filter(x=>x.action==='release').length,submits:e.filter(x=>x.type==='provider_submit').length,held_usd:(held/1e6).toFixed(6)};}
}
async function mediaCase(c) {
 const d=c.def,b=new SyntheticBoundary(),body=c.vectors.media[d.media].request;
 const originalContext=b.context.bind(b);
 b.context=(...args)=>{const ctx=originalContext(...args);const child=ctx.executeWorkflow;ctx.executeWorkflow=async(...a)=>{const result=await child(...a);if(a[0].code.nodes[1].parameters.operation==='get')c.lastReadback=result.data[0][0]?.json.payload;return result};return ctx};
 const server=new Map();let lose=false;
 b.transport={create:async r=>{
  const cp=c.checkpoints.get(r.idempotencyKey);assert.ok(cp);assert.equal(sha(r.bodyJson),cp.request_sha256);assert.equal(r.credentialBinding,cp.credential_binding);
  const id=({video:'vid_',image:'img_',speech:'aud_',music:'mus_'})[r.media]+sha(r.idempotencyKey).slice(0,r.media==='music'?20:24);
  c.emit({...cp,type:'create',execution:c.execution,job_id:id});
  if(!server.has(id)) {
   const status=d.cancel?'queued':d.id==='failed-generation'?'failed':'succeeded';server.set(id,{id,status});
   c.emit({type:'ledger',action:'hold',job_id:id,amount_micros:1000000});
   if(!d.cancel){c.emit({type:'provider_submit',job_id:id});c.emit({type:'ledger',action:status==='failed'?'release':'capture',job_id:id,amount_micros:1000000});}
  }
  if(lose){lose=false;throw Error('synthetic response lost after commit')}
  return clone(server.get(id));
 },get:async r=>{c.emit({type:'query',job_id:r.jobId});return clone(server.get(r.jobId))}};
 const prepare=async(item='one')=>{
  const [[p]]=await c.invoke(b,{resource:d.media,operation:'prepareMedia',requestBody:clone(body),businessBatchId:'fixture-batch',stableItemId:item});
  return {resource:d.media,operationHandle:p.json.operationHandle,stableItemId:item};
 };
 const execute=p=>c.invoke(b,{...p,operation:'executePrepared'});
 let p,id;
 if(d.persistFailed||d.readbackMismatch){b.fault=d.persistFailed?'write-error':'readback-mismatch';await c.reject(()=>prepare(),d.code);return c.result('rejected');}
 if(d.concurrent){const both=await Promise.all([prepare(),prepare()]);p=both[0];assert.notEqual(p.operationHandle,both[1].operationHandle);}
 else p=await prepare();
 if(d.known){c.record=false;const [[o]]=await execute(p);id=o.json.jobId;c.events=[];c.record=true;c.emit({type:'known_job',job_id:id});}
 if(['credential-changed','request-changed','budget-changed','plan-tampered','unsupported-version'].includes(d.id)){
  if(d.id==='credential-changed')b.key=keyB;
  if(d.id==='request-changed')p.requestBody={...body,model:'changed-model'};
  if(d.id==='budget-changed')p.requestBody={...body,max_usd:'2.00'};
  if(d.id==='plan-tampered')c.alterPlan='tampered';
  if(d.id==='unsupported-version')c.alterPlan='version';
  await c.reject(()=>execute(p),d.code);return c.result('rejected');
 }
 if(d.id==='image-cancel-unsupported'){
  await c.reject(()=>c.invoke(b,{...p,operation:'cancelPrepared'}),d.code);return c.result('rejected');
 }
 if(d.restart){
  lose=true;await assert.rejects(()=>execute(p));
  c.execution='execution-2';c.emit({type:'reopen',execution:c.execution});b.mode='retry';
  if(d.reorder){
   // Real node receives reversed items. The unrelated item has no operation and
   // is not dispatched; the retained original handle follows its stable ID.
   const slot=[...c.checkpoints.values()][0].slot;
   c.emit({type:'reordered',before:[slot,'unrelated'],after:['unrelated',slot]});
   const ctx=b.context(i=>i===1?{...p,operation:'executePrepared'}:{resource:'catalog',operation:'listModels'}, {items:[{json:{}},{json:{}}],executionId:c.execution});
   ctx.helpers.httpRequestWithAuthentication=async()=>({object:'list',data:[]});
   const [out]=await als.run(c,()=>new Cuddler({recoveryTransport:b.transport}).execute.call(ctx));
   assert.equal(out[1].pairedItem.item,1);id=out[1].json.jobId;assert.ok(id);
  }
  if(!id){const [[o]]=await execute(p);id=o.json.jobId;}
  await c.invoke(b,{...p,operation:'readPrepared'});return c.result('recovered');
 }
 if(d.id==='explicit-new'){
  for(const item of ['one','two']){const pp=item==='one'?p:await prepare(item);await execute(pp);await c.invoke(b,{...pp,operation:'readPrepared'});}
  return c.result('created');
 }
 if(d.id==='known-job'){await execute(p);await c.invoke(b,{...p,operation:'readPrepared'});return c.result('observed');}
 if(d.files||d.id==='file-unavailable'){
  let reads=0,assets=0;const bytes=Buffer.from('synthetic-output');
  const preparedRead={trustedAssetHosts:['assets.fixture.example'],transport:{
   getOriginal:async r=>{reads++;c.emit({type:'query',job_id:r.jobId});return {statusCode:200,job:{object:d.media,id:r.jobId,status:'succeeded',...(d.id==='file-unavailable'?{}:{output:{url:'https://assets.fixture.example/v'+reads,url_expires_at:future(),retained_until:future(),content_type:mime[d.media]}})}}},
   readAsset:async r=>{assets++;assert.equal(r.credentials,'omit');assert.deepEqual(r.headers,{});c.emit({type:'file',job_id:id,url_version:'v'+reads,credential_forwarded:false,delivered:assets>1});return assets===1?{statusCode:403,headers:{},body:Buffer.alloc(0)}:{statusCode:200,headers:{'content-type':mime[d.media],'content-length':String(bytes.length)},body:bytes};}
  }};
  if(d.id==='file-unavailable'){await c.reject(()=>c.invoke(b,{...p,operation:'downloadPreparedFile'},{preparedRead}),d.code);return c.result('unavailable');}
  const [[out]]=await c.invoke(b,{...p,operation:'downloadPreparedFile'},{preparedRead});assert.equal(out.json.bytes,bytes.length);assert.equal(out.json.sha256,sha(bytes));return c.result('delivered');
 }
 if(d.eventReplay){
  const cb=new Boundary();const e=event(d.media+'.succeeded');e.data.id=id;
  const incoming=await cb.trigger(e);const claimed=await cb.action('claimEvent',incoming.eventReceipt);
  const receipt='event-'+e.id;assert.equal(claimed[0].length,1);c.emit({type:'event',job_id:id,event_id:e.id,authenticated:true,durable:true,dedupe_receipt:receipt,disposition:'accepted'});
  await c.invoke(b,{...p,operation:'readPrepared'});await cb.action('completeEvent',claimed[0][0].json.deliveryHandle);
  c.emit({type:'event_output',job_id:id,event_id:e.id,dedupe_receipt:receipt});
  const again=await cb.action('claimEvent',(await cb.trigger(e)).eventReceipt,{executionId:'execution-2'});assert.equal(again[0].length,0);assert.equal(again[1][0].json.disposition,'duplicate');
  c.emit({type:'event',job_id:id,event_id:e.id,authenticated:true,durable:true,dedupe_receipt:receipt,disposition:'duplicate'});return c.result('observed');
 }
 const [[created]]=await execute(p);id=created.json.jobId;
 if(d.cancel){
  const preparedCancel={transport:{getOriginal:async r=>{c.emit({type:'query',job_id:r.jobId});return {statusCode:200,job:{object:d.media,...server.get(id)}}},cancelQueued:async r=>{
   assert.equal(r.jobId,id);const j=server.get(id);if(j.status!=='canceled'){j.status='canceled';c.emit({type:'ledger',action:'release',job_id:id,amount_micros:1000000});}return {statusCode:200,job:{object:d.media,...j}};
  }}};
  for(let i=0;i<2;i++) {
   const ctx=b.context({...p,operation:'cancelPrepared'},{executionId:c.execution});
   const [[out]]=await als.run(c,()=>new Cuddler({preparedCancel}).execute.call(ctx));
   assert.equal(out.json.status,'canceled');c.emit({type:'cancel',job_id:id,status:out.json.status});
  }
  return c.result('canceled');
 }
 await c.invoke(b,{...p,operation:'readPrepared'});return c.result('failed');
}
async function uploadCase(c) {
 const d=c.def,b=new UploadBoundary(),bytes=Buffer.from(c.vectors.upload.original_text);
 const contentType={image:'image/png',video:'video/mp4',audio:'audio/mpeg'}[d.kind];
 let current,subkey,lose=false,expired=false,changed=false;
 const observed=action=>c.emit({type:'upload',action,upload_id:current?.id??'upl_aaaaaaaaaaaa',key:subkey,content_sha256:sha(bytes),observed_content_sha256:sha(changed?Buffer.from(c.vectors.upload.changed_same_size_text):bytes),source_version:c.vectors.upload.source_version,bytes:bytes.length,credential_forwarded:false});
 const upload={trustedAssetHosts:['upload.fixture.example'],transport:{api:async r=>{
  if(r.path==='/v1/uploads') {
   subkey??=r.idempotencyKey;assert.equal(r.idempotencyKey,subkey);
   current??={object:'upload',id:'upl_aaaaaaaaaaaa',kind:d.kind,content_type:contentType,bytes:bytes.length,status:'pending',expires_at:future(),upload_url:'https://upload.fixture.example/original',upload_headers:{'content-type':contentType,'content-length':String(bytes.length)}};
   observed('reserve');
  } else if(r.method==='GET') {assert.equal(r.path,'/v1/uploads/'+current.id);observed('read');}
  else {assert.equal(r.path,'/v1/uploads/'+current.id+'/complete');current.status='ready';observed('complete');}
  return {statusCode:200,upload:{...current,...(expired?{status:'expired',expires_at:new Date(0).toISOString()}:{})}};
 },put:async r=>{
  assert.equal(r.credentials,'omit');assert.equal(r.headers.Authorization,undefined);assert.deepEqual(r.body,bytes);observed('put');
  if(lose){lose=false;throw Error('synthetic PUT response loss')}return {statusCode:200};
 }}};
 const [[p]]=await c.invoke(b,{resource:'upload',operation:'prepareUpload',businessBatchId:'fixture-upload',stableItemId:'one',uploadKind:d.kind,inputBinaryProperty:'data'},{items:[binaryItem(bytes,contentType)]});
 const params={resource:'upload',operationHandle:p.json.operationHandle,stableItemId:'one',operation:'executePreparedUpload'};
 if(d.id==='response-loss') {
  lose=true;await assert.rejects(()=>c.invoke(b,params,{upload}));
 } else {
  c.record=false;lose=d.id!=='ready-reuse';
  if(lose)await assert.rejects(()=>c.invoke(b,params,{upload}));else await c.invoke(b,params,{upload});
  c.record=true;c.events=[];
 }
 if(d.id==='same-size-swap') {changed=true;for(const k of b.blobs.keys())b.blobs.set(k,Buffer.from(c.vectors.upload.changed_same_size_text));}
 if(d.id==='expired')expired=true;
 if(d.code){await c.reject(()=>c.invoke(b,params,{upload}),d.code);observed('reject');return c.result('rejected');}
 const [[out]]=await c.invoke(b,params,{upload});assert.equal(out.json.uploadId,current.id);assert.equal(out.json.ready,true);observed('ready');return c.result('ready');
}
async function main(){
 assert.equal(process.argv.length,4,'usage: observe-cloud-recovery.cjs <frozen-contract-dir> <report.json>');
 const base=path.resolve(process.argv[2]);const output=path.resolve(process.argv[3]);
 const checker=await import(pathToFileURL(path.join(base,'scripts/platform-cloud-recovery-conformance.mjs')));
 const vectors=checker.loadVectors(),defs=checker.caseDefinitions(vectors);const cases=[],failures=[];
 for(const def of defs){
  const c=new Case(def,vectors);
  try{const result=await (def.upload?uploadCase(c):mediaCase(c));cases.push({case_id:def.case_id,events:c.events,result});}catch(e){failures.push({case_id:def.case_id,error:e.message});}
 }
 const report={contract:vectors.contract,evidence:'synthetic-only',fixture_sha256:checker.vectorDigest(),candidate:{channel:'n8n',commit:execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim()},cases};
 fs.mkdirSync(path.dirname(output),{recursive:true});fs.writeFileSync(output,JSON.stringify(report,null,2)+'\n');
 let validation;try{validation={passed:true,...checker.validateReport(report)}}catch(e){validation={passed:false,error:e.message}}
 const summary={cases:cases.length,executionFailures:failures,validation};
 fs.writeFileSync(output+'.summary.json',JSON.stringify(summary,null,2)+'\n');console.log(JSON.stringify(summary));
 if(failures.length || !validation.passed)process.exitCode=1;
}
main().catch(e=>{console.error(e.message);process.exitCode=1});
