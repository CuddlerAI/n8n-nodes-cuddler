'use strict';
// Built node + actual native factory, synthetic SDK/HTTP boundaries only.
const {test}=require('node:test');
const assert=require('node:assert/strict');
const {Readable}=require('node:stream');
const {Cuddler}=require('../dist/nodes/Cuddler/Cuddler.node.js');
const {createNativeHttpTransports}=require('../dist/nodes/Cuddler/recovery/NativeHttpTransport.js');
const {canonical}=require('../dist/nodes/Cuddler/recovery/ImmutablePlan.js');
const {SyntheticBoundary,keyA}=require('./file-entry-harness.cjs');
const {UploadBoundary,binaryItem}=require('./upload-entry-harness.cjs');
const HOST='assets.public-cdn.com';
const body=media=>({model:'synthetic-'+media,prompt:'private-integration-fixture',max_usd:'0.50'});
const future=()=>new Date(Date.now()+3600000).toISOString();
const full=job=>{const bytes=Buffer.from(JSON.stringify(job));return {statusCode:200,headers:{'content-type':'application/json','content-length':String(bytes.length)},body:Readable.from([bytes])}};
async function prepared(b,media='image',item='one',execute=true){
 const [[p]]=await b.invoke({resource:media,operation:'prepareMedia',businessBatchId:'final-integration-batch',stableItemId:item,requestBody:body(media)});
 const params={resource:media,stableItemId:item,operationHandle:p.json.operationHandle};
 const original=execute?(await b.invoke({...params,operation:'executePrepared'}))[0][0].json:undefined;
 return {params,original};
}
function context(b,parameters,options={}){const ctx=b.context(parameters,options);ctx.getExecutionCancelSignal=()=>undefined;return ctx}

for(const media of ['video','image','speech','music'])test(`typed trusted native factory executes protected original ${media} plan`,async()=>{
 const b=new SyntheticBoundary();const p=await prepared(b,media,'native-'+media,false);
 const calls=[];const ctx=context(b,{...p.params,operation:'executePrepared'});
 ctx.helpers.httpRequest=async request=>{
  calls.push(request);assert.equal(request.headers.Authorization,'Bearer '+keyA);
  assert.equal(request.body.toString(),canonical(body(media)));
  assert.match(request.headers['Idempotency-Key'],/^n8n-/);
  const id=({video:'vid_',image:'img_',speech:'aud_',music:'mus_'})[media]+'a'.repeat(media==='music'?20:24);
  return full({object:media,id,status:'queued'});
 };
 let factoryCalls=0;
 const node=new Cuddler({trustedTransportFactory:(c,i)=>{factoryCalls++;assert.equal(c,ctx);assert.equal(i,0);return createNativeHttpTransports(c,i)}});
 const [[result]]=await node.execute.call(ctx);
 assert.equal(result.json.state,'original-job');assert.equal(calls.length,1);assert.equal(factoryCalls,1);
 assert.equal(calls[0].url,'https://api.cuddler.ai'+({video:'/v1/videos',image:'/v1/images',speech:'/v1/speech',music:'/v1/music'})[media]);
 assert.ok(!JSON.stringify(result).includes('private-integration-fixture'));
});

test('per-item native factory asset grants remain isolated across reorder',async()=>{
 const b=new SyntheticBoundary();const a=await prepared(b,'image','first');const z=await prepared(b,'image','second');
 const order=[z,a],calls=[],instances=[],credentialIndices=[];
 const ctx=context(b,i=>({...order[i].params,operation:'downloadPreparedFile',outputBinaryProperty:'data',maxFileBytes:1024}),{items:[{json:{}},{json:{}}]});
 const get=ctx.getCredentials;ctx.getCredentials=async(name,index)=>{credentialIndices.push(index);return get(name,index)};
 ctx.helpers.httpRequest=async request=>{
  calls.push(request);
  if(request.url.startsWith('https://api.cuddler.ai/')){
   const id=request.url.split('/').pop();return full({object:'image',id,status:'succeeded',output:{url:`https://${HOST}/${id}.png`,url_expires_at:future(),retained_until:future(),content_type:'image/png'}});
  }
  assert.equal(request.headers.Authorization,'');assert.equal(request.headers.Cookie,'');
  const bytes=Buffer.from('synthetic-binary');return {statusCode:200,headers:{'content-type':'image/png','content-length':String(bytes.length)},body:Readable.from([bytes])};
 };
 const node=new Cuddler({trustedTransportFactory:(c,i)=>{const n=createNativeHttpTransports(c,i,{trustedAssetHosts:[HOST]});instances.push({index:i,n});return n}});
 const [out]=await node.execute.call(ctx);
 assert.deepEqual(instances.map(x=>x.index),[0,1]);assert.notEqual(instances[0].n.preparedRead.transport,instances[1].n.preparedRead.transport);
 assert.deepEqual(out.map(x=>x.json.jobId),[z.original.jobId,a.original.jobId]);assert.deepEqual(out.map(x=>x.pairedItem.item),[0,1]);
 assert.ok(credentialIndices.includes(0)&&credentialIndices.includes(1));assert.equal(calls.length,4);
 const cross={method:'GET',url:`https://${HOST}/${z.original.jobId}.png`,headers:{},credentials:'omit',followRedirects:false,maxBytes:1024};
 await assert.rejects(()=>instances[1].n.preparedRead.transport.readAsset(cross));assert.equal(calls.length,4);
});

test('explicit offline recovery override wins over a trusted native factory',async()=>{
 const b=new SyntheticBoundary();const p=await prepared(b,'image','override',false);const ctx=context(b,{...p.params,operation:'executePrepared'});
 let factoryCalls=0;ctx.helpers.httpRequest=()=>assert.fail('offline override must not use native HTTP');
 const node=new Cuddler({recoveryTransport:b.transport,trustedTransportFactory:(c,i)=>{factoryCalls++;return createNativeHttpTransports(c,i)}});
 const [[out]]=await node.execute.call(ctx);assert.equal(out.json.state,'original-job');assert.equal(factoryCalls,1);
});

test('explicit offline prepared-read override wins over native factory',async()=>{
 const b=new SyntheticBoundary();const p=await prepared(b,'image','read-override');const ctx=context(b,{...p.params,operation:'pollPrepared'});let reads=0;
 ctx.helpers.httpRequest=()=>assert.fail('read override must not use native HTTP');
 const preparedRead={transport:{getOriginal:async request=>{reads++;return {statusCode:200,job:{object:'image',id:request.jobId,status:'running'}}},readAsset:()=>assert.fail('poll must not read asset')}};
 const [[out]]=await new Cuddler({preparedRead,trustedTransportFactory:(c,i)=>createNativeHttpTransports(c,i)}).execute.call(ctx);
 assert.equal(out.json.status,'running');assert.equal(reads,1);
});

test('native media factory does not enable upload or reuse its different binding domain',async()=>{
 const b=new UploadBoundary();const [[p]]=await b.invoke({resource:'upload',operation:'prepareUpload',businessBatchId:'final-upload-batch',stableItemId:'one',uploadKind:'image',inputBinaryProperty:'data'},{items:[binaryItem(Buffer.from('synthetic'))]});
 const ctx=context(b,{resource:'upload',operation:'executePreparedUpload',operationHandle:p.json.operationHandle,stableItemId:'one'});
 ctx.helpers.httpRequest=()=>assert.fail('native media HTTP must never handle upload');
 await assert.rejects(()=>new Cuddler({trustedTransportFactory:(c,i)=>createNativeHttpTransports(c,i)}).execute.call(ctx),/UPLOAD_RECOVERY_FAILED/);
});

test('explicit offline upload override remains supported beside native media factory',async()=>{
 const b=new UploadBoundary();const [[p]]=await b.invoke({resource:'upload',operation:'prepareUpload',businessBatchId:'final-upload-batch',stableItemId:'two',uploadKind:'image',inputBinaryProperty:'data'},{items:[binaryItem(Buffer.from('synthetic'))]});
 const ctx=context(b,{resource:'upload',operation:'executePreparedUpload',operationHandle:p.json.operationHandle,stableItemId:'two'});let uploadCalls=0;
 ctx.helpers.httpRequest=()=>assert.fail('offline upload must not use native media HTTP');
 const upload={transport:{api:async request=>{uploadCalls++;assert.equal(request.path,'/v1/uploads');assert.match(request.idempotencyKey,/^n8n-upload-/);return {statusCode:201,upload:{object:'upload',id:'upl_aaaaaaaaaaaa',kind:'image',content_type:'image/png',bytes:9,status:'ready',expires_at:future()}}},put:()=>assert.fail('already ready must not PUT')}};
 const [[out]]=await new Cuddler({upload,trustedTransportFactory:(c,i)=>createNativeHttpTransports(c,i)}).execute.call(ctx);
 assert.equal(out.json.ready,true);assert.equal(out.json.uploadId,'upl_aaaaaaaaaaaa');assert.equal(uploadCalls,1);
});

test('workflow transport fields cannot replace the ordinary installed SDK boundary',async()=>{
 const b=new SyntheticBoundary();const p=await prepared(b,'image','default-off',false);
 const ctx=context(b,{...p.params,operation:'executePrepared',trustedTransportFactory:'native',enableNative:true});
 let calls=0;ctx.helpers.httpRequest=request=>{calls++;assert.equal(request.url,'https://api.cuddler.ai/v1/images');assert.equal(request.method,'POST');throw Error('synthetic SDK boundary unavailable');};
 await assert.rejects(()=>new Cuddler().execute.call(ctx),/Cuddler request failed/);
 assert.equal(calls,1);assert.equal(b.sideEffects().length,0);
});

test('trusted factory reference is captured at construction before mutable options change',async()=>{
 const b=new SyntheticBoundary();const p=await prepared(b,'image','capture',false);let first=0;
 const options={trustedTransportFactory:()=>{first++;return {recoveryTransport:b.transport}}};const node=new Cuddler(options);
 options.trustedTransportFactory=()=>assert.fail('mutated caller options selected');
 await node.execute.call(context(b,{...p.params,operation:'executePrepared'}));assert.equal(first,1);
});
