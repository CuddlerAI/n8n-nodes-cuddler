'use strict';
// Actual built node and transport entry; SDK/HTTP fixtures are wholly synthetic.
const test=require('node:test');
const assert=require('node:assert/strict');
const {Readable}=require('node:stream');
const {createHmac}=require('node:crypto');
const {Cuddler}=require('../dist/nodes/Cuddler/Cuddler.node.js');
const {createNativeCuddlerHttpTransports}=require('../dist/nodes/Cuddler/recovery/NativeCuddlerHttpTransports.js');
const {SyntheticBoundary,keyA,keyB}=require('./file-entry-harness.cjs');
const MEDIA=['video','speech','music'];
const PATH={video:'/v1/videos',speech:'/v1/speech',music:'/v1/music'};
const body=()=>({model:'synthetic',prompt:'private-cancel-fixture',max_usd:'0.50'});
const binding=()=>createHmac('sha256',keyA).update('cuddler.n8n.hmac-json/1/credential-binding').digest('hex');
async function prepare(b,media='video',item='one',execute=true){
 const [[p]]=await b.invoke({resource:media,operation:'prepareMedia',businessBatchId:'cancel-batch',stableItemId:item,requestBody:body()});
 const params={resource:media,operationHandle:p.json.operationHandle,stableItemId:item};
 const job=execute?(await b.invoke({...params,operation:'executePrepared'}))[0][0].json:undefined;
 b.events.length=0;return {params,job};
}
function fixture(b,p,options={}){
 const state={status:options.status||'queued',events:[],cancel:0};
 const transport={getOriginal:async request=>{state.events.push({kind:'get',request});if(options.onGet)await options.onGet(request,state);return options.read||{statusCode:200,job:{object:p.params.resource,id:p.job.jobId,status:state.status}}},cancelQueued:async request=>{
  state.events.push({kind:'cancel',request});state.cancel++;if(options.onCancel)return options.onCancel(request,state);
  state.status='canceled';return {statusCode:200,job:{object:p.params.resource,id:p.job.jobId,status:'canceled',prompt:'must-not-return',api_key_id:'internal',usage:{usd:'must-not-return'}}};
 }};return {state,preparedCancel:{transport}};
}
async function invoke(b,params,options={},contextOptions={}){return new Cuddler(options).execute.call(b.context({...params,operation:'cancelPrepared'},contextOptions))}
const failure=/Cuddler cancel refused/;
for(const media of MEDIA){
 test(`${media}: original protected queued job GET precedes cancel and output is narrow`,async()=>{
  const b=new SyntheticBoundary(),p=await prepare(b,media),f=fixture(b,p);const [[out]]=await invoke(b,p.params,{preparedCancel:f.preparedCancel});
  assert.deepEqual(f.state.events.map(x=>x.kind),['get','cancel']);assert.equal(out.json.jobId,p.job.jobId);assert.equal(out.json.status,'canceled');assert.equal(out.json.cancelRequested,true);assert.equal(out.pairedItem.item,0);
  const r=f.state.events[1].request;assert.equal(r.method,'POST');assert.equal(r.path,PATH[media]+'/'+p.job.jobId+'/cancel');assert.equal(r.jobId,p.job.jobId);assert.equal(r.credentialBinding,binding());assert.ok(Object.isFrozen(r));
  assert.deepEqual(Object.keys(r).sort(),['origin','method','path','media','jobId','credentialBinding'].sort());
  for(const word of [keyA,'private-cancel-fixture','must-not-return','internal','refund','hold_usd'])assert.equal(JSON.stringify(out).includes(word),false);
  assert.equal(b.count('create'),0);assert.equal(b.count('store-write'),0);
 });
 for(const status of ['running','succeeded','failed'])test(`${media}: ${status} original refuses without POST`,async()=>{
  const b=new SyntheticBoundary(),p=await prepare(b,media),f=fixture(b,p,{status});await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),/CANCEL_ONLY_QUEUED/);assert.equal(f.state.cancel,0);assert.equal(f.state.events.length,1);
 });
 test(`${media}: already canceled original is GET-only, not another cancel`,async()=>{
  const b=new SyntheticBoundary(),p=await prepare(b,media),f=fixture(b,p,{status:'canceled'});const [[out]]=await invoke(b,p.params,{preparedCancel:f.preparedCancel});assert.equal(out.json.cancelRequested,false);assert.equal(f.state.cancel,0);assert.equal(f.state.events.length,1);
 });
}
test('image cancel is unsupported before credentials or any external capability',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b,'image'),f=fixture(b,p);await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),/CANCEL_UNSUPPORTED_MEDIA/);assert.equal(b.count('credentials'),0);assert.equal(f.state.events.length,0);
 const d=new Cuddler().description.properties.filter(x=>x.name==='operation'&&x.displayOptions?.show?.resource?.includes('image'))[0];assert.ok(!d.options.some(x=>x.value==='cancelPrepared'));
});
test('prepared operation without persisted original job never GETs, cancels or creates',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b,'video','unsubmitted',false),f=fixture(b,p);await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),/CANCEL_RECOVERY_FAILED/);assert.equal(f.state.events.length,0);assert.equal(b.count('create'),0);
});
for(const which of ['key','table','row','workflow','body','item','handle'])test(`${which} tamper refuses before original GET`,async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p),params={...p.params};
 if(which==='key')b.key=keyB;if(which==='table')b.ledgerTableId='DifferentTable';if(which==='row')b.ledgerRowId=2;if(which==='workflow')b.workflowId='DifferentWorkflow';if(which==='body')params.requestBody={...body(),max_usd:'0.60'};if(which==='item')params.stableItemId='other';if(which==='handle'){const at=Math.floor(params.operationHandle.length/2);params.operationHandle=params.operationHandle.slice(0,at)+(params.operationHandle[at]==='A'?'B':'A')+params.operationHandle.slice(at+1)};
 await assert.rejects(()=>invoke(b,params,{preparedCancel:f.preparedCancel}),failure);assert.equal(f.state.events.length,0);assert.equal(b.count('create'),0);
});
for(const change of ['key','table','row','workflow'])test(`protected ${change} change after GET refuses before cancel`,async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p,{onGet:()=>{if(change==='key')b.key=keyB;if(change==='table')b.ledgerTableId='Other';if(change==='row')b.ledgerRowId=2;if(change==='workflow')b.workflowId='OtherWorkflow'}});
 await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),failure);assert.equal(f.state.cancel,0);
});
for(const status of [401,403,404,429,503])test(`original GET HTTP${status} cannot cancel`,async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p,{read:{statusCode:status,job:{secret:'untrusted'}}});await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),/CANCEL_ORIGINAL_GET_FAILED/);assert.equal(f.state.cancel,0);
});
for(const changed of [{object:'music'},{id:'vid_wrongaaaaaaaaaaaa'},{status:{secret:'bad'}}])test('invalid original GET fields cannot cancel',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p,{read:{statusCode:200,job:{object:'video',id:p.job.jobId,status:'queued',...changed}}});await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),/CANCEL_INVALID_ORIGINAL_RESPONSE/);assert.equal(f.state.cancel,0);
});
for(const statusCode of [409,401,503])test(`cancel HTTP${statusCode} is not retried or a replacement job`,async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p,{onCancel:()=>({statusCode})});await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),statusCode===409?/CANCEL_NOT_CANCELABLE/:/CANCEL_REQUEST_FAILED/);assert.equal(f.state.cancel,1);assert.equal(b.count('create'),0);
});
test('HTTP200 without canceled status cannot claim success or settlement',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p,{onCancel:()=>({statusCode:200,job:{object:'video',id:p.job.jobId,status:'running'}})});await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),/CANCEL_UNCONFIRMED/);
});
test('lost cancel response resumes original GET-only canceled job',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p,{onCancel:(_r,s)=>{s.status='canceled';throw new Error(keyA+' lost provider response')}});
 await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),/CANCEL_RECOVERY_FAILED/);const [[out]]=await invoke(b,p.params,{preparedCancel:f.preparedCancel});assert.equal(out.json.jobId,p.job.jobId);assert.equal(out.json.cancelRequested,false);assert.equal(f.state.cancel,1);assert.deepEqual(f.state.events.map(x=>x.kind),['get','cancel','get']);
});
test('captured original handle/body/item survive later parameter mutation',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p),params={...p.params,operation:'cancelPrepared'};let mutated=false;
 const ctx=b.context(params,{onCredentials:()=>{if(!mutated){mutated=true;params.operationHandle+='changed';params.stableItemId='changed';params.requestBody={other:true}}}});
 const [[out]]=await new Cuddler({preparedCancel:f.preparedCancel}).execute.call(ctx);assert.equal(out.json.jobId,p.job.jobId);assert.equal(f.state.cancel,1);
});
test('retry mode refuses before credentials; workflow fields cannot bypass queued-only original cancel',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p);b.mode='retry';await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),/AUTOMATIC_RETRY_UNSUPPORTED/);assert.equal(b.count('credentials'),0);b.mode='manual';
 const ctx=b.context({...p.params,operation:'cancelPrepared',enableNative:true,preparedCancel:'native'});ctx.getExecutionCancelSignal=()=>undefined;let calls=0;
 ctx.helpers.httpRequest=async request=>{calls++;assert.equal(request.method,'GET');const bytes=Buffer.from(JSON.stringify({object:'video',id:p.job.jobId,status:'running'}));return {statusCode:200,headers:{'content-type':'application/json'},body:Readable.from([bytes])};};
 await assert.rejects(()=>new Cuddler().execute.call(ctx),/CANCEL_/);assert.equal(calls,1);assert.equal(f.state.cancel,0);
});
test('error projections contain only fixed refusal code',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p,{onGet:()=>{throw new Error(keyA+' secret prompt https://private.invalid')}});const [[out]]=await invoke(b,p.params,{preparedCancel:f.preparedCancel},{continueOnFail:true});assert.match(out.json.error,/CANCEL_RECOVERY_FAILED/);for(const value of [keyA,'secret prompt','private.invalid'])assert.equal(out.json.error.includes(value),false);
});
for(const media of MEDIA)test(`native composite ${media} cancel is original GET then bodyless POST with captured Bearer`,async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b,media),calls=[];const ctx=b.context({...p.params,operation:'cancelPrepared'});ctx.getExecutionCancelSignal=()=>undefined;
 ctx.helpers.httpRequest=async r=>{calls.push(r);assert.equal(r.headers.Authorization,'Bearer '+keyA);assert.equal(r.headers.Cookie,'');assert.equal(r.disableFollowRedirect,true);assert.equal(r.body,undefined);assert.equal(r.headers['Idempotency-Key'],undefined);assert.equal(Object.isFrozen(r.headers),false);const bytes=Buffer.from(JSON.stringify({object:media,id:p.job.jobId,status:r.method==='GET'?'queued':'canceled'}));return {statusCode:200,headers:{'content-type':'application/json','content-length':String(bytes.length)},body:Readable.from([bytes])}};
 const [[out]]=await new Cuddler({trustedTransportFactory:(c,i)=>createNativeCuddlerHttpTransports(c,i)}).execute.call(ctx);assert.equal(out.json.status,'canceled');assert.deepEqual(calls.map(x=>x.method),['GET','POST']);assert.equal(calls[1].url,calls[0].url+'/cancel');
});
test('native cancel cannot POST without its own original queued GET grant',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),ctx=b.context({});ctx.getExecutionCancelSignal=()=>undefined;let calls=0;ctx.helpers.httpRequest=()=>{calls++;assert.fail('no grant')};
 const n=createNativeCuddlerHttpTransports(ctx,0);await assert.rejects(()=>n.preparedCancel.transport.cancelQueued({origin:'https://api.cuddler.ai',method:'POST',path:PATH.video+'/'+p.job.jobId+'/cancel',media:'video',jobId:p.job.jobId,credentialBinding:binding()}));assert.equal(calls,0);
});


test('reordered original handles cancel their own jobs and retain current pairedItem',async()=>{
 const b=new SyntheticBoundary(),a=await prepare(b,'video','a'),z=await prepare(b,'video','b'),order=[z,a],calls=[];
 const transport={getOriginal:async r=>({statusCode:200,job:{object:'video',id:r.jobId,status:'queued'}}),cancelQueued:async r=>{calls.push(r.jobId);return {statusCode:200,job:{object:'video',id:r.jobId,status:'canceled'}}}};
 const ctx=b.context(i=>({...order[i].params,operation:'cancelPrepared'}),{items:[{json:{}},{json:{}}]});const [out]=await new Cuddler({preparedCancel:{transport}}).execute.call(ctx);
 assert.deepEqual(calls,[z.job.jobId,a.job.jobId]);assert.deepEqual(out.map(x=>x.json.jobId),calls);assert.deepEqual(out.map(x=>x.pairedItem.item),[0,1]);assert.equal(b.count('create'),0);
});

test('key change after cancel response refuses a success projection',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),f=fixture(b,p,{onCancel:()=>{b.key=keyB;return {statusCode:200,job:{object:'video',id:p.job.jobId,status:'canceled'}}}});await assert.rejects(()=>invoke(b,p.params,{preparedCancel:f.preparedCancel}),/CANCEL_RECOVERY_FAILED/);assert.equal(f.state.cancel,1);
});

test('native per-item queued grants cannot cross instances or survive credential rotation',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),ctx=b.context({});ctx.getExecutionCancelSignal=()=>undefined;let calls=0;
 ctx.helpers.httpRequest=async()=>{calls++;const bytes=Buffer.from(JSON.stringify({object:'video',id:p.job.jobId,status:'queued'}));return {statusCode:200,headers:{'content-type':'application/json'},body:Readable.from([bytes])}};
 const a=createNativeCuddlerHttpTransports(ctx,0),z=createNativeCuddlerHttpTransports(ctx,1);const read={origin:'https://api.cuddler.ai',method:'GET',path:PATH.video+'/'+p.job.jobId,media:'video',jobId:p.job.jobId,credentialBinding:binding()},cancel={...read,method:'POST',path:read.path+'/cancel'};
 await a.preparedCancel.transport.getOriginal(read);await assert.rejects(()=>z.preparedCancel.transport.cancelQueued(cancel));assert.equal(calls,1);b.key=keyB;await assert.rejects(()=>a.preparedCancel.transport.cancelQueued(cancel));assert.equal(calls,1);
});

test('native cancel request never accepts body, query or image endpoint',async()=>{
 const b=new SyntheticBoundary(),p=await prepare(b),ctx=b.context({});ctx.getExecutionCancelSignal=()=>undefined;let calls=0;ctx.helpers.httpRequest=()=>{calls++;assert.fail('invalid cancel must not dispatch')};const n=createNativeCuddlerHttpTransports(ctx,0);
 const base={origin:'https://api.cuddler.ai',method:'POST',path:PATH.video+'/'+p.job.jobId+'/cancel',media:'video',jobId:p.job.jobId,credentialBinding:binding()};
 for(const altered of [{...base,body:{}},{...base,path:base.path+'?retry=1'},{...base,media:'image',jobId:'img_aaaaaaaaaaaa',path:'/v1/images/img_aaaaaaaaaaaa/cancel'}])await assert.rejects(()=>n.preparedCancel.transport.cancelQueued(altered));assert.equal(calls,0);
});
