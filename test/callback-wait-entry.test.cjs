'use strict';
// Actual compiled node entries + real encrypted/CAS algorithms. The SDK child
// and HTTP boundaries are synthetic; this is not a native Wait/SQL/host result.
const test = require('node:test');
const assert = require('node:assert/strict');
const { createHash, createHmac } = require('node:crypto');
const { Readable } = require('node:stream');
const { Cuddler } = require('../dist/nodes/Cuddler/Cuddler.node');
const { CuddlerTaskCallbackWait } = require('../dist/nodes/CuddlerTaskCallback/CuddlerTaskCallbackWait.node');
const { CuddlerTaskCallbackTrigger } = require('../dist/nodes/CuddlerTaskCallback/CuddlerTaskCallbackTrigger.node');
const { CuddlerTaskCallbackReceipt } = require('../dist/nodes/CuddlerTaskCallback/CuddlerTaskCallbackReceipt.node');
const { INBOX_FORMAT } = require('../dist/nodes/CuddlerTaskCallback/TaskCallbackInbox');
const clone = value => structuredClone(value);
const hash = value => createHash('sha256').update(value).digest('hex');
const KEY = 'synthetic-only-original-organization-key';
const SECRET = 'whsec_'+'s'.repeat(43);
const TOKEN = 'd'.repeat(64);
const WAIT = {name:'Original Single Wait',type:'n8n-nodes-base.wait',typeVersion:1.1,disabled:false,parameters:{resume:'webhook',httpMethod:'POST',responseMode:'responseNode',responseCode:503,limitWaitTime:true,limitType:'afterTimeInterval',resumeUnit:'seconds',resumeAmount:90,authentication:'none',options:{}}};

class Boundary {
  constructor(media='video') {
    this.media=media;this.workflowId='CallbackWaitFixture';this.executionId='100';this.nodeId='wait-authority';this.mode='manual';
    this.callback={apiKey:KEY,signingSecret:SECRET,ledgerTableId:'CallbackLedger',ledgerRowId:'2',waitTableId:'CallbackLedger',waitRowId:'3',callbackUrl:'http://127.0.0.1:18543/webhook/callback-fixture'};
    this.mediaCredential={apiKey:KEY,ledgerTableId:'CallbackLedger',ledgerRowId:'1'};
    this.rows=new Map([1,2,3].map(id=>[id,{id,etag:'0'.repeat(64),payload:JSON.stringify({format:id===2?INBOX_FORMAT:'cuddler.n8n.cas-ledger/1',slots:{}})}]));
    this.children=[];this.outputs=[];this.credentialReads=0;this.http=[];this.creates=[];this.childHook=undefined;this.credentialHook=undefined;
    this.parents=[];this.childrenMetadata=[clone(WAIT)];this.base='http://127.0.0.1:18543/';this.resume='http://127.0.0.1:18543/webhook-waiting/100?signature='+TOKEN;
    this.job={id:({video:'vid_',image:'img_',speech:'aud_',music:'mus_'})[media]+'a'.repeat(media==='music'?20:24),status:'queued'};
    this.httpHook=async()=>({statusCode:200,body:{}});this.waitOptions={};this.originalReads=[];this.waitState={status:'waiting',claims:0};
    this.transport={create:async req=>{this.creates.push(clone(req));return clone(this.job);},get:async req=>({id:req.jobId,status:'succeeded'})};
  }
  async creds(name) {this.credentialReads++;if(this.credentialHook)await this.credentialHook(name,this);return clone(name==='cuddlerApi'?this.mediaCredential:this.callback);}
  async child(input,_data,_unused,options) {
    const graph=input.code; assert.equal(graph.id,this.workflowId);assert.equal(graph.nodes.length,2);assert.equal(graph.nodes[1].type,'n8n-nodes-base.dataTable');
    assert.equal(options.parentExecution.workflowId,this.workflowId);
    const p=graph.nodes[1].parameters;assert.equal(p.dataTableId.value,'CallbackLedger');assert.equal(p.matchType,'allConditions');
    this.children.push(clone(graph));if(this.childHook)await this.childHook(p,this);
    const id=p.filters.conditions[0].keyValue;const current=this.rows.get(id);let before=current&&clone(current);
    if(p.operation==='update'&&current?.etag===p.filters.conditions[1].keyValue)this.rows.set(id,{...current,...clone(p.columns.value)});
    return {executionId:String(1000+this.children.length),data:[before?[{json:p.operation==='get'?clone(this.rows.get(id)):before}]:[]]};
  }
  context(parameters,options={}) {
    const b=this;const node={id:b.nodeId,name:'Callback Wait',type:'n8n-nodes-cuddler.cuddlerTaskCallbackWait',typeVersion:1,parameters:{},...options.node};
    return {
      getInputData:()=>options.items||[{json:{}}],getNodeParameter:(name,_i,fallback)=>Object.hasOwn(parameters,name)?parameters[name]:fallback,
      getNode:()=>node,getWorkflow:()=>({id:b.workflowId}),getWorkflowSettings:()=>options.settings||{},getExecutionId:()=>b.executionId,getMode:()=>b.mode,
      getCredentials:name=>b.creds(name),getExecutionCancelSignal:()=>undefined,continueOnFail:()=>false,executeWorkflow:(...args)=>b.child(...args),
      getParentNodes:()=>clone(b.parents),getChildNodes:()=>clone(b.childrenMetadata),getInstanceBaseUrl:()=>b.base,evaluateExpression:()=>b.resume,
      helpers:{httpRequest:async options=>{b.http.push(clone(options));return await b.httpHook(options);},httpRequestWithAuthentication:async()=>{throw Error('authenticated HTTP forbidden');}},
    };
  }
  async invoke(operation,values={},options={}) {
    const result=await new CuddlerTaskCallbackWait(this.waitOptions).execute.call(this.context({operation,...values},options));this.outputs.push(clone(result));return result[0][0].json;
  }
  async original(options={}) {
    const setup=await this.invoke('prepareCallbackScope');
    const body={prompt:'PRIVATE_PAID_PROMPT',model:'fixture',webhook_url:setup.callbackUrl,metadata:{cuddler_callback_scope:setup.callbackScope},...options.body};
    const common={resource:this.media,requestBody:body,businessBatchId:'durable-original-batch',stableItemId:'stable-original-item'};
    const ctx=this.context({...common,operation:'prepareMedia'},{node:{id:'original-media-node',type:'n8n-nodes-cuddler.cuddler'}});
    const mediaNode=new Cuddler({recoveryTransport:this.transport});
    const prepared=(await mediaNode.execute.call(ctx))[0][0].json;this.prepared=prepared;
    if(options.unknown)return prepared;
    const executed=(await mediaNode.execute.call(this.context({resource:this.media,operation:'executePrepared',operationHandle:prepared.operationHandle,requestBody:'',stableItemId:'stable-original-item'})))[0][0].json;
    this.originalOutput=executed;return executed;
  }
  async claim(options={}) {
    const raw={object:'event',id:options.eventId||'evt_'+'e'.repeat(20),type:this.media+'.succeeded',created_at:new Date().toISOString(),data:{object:this.media,id:options.jobId||this.job.id,status:'succeeded',prompt:'PRIVATE_EVENT_PROMPT',output:{url:'https://example.invalid/PRIVATE_ASSET'},metadata:{cuddler_callback_scope:(await this.invoke('prepareCallbackScope')).callbackScope,private:'PRIVATE_META'}}};
    const body=Buffer.from(JSON.stringify(raw));const time=Math.floor(Date.now()/1000);const signature='t='+time+',v1='+createHmac('sha256',SECRET).update(time+'.').update(body).digest('hex');
    const req={method:'POST',rawBody:body,headers:{'content-type':'application/json','cuddler-signature':signature,'cuddler-event-id':raw.id}};
    const triggerCtx={getRequestObject:()=>req,getHeaderData:()=>req.headers,getMode:()=> 'webhook',getWorkflowSettings:()=>({}),getWebhookResourceUrl:()=>this.callback.callbackUrl,getNodeWebhookUrl:()=>this.callback.callbackUrl,getResponseObject:()=>({status:()=>({json:()=>{}})}),getCredentials:()=>this.creds('cuddlerTaskCallbackApi'),getWorkflow:()=>({id:this.workflowId}),getNode:()=>({id:'original-trigger',name:'Stable Callback',type:'n8n-nodes-cuddler.cuddlerTaskCallbackTrigger',parameters:{}})};
    const ingress=await CuddlerTaskCallbackTrigger.prototype.webhook.call(triggerCtx);assert.ok(ingress.workflowData);
    const receipt=ingress.workflowData[0][0].json.eventReceipt;
    const ctx=this.context({operation:'claimEvent',eventReceipt:receipt},{node:{id:'receipt',type:'n8n-nodes-cuddler.cuddlerTaskCallbackReceipt'}});
    const result=await CuddlerTaskCallbackReceipt.prototype.execute.call(ctx);this.outputs.push(clone(result));return result;
  }
  async ready(){const original=await this.original();const registered=await this.invoke('registerOriginalWait',{operationHandle:original.operationHandle});this.executionId='200';this.mode='webhook';return {original,registered};}
  async runComplete(body,options={}) {const prior={execution:this.executionId,parents:this.parents,children:this.childrenMetadata};try{this.executionId=options.executionId||'100';this.parents=[clone(WAIT)];this.childrenMetadata=[];return await this.invoke('completeOriginalWait',{resumeProof:body});}finally{this.executionId=prior.execution;this.parents=prior.parents;this.childrenMetadata=prior.children;}}
  privateBoundaries(){return JSON.stringify({rows:[...this.rows],children:this.children,outputs:this.outputs});}
}
async function rejected(promise) {await assert.rejects(promise,e=>{assert.equal(e.message,'CUDDLER_CALLBACK_WAIT_REJECTED');for(const value of [KEY,SECRET,TOKEN,'PRIVATE_PAID_PROMPT','PRIVATE_EVENT_PROMPT','PRIVATE_ASSET'])assert.ok(!JSON.stringify(e).includes(value));return true;});}

for(const media of ['video','image','speech','music'])test('actual entries bind original '+media+' and durable complete precedes200',async()=>{
  const b=new Boundary(media);const {original}=await b.ready();const claim=await b.claim();const handle=claim[0][0].json.deliveryHandle;
  b.httpHook=async options=>{const complete=await b.runComplete(options.body);assert.equal(complete.jobId,original.jobId);return {statusCode:200,body:complete};};
  const result=await b.invoke('deliverOriginalWait',{deliveryHandle:handle});assert.equal(result.statusCode,200);assert.equal(b.http.length,1);
  assert.equal(b.http[0].url,b.resume);assert.equal(b.http[0].disableFollowRedirect,true);assert.equal(b.http[0].timeout,10000);assert.equal(b.http[0].headers.Authorization,undefined);assert.equal(b.http[0].headers.Cookie,undefined);
  assert.equal((await b.invoke('deliverOriginalWait',{deliveryHandle:handle})).statusCode,200);assert.equal(b.http.length,1);assert.equal(b.creates.length,1);
  for(const value of [KEY,SECRET,TOKEN,'PRIVATE_PAID_PROMPT','PRIVATE_EVENT_PROMPT','PRIVATE_ASSET','PRIVATE_META'])assert.ok(!b.privateBoundaries().includes(value),'sensitive bytes escaped encrypted child/output');
});

test('original lost-job response path refuses registration until known original ID',async()=>{const b=new Boundary();const p=await b.original({unknown:true});await rejected(b.invoke('registerOriginalWait',{operationHandle:p.operationHandle}));assert.equal(b.creates.length,0);assert.equal(b.http.length,0);});
test('early event remains pending503 without original map or guessed POST',async()=>{const b=new Boundary();await b.original();b.executionId='200';const claim=await b.claim();const result=await b.invoke('deliverOriginalWait',{deliveryHandle:claim[0][0].json.deliveryHandle});assert.equal(result.statusCode,503);assert.equal(result.disposition,'waiting-for-original-job');assert.equal(b.http.length,0);assert.equal(b.creates.length,1);});
for(const response of [200,202,409,410,500])test('HTTP '+response+' is not durable event completion',async()=>{const b=new Boundary();await b.ready();const claim=await b.claim();b.httpHook=async()=>({statusCode:response,body:{accepted:true}});assert.equal((await b.invoke('deliverOriginalWait',{deliveryHandle:claim[0][0].json.deliveryHandle})).statusCode,503);});
test('lost resume response after actual durable completion is200 on readback',async()=>{const b=new Boundary();await b.ready();const claim=await b.claim();b.httpHook=async options=>{await b.runComplete(options.body);throw Error('PRIVATE cause '+TOKEN);};assert.equal((await b.invoke('deliverOriginalWait',{deliveryHandle:claim[0][0].json.deliveryHandle})).statusCode,200);});
test('lost response before completion is pending and hides resumeURL',async()=>{const b=new Boundary();await b.ready();const claim=await b.claim();b.httpHook=async()=>{throw Error('PRIVATE cause '+TOKEN);};assert.equal((await b.invoke('deliverOriginalWait',{deliveryHandle:claim[0][0].json.deliveryHandle})).statusCode,503);});
test('cross-execution delivery and direct wrong-execution completion reject',async()=>{const b=new Boundary();await b.ready();const claim=await b.claim();const handle=claim[0][0].json.deliveryHandle;b.executionId='201';await rejected(b.invoke('deliverOriginalWait',{deliveryHandle:handle}));b.executionId='200';b.httpHook=async options=>{await rejected(b.runComplete(options.body,{executionId:'101'}));return {statusCode:409};};assert.equal((await b.invoke('deliverOriginalWait',{deliveryHandle:handle})).statusCode,503);});
for(const field of ['waitRowId','ledgerRowId','callbackUrl','signingSecret','apiKey'])test('frozen '+field+' cannot map original paid job to a new destination',async()=>{const b=new Boundary();const o=await b.original();b.callback[field]=({waitRowId:'4',ledgerRowId:'4',callbackUrl:'http://127.0.0.1:18543/webhook/other',signingSecret:'whsec_'+'t'.repeat(43),apiKey:'other-key'})[field];await rejected(b.invoke('registerOriginalWait',{operationHandle:o.operationHandle}));assert.equal(b.http.length,0);});
for(const body of [{webhook_url:'https://other.invalid/callback'},{metadata:{}},{metadata:{cuddler_callback_scope:'a'.repeat(64)}}])test('paid request must already contain exact stable callback and scope '+JSON.stringify(body),async()=>{const b=new Boundary();const o=await b.original({body});await rejected(b.invoke('registerOriginalWait',{operationHandle:o.operationHandle}));});
for(const resume of ['https://other.invalid/webhook-waiting/100?signature='+TOKEN,'http://127.0.0.1:18543/webhook-waiting/101?signature='+TOKEN,'http://127.0.0.1:18543/webhook-waiting/100?signature='+TOKEN+'&other=x','http://u:p@127.0.0.1:18543/webhook-waiting/100?signature='+TOKEN])test('resume target refuses '+resume.split('?')[0],async()=>{const b=new Boundary();const o=await b.original();b.resume=resume;await rejected(b.invoke('registerOriginalWait',{operationHandle:o.operationHandle}));assert.equal(b.http.length,0);});
test('new execution consumes original job without replacing immutable first waiter',async()=>{const b=new Boundary();const {original}=await b.ready();const snapshot=clone(b.rows.get(3));b.executionId='101';b.resume=b.resume.replace('/100?','/101?');b.childrenMetadata=[];const recovered=await b.invoke('registerOriginalWait',{operationHandle:original.operationHandle});assert.equal(recovered.registered,false);assert.equal(recovered.jobId,original.jobId);assert.equal(recovered.originalWaitExecutionId,'100');assert.equal(recovered.recovery,'read-original-job-with-original-handle');assert.deepEqual(b.rows.get(3),snapshot);assert.equal(b.creates.length,1);assert.equal(b.http.length,0);});
for(const alter of [b=>{b.childrenMetadata.push({...clone(WAIT),name:'second'});},b=>{b.childrenMetadata[0].parameters.responseMode='onReceived';},b=>{b.childrenMetadata[0].parameters.limitWaitTime=false;},b=>{b.childrenMetadata[0].parameters.resumeAmount=301;},b=>{b.childrenMetadata[0].parameters.options.webhookSuffix='second';}])test('single bounded response-node Wait topology required '+String(alter),async()=>{const b=new Boundary();const o=await b.original();alter(b);await rejected(b.invoke('registerOriginalWait',{operationHandle:o.operationHandle}));});
test('precredential policy rejects tool, multiple items, enginev2 and continue error',async()=>{for(const options of [{node:{type:'n8n-nodes-cuddler.cuddlerTaskCallbackWaitTool'}},{items:[{json:{}},{json:{}}]},{settings:{engineType:'v2'}},{node:{retryOnFail:true}},{node:{onError:'continueRegularOutput'}}]){const b=new Boundary();await rejected(b.invoke('prepareCallbackScope',{},options));assert.equal(b.credentialReads,0);assert.equal(b.children.length,0);}});
test('changed credential during helper await cannot acknowledge',async()=>{const b=new Boundary();await b.ready();const claim=await b.claim();b.httpHook=async()=>{b.callback.signingSecret='whsec_'+'x'.repeat(43);return {statusCode:200};};await rejected(b.invoke('deliverOriginalWait',{deliveryHandle:claim[0][0].json.deliveryHandle}));});
test('tampered callbackWait handle and delivery proof reject without extra HTTP',async()=>{const b=new Boundary();await b.ready();const claim=await b.claim();b.httpHook=async options=>{const changed=clone(options.body);changed.callbackWaitHandle=changed.callbackWaitHandle.slice(0,-4)+'AAAA';await rejected(b.runComplete(changed));return {statusCode:500};};assert.equal((await b.invoke('deliverOriginalWait',{deliveryHandle:claim[0][0].json.deliveryHandle})).statusCode,503);assert.equal(b.http.length,1);});
test('different signed job cannot use existing original mapping',async()=>{const b=new Boundary();await b.ready();const claim=await b.claim({jobId:'vid_'+'b'.repeat(24)});assert.equal((await b.invoke('deliverOriginalWait',{deliveryHandle:claim[0][0].json.deliveryHandle})).statusCode,503);assert.equal(b.http.length,0);});


test('lost create response recovers same original body/key in new execution before first Wait',async()=>{
  const b=new Boundary();const create=b.transport.create;let lost=true;b.transport.create=async request=>{const result=await create(request);if(lost){lost=false;throw Error('synthetic lost original create response');}return result;};
  await assert.rejects(b.original());const handle=b.prepared.operationHandle;b.executionId='101';b.resume=b.resume.replace('/100?','/101?');
  const mediaNode=new Cuddler({recoveryTransport:b.transport});const original=(await mediaNode.execute.call(b.context({resource:b.media,operation:'executePrepared',operationHandle:handle,requestBody:'',stableItemId:'stable-original-item'})))[0][0].json;
  assert.equal(b.creates.length,2);assert.deepEqual(b.creates[0],b.creates[1]);
  const mapped=await b.invoke('registerOriginalWait',{operationHandle:original.operationHandle});assert.equal(mapped.registered,true);assert.equal(mapped.originalWaitExecutionId,'101');assert.equal(mapped.jobId,b.job.id);
  assert.equal(JSON.parse(b.creates[0].bodyJson).webhook_url,b.callback.callbackUrl);assert.equal(b.http.length,0);
});
test('concurrent first-wait mappings elect one original execution without overwriting it',async()=>{
  const b=new Boundary();const original=await b.original();
  const left=Object.create(b);left.executionId='100';left.resume=b.resume;
  const right=Object.create(b);right.executionId='101';right.resume=b.resume.replace('/100?','/101?');
  const outputs=await Promise.all([left.invoke('registerOriginalWait',{operationHandle:original.operationHandle}),right.invoke('registerOriginalWait',{operationHandle:original.operationHandle})]);
  assert.equal(outputs.filter(value=>value.registered).length,1);assert.equal(new Set(outputs.map(value=>value.originalWaitExecutionId)).size,1);assert.ok(outputs.every(value=>value.jobId===original.jobId));assert.equal(b.creates.length,1);assert.equal(b.http.length,0);
});
test('changing Wait row changes stable paid marker and cannot recover original map',async()=>{const b=new Boundary();const {original}=await b.ready();b.callback.waitRowId='4';await rejected(b.invoke('registerOriginalWait',{operationHandle:original.operationHandle}));assert.equal(b.http.length,0);});

function enableOriginalRead(b,hook){b.waitOptions={preparedRead:{transport:{getOriginal:async request=>{b.originalReads.push(clone(request));assert.equal(request.method,'GET');assert.equal(request.origin,'https://api.cuddler.ai');assert.ok(request.path.endsWith('/'+b.job.id));if(hook)return await hook(request);return {statusCode:200,job:{object:b.media,id:b.job.id,status:'succeeded',prompt:'PRIVATE_READ_PROMPT',output:{url:'https://example.invalid/PRIVATE_READ_URL'}}};},readAsset:async()=>{throw Error('asset not required for event ACK');}}}};}
async function consumeWaitBeforeCompletion(b){b.httpHook=async()=>{if(b.waitState.status!=='waiting')return {statusCode:409};b.waitState.status='consumed';b.waitState.claims++;throw Error('synthetic crash after native Wait claim before Complete');};const claim=await b.claim();const first=claim[0][0].json.deliveryHandle;assert.equal((await b.invoke('deliverOriginalWait',{deliveryHandle:first})).statusCode,503);return first;}
async function freshReceiver(b,action){const now=Date.now;try{const time=now()+61000;Date.now=()=>time;b.executionId='201';const claim=await b.claim();assert.equal(claim[0].length,1);await action(claim[0][0].json.deliveryHandle);}finally{Date.now=now;}}
for(const media of ['video','image','speech','music'])test('consumed Wait '+media+' crash/expired lease recovers original job then ACK with fresh lease',async()=>{
 const b=new Boundary(media);await b.ready();enableOriginalRead(b);const originalMap=clone(JSON.parse(b.rows.get(3).payload).slots);await consumeWaitBeforeCompletion(b);
 await freshReceiver(b,async handle=>{assert.equal((await b.invoke('deliverOriginalWait',{deliveryHandle:handle})).statusCode,503);const result=await b.invoke('recoverOriginalEvent',{deliveryHandle:handle});assert.equal(result.statusCode,200);assert.equal(result.jobId,b.job.id);assert.equal(result.disposition,'recovered-original-job');});
 assert.equal(b.waitState.claims,1);assert.equal(b.waitState.status,'consumed');assert.equal(b.creates.length,1);assert.equal(b.originalReads.length,1);for(const [slot,entry]of Object.entries(originalMap))assert.deepEqual(JSON.parse(b.rows.get(3).payload).slots[slot],entry);
 for(const value of [KEY,SECRET,TOKEN,'PRIVATE_PAID_PROMPT','PRIVATE_READ_PROMPT','PRIVATE_READ_URL'])assert.ok(!b.privateBoundaries().includes(value));
 const duplicate=await b.claim();assert.equal(duplicate[0].length,0);assert.equal(duplicate[1][0].json.statusCode,200);
});
test('installed GET refuses malformed HTTP after original Wait is consumed',async()=>{const b=new Boundary();await b.ready();const handle=await consumeWaitBeforeCompletion(b);const calls=b.http.length;assert.equal((await b.invoke('recoverOriginalEvent',{deliveryHandle:handle})).statusCode,503);assert.equal(b.originalReads.length,0);assert.equal(b.http.length,calls+1);assert.equal(b.http.at(-1).method,'GET');assert.equal(b.creates.length,1);});

for(const media of ['video','image','speech','music'])test('ordinary zero-argument installed '+media+' consumed-Wait recovery uses original GET before durable ACK',async()=>{
 const b=new Boundary(media);await b.ready();const originalMap=clone(JSON.parse(b.rows.get(3).payload).slots);await consumeWaitBeforeCompletion(b);
 let reads=0;b.httpHook=async options=>{
  if(options.method==='POST')return {statusCode:409};
  reads++;assert.equal(options.method,'GET');assert.equal(options.url,'https://api.cuddler.ai/v1/'+({video:'videos',image:'images',speech:'speech',music:'music'})[media]+'/'+b.job.id);
  assert.equal(options.headers.Authorization,'Bearer '+KEY);assert.equal(options.headers.Cookie,'');assert.equal(options.disableFollowRedirect,true);assert.equal(options.skipSslCertificateValidation,false);
  const body=Buffer.from(JSON.stringify({object:media,id:b.job.id,status:'succeeded',prompt:'PRIVATE_INSTALLED_PROMPT',output:{url:'https://private.invalid/PRIVATE_INSTALLED_ASSET'}}));
  return {statusCode:200,headers:{'content-type':'application/json','content-length':String(body.length)},body:Readable.from([body])};
 };
 await freshReceiver(b,async handle=>{
  const output=await new CuddlerTaskCallbackWait().execute.call(b.context({operation:'recoverOriginalEvent',deliveryHandle:handle}));
  b.outputs.push(clone(output));assert.equal(output[0][0].json.statusCode,200);assert.equal(output[0][0].json.jobId,b.job.id);assert.equal(output[0][0].json.disposition,'recovered-original-job');
 });
 assert.equal(reads,1);assert.equal(b.waitState.claims,1);assert.equal(b.waitState.status,'consumed');assert.equal(b.creates.length,1);assert.equal(b.originalReads.length,0);
 for(const [slot,entry]of Object.entries(originalMap))assert.deepEqual(JSON.parse(b.rows.get(3).payload).slots[slot],entry);
 const duplicate=await b.claim();assert.equal(duplicate[0].length,0);assert.equal(duplicate[1][0].json.statusCode,200);assert.equal(reads,1);
 for(const privateValue of [KEY,SECRET,TOKEN,'PRIVATE_INSTALLED_PROMPT','PRIVATE_INSTALLED_ASSET'])assert.equal(b.privateBoundaries().includes(privateValue),false);
});

for(const wrong of ['job','key'])test('ordinary installed callback recovery refuses wrong original '+wrong,async()=>{
 const b=new Boundary();await b.ready();const handle=await consumeWaitBeforeCompletion(b);let reads=0;
 if(wrong==='key')b.mediaCredential.apiKey='wrong-original-key';
 b.httpHook=async options=>{reads++;assert.equal(options.method,'GET');const body=Buffer.from(JSON.stringify({object:'video',id:'vid_'+'z'.repeat(24),status:'succeeded'}));return {statusCode:200,headers:{'content-type':'application/json'},body:Readable.from([body])};};
 if(wrong==='key')await rejected(new CuddlerTaskCallbackWait().execute.call(b.context({operation:'recoverOriginalEvent',deliveryHandle:handle})));
 else assert.equal((await b.invoke('recoverOriginalEvent',{deliveryHandle:handle})).statusCode,503);
 assert.equal(reads,wrong==='key'?0:1);assert.equal(Object.keys(JSON.parse(b.rows.get(3).payload).slots).length,1);assert.equal(b.creates.length,1);
});
test('current Inbox lease is required before original GET',async()=>{const b=new Boundary();await b.ready();enableOriginalRead(b);const handle=await consumeWaitBeforeCompletion(b);const now=Date.now;try{Date.now=()=>now()+61000;await rejected(b.invoke('recoverOriginalEvent',{deliveryHandle:handle}));}finally{Date.now=now;}assert.equal(b.originalReads.length,0);});
test('lease expiration during original GET prevents marker/completion and ACK',async()=>{const b=new Boundary();await b.ready();const handle=await consumeWaitBeforeCompletion(b);const now=Date.now;try{enableOriginalRead(b,async()=>{Date.now=()=>now()+61000;return {statusCode:200,job:{object:b.media,id:b.job.id,status:'succeeded'}};});await rejected(b.invoke('recoverOriginalEvent',{deliveryHandle:handle}));}finally{Date.now=now;}assert.equal(Object.keys(JSON.parse(b.rows.get(3).payload).slots).length,1);});
for(const wrong of ['job','status','object','lost','rate-limit'])test('original recovery '+wrong+' does not complete signed event',async()=>{const b=new Boundary();await b.ready();const handle=await consumeWaitBeforeCompletion(b);enableOriginalRead(b,async()=>{if(wrong==='lost')throw Error('PRIVATE_GET cause '+KEY);if(wrong==='rate-limit')return {statusCode:429,headers:{'retry-after':'10'}};return {statusCode:200,job:{object:wrong==='object'?'image':b.media,id:wrong==='job'?'vid_'+'z'.repeat(24):b.job.id,status:wrong==='status'?'failed':'succeeded'}};});assert.equal((await b.invoke('recoverOriginalEvent',{deliveryHandle:handle})).statusCode,503);assert.equal(Object.keys(JSON.parse(b.rows.get(3).payload).slots).length,1);assert.equal(b.creates.length,1);});
test('lost consumption-marker CAS response is recovered by durable readback, not a new Wait or create',async()=>{const b=new Boundary();await b.ready();enableOriginalRead(b);const handle=await consumeWaitBeforeCompletion(b);const child=b.child.bind(b);let lost=true;b.child=async(...args)=>{const result=await child(...args);const p=args[0].code.nodes[1].parameters;if(lost&&p.operation==='update'&&p.filters.conditions[0].keyValue===3){lost=false;throw Error('synthetic lost marker CAS response');}return result;};await rejected(b.invoke('recoverOriginalEvent',{deliveryHandle:handle}));assert.equal(b.originalReads.length,1);await freshReceiver(b,async current=>assert.equal((await b.invoke('recoverOriginalEvent',{deliveryHandle:current})).statusCode,200));assert.equal(b.originalReads.length,1);assert.equal(b.waitState.claims,1);assert.equal(b.creates.length,1);});
test('lost Inbox completion CAS response becomes completed duplicate only from durable state',async()=>{const b=new Boundary();await b.ready();enableOriginalRead(b);const handle=await consumeWaitBeforeCompletion(b);const child=b.child.bind(b);let lost=true;b.child=async(...args)=>{const result=await child(...args);const p=args[0].code.nodes[1].parameters;if(lost&&p.operation==='update'&&p.filters.conditions[0].keyValue===2){lost=false;throw Error('synthetic lost completion CAS response');}return result;};await rejected(b.invoke('recoverOriginalEvent',{deliveryHandle:handle}));b.executionId='201';const duplicate=await b.claim();assert.equal(duplicate[0].length,0);assert.equal(duplicate[1][0].json.statusCode,200);assert.equal(b.originalReads.length,1);});
test('marker CAS no-op cannot acknowledge recovery',async()=>{const b=new Boundary();await b.ready();enableOriginalRead(b);const handle=await consumeWaitBeforeCompletion(b);const child=b.child.bind(b);b.child=async(...args)=>{const p=args[0].code.nodes[1].parameters;if(p.operation==='update'&&p.filters.conditions[0].keyValue===3)return {executionId:'cas-noop',data:[[{json:clone(b.rows.get(3))}]]};return await child(...args);};await rejected(b.invoke('recoverOriginalEvent',{deliveryHandle:handle}));assert.equal(Object.keys(JSON.parse(b.rows.get(3).payload).slots).length,1);assert.equal(b.creates.length,1);});
test('native completion and GET recovery share one durable event completion',async()=>{const b=new Boundary();await b.ready();enableOriginalRead(b);const claim=await b.claim();const handle=claim[0][0].json.deliveryHandle;let proof;b.httpHook=async options=>{proof=clone(options.body);b.waitState.status='consumed';b.waitState.claims++;return {statusCode:409};};await b.invoke('deliverOriginalWait',{deliveryHandle:handle});const original=Object.create(b);original.executionId='100';original.parents=[clone(WAIT)];original.childrenMetadata=[];const results=await Promise.allSettled([original.invoke('completeOriginalWait',{resumeProof:proof}),b.invoke('recoverOriginalEvent',{deliveryHandle:handle})]);assert.ok(results.some(value=>value.status==='fulfilled'&&value.value.statusCode===200));b.executionId='201';const duplicate=await b.claim();assert.equal(duplicate[1][0].json.statusCode,200);assert.equal(b.waitState.claims,1);assert.equal(b.creates.length,1);});
for(const change of [b=>{b.callback.apiKey='wrong-original-key';},b=>{b.callback.waitRowId='4';},b=>{b.callback.ledgerRowId='4';}])test('recovery refuses changed key/row scope before original GET '+String(change),async()=>{const b=new Boundary();await b.ready();enableOriginalRead(b);const handle=await consumeWaitBeforeCompletion(b);change(b);await rejected(b.invoke('recoverOriginalEvent',{deliveryHandle:handle}));assert.equal(b.originalReads.length,0);});
test('tampered delivery and protected original mapping never call original GET',async()=>{const b=new Boundary();await b.ready();enableOriginalRead(b);const handle=await consumeWaitBeforeCompletion(b);const box=JSON.parse(handle);box.ciphertext='A'.repeat(box.ciphertext.length);await rejected(b.invoke('recoverOriginalEvent',{deliveryHandle:JSON.stringify(box)}));const ledger=JSON.parse(b.rows.get(3).payload);const entry=Object.values(ledger.slots)[0];const encrypted=JSON.parse(entry.planJson);encrypted.ciphertext='B'.repeat(encrypted.ciphertext.length);entry.planJson=JSON.stringify(encrypted);b.rows.get(3).payload=JSON.stringify(ledger);await rejected(b.invoke('recoverOriginalEvent',{deliveryHandle:handle}));assert.equal(b.originalReads.length,0);});
test('marker readback mismatch cannot acknowledge original recovery',async()=>{const b=new Boundary();await b.ready();enableOriginalRead(b);const handle=await consumeWaitBeforeCompletion(b);const child=b.child.bind(b);b.child=async(...args)=>{const p=args[0].code.nodes[1].parameters;const old=clone(b.rows.get(3));const result=await child(...args);if(p.operation==='update'&&p.filters.conditions[0].keyValue===3)b.rows.set(3,{...b.rows.get(3),payload:old.payload});return result;};await rejected(b.invoke('recoverOriginalEvent',{deliveryHandle:handle}));assert.equal(b.creates.length,1);});
test('Inbox completion no-op retains durable marker for fresh-lease recovery',async()=>{const b=new Boundary();await b.ready();enableOriginalRead(b);const handle=await consumeWaitBeforeCompletion(b);const child=b.child.bind(b);b.child=async(...args)=>{const p=args[0].code.nodes[1].parameters;if(p.operation==='update'&&p.filters.conditions[0].keyValue===2)return {executionId:'completion-noop',data:[[{json:clone(b.rows.get(2))}]]};return await child(...args);};await rejected(b.invoke('recoverOriginalEvent',{deliveryHandle:handle}));assert.equal(b.originalReads.length,1);b.child=child;await freshReceiver(b,async current=>assert.equal((await b.invoke('recoverOriginalEvent',{deliveryHandle:current})).statusCode,200));assert.equal(b.originalReads.length,1);});
