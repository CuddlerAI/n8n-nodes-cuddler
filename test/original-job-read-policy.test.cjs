'use strict';
// Real compiled narrow adapter; SDK credentials/HTTP stream are synthetic.
// The narrow exported GET capability remains separate from the full installed adapter.
// This remains synthetic helper-boundary proof, not a native host result.
const test=require('node:test');
const assert=require('node:assert/strict');
const {Readable}=require('node:stream');
const {createHmac}=require('node:crypto');
const {createNativeOriginalJobReadTransport}=require('../dist/nodes/Cuddler/recovery/NativeHttpTransport.js');
const {installedProductionTransportFactory}=require('../dist/nodes/Cuddler/recovery/InstalledProductionTransport.js');
const KEY='synthetic-only-original-status-key';
const paths={video:'/v1/videos',image:'/v1/images',speech:'/v1/speech',music:'/v1/music'};
const ids={video:'vid_'+'a'.repeat(24),image:'img_'+'b'.repeat(24),speech:'aud_'+'c'.repeat(24),music:'mus_'+'d'.repeat(20)};
const credentialBinding=createHmac('sha256',KEY).update('cuddler.n8n.hmac-json/1/credential-binding').digest('hex');
const request=(media='video')=>({origin:'https://api.cuddler.ai',method:'GET',path:paths[media]+'/'+ids[media],media,jobId:ids[media],credentialBinding});
function full(value,statusCode=200,headers={}) {
  const body=Buffer.from(JSON.stringify(value));
  return {statusCode,headers:{'content-type':'application/json','content-length':String(body.length),...headers},body:Readable.from([body])};
}
function boundary(media='video',reply) {
  const calls=[],credentials=[];
  const context={getExecutionCancelSignal:()=>undefined,
    getCredentials:async(type,index)=>{credentials.push({type,index});return {apiKey:KEY};},
    helpers:{httpRequest:async options=>{calls.push(options);return reply?reply(options):full({object:media,id:ids[media],status:'succeeded',stage:'preparing_references',poll_after_seconds:10,prompt:'PRIVATE_PROMPT',provider_error:'PRIVATE_ERROR',metadata:{private:'PRIVATE_METADATA'},output:{url:'https://private.invalid/PRIVATE_ASSET',url_expires_at:new Date(Date.now()+60000).toISOString()}});},
      httpRequestWithAuthentication:()=>assert.fail('the original captured key must be used')},
  };
  return {context,calls,credentials,read:createNativeOriginalJobReadTransport(context,2)};
}

test('installed source selects complete protected API capability before any context read',()=>{
  const context=new Proxy({}, {get(){assert.fail('factory construction must not touch context');}});
  const installed=installedProductionTransportFactory(context,0);
  assert.deepEqual(Object.keys(installed).sort(),['preparedCancel','preparedRead','recoveryTransport','upload']);
  assert.deepEqual(Object.keys(installed.preparedRead.transport).sort(),['getOriginal','readAsset']);
  assert.deepEqual(installed.preparedRead.trustedAssetHosts,['4e8dd0f84c1d428e392189a299a9fa2c.r2.cloudflarestorage.com']);
  assert.deepEqual(Object.keys(require('../dist/nodes/Cuddler/recovery/InstalledProductionTransport.js')),['installedProductionTransportFactory']);
});

for(const media of Object.keys(paths)) test('narrow '+media+' read uses bound GET and emits only status projection',async()=>{
  const b=boundary(media);
  const result=await b.read.transport.getOriginal(request(media));
  assert.deepEqual(result,{statusCode:200,job:{object:media,id:ids[media],status:'succeeded',stage:'preparing_references',poll_after_seconds:10}});
  assert.deepEqual(Object.keys(b.read).sort(),['transport','trustedAssetHosts']);
  assert.deepEqual(Object.keys(b.read.transport).sort(),['getOriginal','readAsset']);
  assert.deepEqual(b.read.trustedAssetHosts,[]);
  assert.ok(Object.isFrozen(b.read)&&Object.isFrozen(b.read.transport)&&Object.isFrozen(b.read.trustedAssetHosts));
  assert.deepEqual(b.credentials,[{type:'cuddlerApi',index:2}]);
  assert.equal(b.calls.length,1);
  const actual=b.calls[0];
  assert.equal(actual.method,'GET');assert.equal(actual.url,'https://api.cuddler.ai'+paths[media]+'/'+ids[media]);
  assert.equal(actual.headers.Authorization,'Bearer '+KEY);assert.equal(actual.headers.Cookie,'');
  assert.equal(actual.disableFollowRedirect,true);assert.equal(actual.maxRedirects,0);assert.equal(actual.skipSslCertificateValidation,false);
  assert.equal(actual.allowedDomains,'api.cuddler.ai');assert.equal(actual.body,undefined);
  for(const value of ['PRIVATE_PROMPT','PRIVATE_ERROR','PRIVATE_METADATA','PRIVATE_ASSET',KEY]) assert.equal(JSON.stringify(result).includes(value),false);
});

test('narrow status read never grants asset/create/cancel/upload capability',async()=>{
  const b=boundary();await b.read.transport.getOriginal(request());
  await assert.rejects(()=>b.read.transport.readAsset({method:'GET',url:'https://private.invalid/PRIVATE_ASSET',headers:{},credentials:'omit',followRedirects:false,maxBytes:1024}),{message:'Cuddler transport request was rejected or could not complete.'});
  assert.equal(b.calls.length,1);assert.equal(b.credentials.length,1);
  for(const name of ['create','cancelQueued','upload','recoveryTransport','preparedCancel']) assert.equal(b.read[name]??b.read.transport[name],undefined);
});

for(const change of [r=>r.method='POST',r=>r.path+='/cancel',r=>r.origin='https://other.invalid',r=>r.path+='?auth=private',r=>r.jobId='../other']) test('narrow read rejects altered method/origin/original path before credentials '+String(change),async()=>{
  const b=boundary(),input=request();change(input);
  await assert.rejects(()=>b.read.transport.getOriginal(input),{message:'Cuddler transport request was rejected or could not complete.'});
  assert.equal(b.credentials.length,0);assert.equal(b.calls.length,0);
});

test('read-only adapter retains bounded retry projection without response body or secret headers',async()=>{
  const b=boundary('video',()=>full({private:'PRIVATE_BODY'},503,{'retry-after':'120','set-cookie':'PRIVATE_COOKIE','authorization':'PRIVATE_HEADER'}));
  assert.deepEqual(await b.read.transport.getOriginal(request()),{statusCode:503,headers:{'retry-after':'120'}});
});

test('read-only adapter keeps raw helper errors and credentials out of errors',async()=>{
  const b=boundary('video',()=>{const e=new Error('PRIVATE_ERROR '+KEY);e.response={headers:{authorization:KEY}};throw e;});
  await assert.rejects(()=>b.read.transport.getOriginal(request()),error=>{
    assert.equal(error.message,'Cuddler transport request was rejected or could not complete.');
    assert.equal(error.cause,undefined);assert.equal(error.response,undefined);assert.equal(error.headers,undefined);
    return true;
  });
});
