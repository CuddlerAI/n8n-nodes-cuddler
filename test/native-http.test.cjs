'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const {Readable}=require('node:stream');
const {createHmac}=require('node:crypto');
const {setTimeout:delay}=require('node:timers/promises');
const {createNativeHttpTransports}=require('../dist/nodes/Cuddler/recovery/NativeHttpTransport.js');

// All keys, IDs, links and request bodies below are local synthetic fixtures.
const KEY_A='fixture-only-native-transport-a';
const KEY_B='fixture-only-native-transport-b';
const HOST='assets.public-cdn.com';
const ORIGIN='https://api.cuddler.ai';
const FAILURE='Cuddler transport request was rejected or could not complete.';
const paths={video:'/v1/videos',image:'/v1/images',speech:'/v1/speech',music:'/v1/music'};
const ids={video:'vid_0123456789abcdef01234567',image:'img_0123456789abcdef01234567',speech:'aud_0123456789abcdef01234567',music:'mus_0123456789abcdef0123'};
const binding=key=>createHmac('sha256',key).update('cuddler.n8n.hmac-json/1/credential-binding').digest('hex');
function read(media='video') {return {origin:ORIGIN,method:'GET',path:paths[media]+'/'+ids[media],media,jobId:ids[media],credentialBinding:binding(KEY_A)}}
function create(media='video') {return {origin:ORIGIN,method:'POST',path:paths[media],media,bodyJson:' {"model":"synthetic-model","max_usd":"0.010000","prompt":"synthetic-only 日本語"} ',idempotencyKey:'n8n-01234567-89ab-4def-8123-0123456789ab',credentialBinding:binding(KEY_A)}}
function publicJob(media='video',extra={}) {return {object:media,id:ids[media],status:'succeeded',poll_after_seconds:10,output:{url:`https://${HOST}/${ids[media]}/file`,url_expires_at:new Date(Date.now()+60000).toISOString(),retained_until:new Date(Date.now()+86400000).toISOString(),content_type:media==='image'?'image/png':media==='video'?'video/mp4':'audio/mpeg'},...extra}}
function full(body,statusCode=200,headers={}) {
 const bytes=Buffer.from(typeof body==='string'?body:JSON.stringify(body));
 return {statusCode,headers:{'content-type':'application/json','content-length':String(bytes.length),...headers},body:Readable.from([bytes])};
}
function harness(options={}) {
 const calls=[],credentialCalls=[];
 const state={key:KEY_A,row:{apiKey:KEY_A},reply:undefined,signal:undefined};
 const context={
  getExecutionCancelSignal:()=>state.signal,
  getCredentials:async(type,index)=>{credentialCalls.push({type,index});return options.credentials?await options.credentials(state):{apiKey:state.key}},
  helpers:{httpRequest:async request=>{calls.push(request);return options.http?await options.http(request,state):full(publicJob(options.media||'video'),request.method==='POST'?202:200)}}
 };
 const native=createNativeHttpTransports(context,3,{trustedAssetHosts:[HOST],...options.factory});
 return {native,context,state,calls,credentialCalls};
}
async function rejects(operation) {
 await assert.rejects(operation,error=>{
  assert.equal(error.message,FAILURE);assert.equal(error.cause,undefined);
  assert.equal(Object.hasOwn(error,'response'),false);assert.equal(Object.hasOwn(error,'headers'),false);return true;
 });
}
async function grant(h,media='video') {await h.native.preparedRead.transport.getOriginal(read(media));return {method:'GET',url:publicJob(media).output.url,headers:{},credentials:'omit',followRedirects:false,maxBytes:1000}}

for(const media of Object.keys(paths)) test(`fixed ${media} create preserves approved bytes and idempotency`,async()=>{
 const h=harness({media});const input=create(media);const result=await h.native.recoveryTransport.create(input);
 assert.deepEqual(result,{id:ids[media],status:'succeeded'});assert.equal(h.calls.length,1);
 const request=h.calls[0];assert.equal(request.url,ORIGIN+paths[media]);assert.equal(request.method,'POST');
 assert.ok(Buffer.isBuffer(request.body));assert.equal(request.body.toString(),input.bodyJson);
 assert.equal(request.headers['Idempotency-Key'],input.idempotencyKey);assert.equal(request.headers.Authorization,'Bearer '+KEY_A);assert.equal(request.headers.Cookie,'');
 assert.equal(request.disableFollowRedirect,true);assert.equal(request.maxRedirects,0);assert.equal(request.sendCredentialsOnCrossOriginRedirect,false);
 assert.equal(request.encoding,'stream');assert.equal(request.returnFullResponse,true);assert.equal(request.skipSslCertificateValidation,false);
 assert.equal(request.allowedDomains,'api.cuddler.ai');assert.deepEqual(h.credentialCalls,[{type:'cuddlerApi',index:3}]);
});

test('matched captured key is sent even if credential object rotates before helper',async()=>{
 const row={apiKey:KEY_A};const h=harness({credentials:async()=>row,http:async request=>{row.apiKey=KEY_B;assert.equal(request.headers.Authorization,'Bearer '+KEY_A);return full(publicJob())}});
 await h.native.recoveryTransport.get(read());assert.equal(h.credentialCalls.length,1);
 await rejects(()=>h.native.recoveryTransport.get(read()));assert.equal(h.calls.length,1);
});

test('wrong credential binding rejects before HTTP and no authentication helper is used',async()=>{
 const h=harness();h.state.key=KEY_B;h.context.helpers.httpRequestWithAuthentication=()=>assert.fail('must never fetch another key');
 await rejects(()=>h.native.recoveryTransport.create(create()));assert.equal(h.calls.length,0);
});

test('caller request is detached before the first credential await',async()=>{
 let release;const blocked=new Promise(resolve=>{release=resolve});
 const h=harness({credentials:async()=>{await blocked;return {apiKey:KEY_A}}});
 const request=create(),original=request.bodyJson;const pending=h.native.recoveryTransport.create(request);
 request.bodyJson='{"model":"changed"}';request.path='/v1/music';request.media='music';request.credentialBinding=binding(KEY_B);release();
 await pending;assert.equal(h.calls[0].body.toString(),original);assert.equal(h.calls[0].url,ORIGIN+'/v1/videos');
});

for(const change of [
 r=>r.origin='https://other.com',r=>r.path='/v1/videos/'+ids.video+'/download',r=>r.jobId='../escape',r=>r.path+='?other=1',r=>r.media='other',r=>r.headers={Authorization:'caller'},
]) test('invalid original request fails before credential lookup',async()=>{
 const h=harness(),request=read();change(request);await rejects(()=>h.native.recoveryTransport.get(request));assert.equal(h.calls.length,0);assert.equal(h.credentialCalls.length,0);
});

test('API redirect is destroyed and never followed',async()=>{
 const response=full('raw private error',302,{'location':'https://other.com','set-cookie':'fixture-private-cookie'});
 const h=harness({http:async()=>response});await rejects(()=>h.native.recoveryTransport.get(read()));assert.equal(h.calls.length,1);assert.equal(response.body.destroyed,true);
});

test('raw helper failures never escape with key/header/prompt/cause',async()=>{
 const h=harness({http:async()=>{const error=new Error(KEY_A+' private prompt');error.response={headers:{cookie:'private'}};throw error}});
 await rejects(()=>h.native.recoveryTransport.get(read()));
});

test('original 429 exposes only the bounded retry hint',async()=>{
 const response=full('private failure',429,{'retry-after':'120','set-cookie':'private','authorization':'private'});
 const h=harness({http:async()=>response});const result=await h.native.preparedRead.transport.getOriginal(read());
 assert.deepEqual(result,{statusCode:429,headers:{'retry-after':'120'}});assert.equal(response.body.destroyed,true);
});

test('malformed retry header is rejected rather than exposed',async()=>{
 const h=harness({http:async()=>full('private failure',429,{'retry-after':'private credential text'})});
 await rejects(()=>h.native.preparedRead.transport.getOriginal(read()));
});

test('invalid UTF8 JSON bytes are rejected without replacement decoding',async()=>{
 const bytes=Buffer.concat([Buffer.from('{"object":"video","id":"'+ids.video+'","status":"succeeded","output":{"url":"'),Buffer.from([0xff]),Buffer.from('"}}')]);
 const h=harness({http:async()=>({statusCode:200,headers:{'content-type':'application/json'},body:Readable.from([bytes])})});
 await rejects(()=>h.native.preparedRead.transport.getOriginal(read()));
});

for(const extra of [{id:ids.image},{object:'image'},{status:{secret:'private'}},{poll_after_seconds:{secret:'private'}}]) test('invalid original response cannot leak typed fields',async()=>{
 const h=harness({http:async()=>full(publicJob('video',extra))});await rejects(()=>h.native.recoveryTransport.get(read()));
});

test('declared oversize API response is refused before buffering',async()=>{
 let reads=0;const stream=new Readable({read(){reads++;this.push(Buffer.alloc(10));this.push(null)}});
 const h=harness({http:async()=>({statusCode:200,headers:{'content-type':'application/json','content-length':String(1024*1024+1)},body:stream})});
 await rejects(()=>h.native.recoveryTransport.get(read()));assert.equal(stream.destroyed,true);assert.equal(reads,0);
});

test('API body must be a real async stream and not an already buffered body',async()=>{
 const h=harness({http:async()=>({statusCode:200,headers:{'content-type':'application/json'},body:Buffer.from('{}')})});await rejects(()=>h.native.recoveryTransport.get(read()));
});

for(const host of ['localhost','127.0.0.1','[::1]','*.public-cdn.com','http://public-cdn.com','assets.public-cdn.com:443','api.cuddler.ai','asset.internal','asset.example','host.1']) test(`asset policy rejects nonpublic exact host ${host}`,()=>{
 assert.throws(()=>harness({factory:{trustedAssetHosts:[host]}}),{message:FAILURE});
});

test('empty default asset host list stays fail closed',async()=>{
 const h=harness({factory:{trustedAssetHosts:[]}}),request=await grant(h);await rejects(()=>h.native.preparedRead.transport.readAsset(request));assert.equal(h.calls.length,1);
});

test('allowed host alone cannot authorize a URL that original GET did not return',async()=>{
 const h=harness();await rejects(()=>h.native.preparedRead.transport.readAsset({method:'GET',url:`https://${HOST}/unknown`,headers:{},credentials:'omit',followRedirects:false,maxBytes:1000}));assert.equal(h.calls.length,0);
});

test('asset request is bound to original protected credential, never forwards Bearer or cookie',async()=>{
 const bytes=Buffer.from('synthetic media bytes');
 const h=harness({http:async request=>request.url.startsWith(ORIGIN)?full(publicJob()):{statusCode:200,headers:{'content-type':'video/mp4','content-length':String(bytes.length),'set-cookie':'private','authorization':'private'},body:Readable.from([bytes])}});
 const request=await grant(h),result=await h.native.preparedRead.transport.readAsset(request);
 assert.equal(result.body.toString(),bytes.toString());assert.deepEqual(result.headers,{'content-type':'video/mp4','content-length':String(bytes.length)});
 const asset=h.calls[1];assert.equal(asset.headers.Authorization,'');assert.equal(asset.headers.Cookie,'');assert.equal(asset.headers['Accept-Encoding'],'identity');assert.equal(Object.isFrozen(asset.headers),false);assert.equal(asset.allowedDomains,HOST);assert.equal(h.credentialCalls.length,2);
});

test('credential rotation between original GET and asset fails before asset HTTP',async()=>{
 const h=harness(),request=await grant(h);h.state.key=KEY_B;await rejects(()=>h.native.preparedRead.transport.readAsset(request));assert.equal(h.calls.length,1);
});

for(const change of [r=>r.url='http://'+HOST+'/file',r=>r.url=`https://user@${HOST}/file`,r=>r.url=`https://${HOST}:444/file`,r=>r.headers={Authorization:'forbidden'},r=>r.followRedirects=true,r=>r.credentials='include']) test('caller asset mutation is rejected',async()=>{
 const h=harness(),request=await grant(h);change(request);await rejects(()=>h.native.preparedRead.transport.readAsset(request));assert.equal(h.calls.length,1);
});

test('asset stream is aborted as soon as bounded chunks exceed the cap',async()=>{
 let response;const h=harness({http:async request=>{
  if(request.url.startsWith(ORIGIN))return full(publicJob());
  response={statusCode:200,headers:{'content-type':'video/mp4'},body:Readable.from([Buffer.alloc(4),Buffer.alloc(4),Buffer.alloc(100000)])};return response;
 }});const request=await grant(h);request.maxBytes=7;
 await rejects(()=>h.native.preparedRead.transport.readAsset(request));assert.equal(response.body.destroyed,true);assert.equal(h.calls[1].abortSignal.aborted,true);
});

test('asset advertised length over cap aborts without reading any chunk',async()=>{
 let reads=0,stream;const h=harness({http:async request=>{
  if(request.url.startsWith(ORIGIN))return full(publicJob());
  stream=new Readable({read(){reads++;this.push(Buffer.alloc(10));this.push(null)}});return {statusCode:200,headers:{'content-length':'1001','content-type':'video/mp4'},body:stream};
 }});await rejects(async()=>h.native.preparedRead.transport.readAsset(await grant(h)));assert.equal(reads,0);assert.equal(stream.destroyed,true);
});

test('stalled asset stream has a wall-clock abort and destroys the source',async()=>{
 let stream;const h=harness({factory:{timeoutMs:25},http:async request=>request.url.startsWith(ORIGIN)?full(publicJob()):{statusCode:200,headers:{'content-type':'video/mp4'},body:(stream=new Readable({read(){}}))}});
 const request=await grant(h);const keepAlive=delay(70);await rejects(()=>h.native.preparedRead.transport.readAsset(request));assert.equal(stream.destroyed,true);await keepAlive;
});

test('header timeout rejects and destroys a late stream even if helper ignores abort',async()=>{
 const late=full(publicJob());const h=harness({factory:{timeoutMs:15},http:async()=>{await delay(40);return late}});
 await rejects(()=>h.native.recoveryTransport.get(read()));await delay(50);assert.equal(late.body.destroyed,true);
});

test('execution cancellation before request avoids credentials and helper',async()=>{
 const h=harness(),controller=new AbortController();controller.abort();h.state.signal=controller.signal;
 await rejects(()=>h.native.recoveryTransport.get(read()));assert.equal(h.calls.length,0);assert.equal(h.credentialCalls.length,0);
});

test('asset redirect is rejected and source destroyed',async()=>{
 let stream;const h=harness({http:async request=>request.url.startsWith(ORIGIN)?full(publicJob()):{statusCode:302,headers:{location:'https://other.com'},body:(stream=Readable.from([Buffer.from('private')]))}});
 const request=await grant(h);await rejects(()=>h.native.preparedRead.transport.readAsset(request));assert.equal(stream.destroyed,true);assert.equal(h.calls.length,2);
});

test('expired asset response may refresh original job without exposing headers or bytes',async()=>{
 const h=harness({http:async request=>request.url.startsWith(ORIGIN)?full(publicJob()):{statusCode:403,headers:{'set-cookie':'private','location':'private'},body:Readable.from([Buffer.from('private response')])}});
 const request=await grant(h);assert.deepEqual(await h.native.preparedRead.transport.readAsset(request),{statusCode:403,headers:{},body:Buffer.alloc(0)});
});

test('compressed or truncated asset cannot pass bounded consumption',async()=>{
 for(const metadata of [{'content-encoding':'gzip'},{'content-length':'12'}]) {
  const h=harness({http:async request=>request.url.startsWith(ORIGIN)?full(publicJob()):{statusCode:200,headers:{'content-type':'video/mp4',...metadata},body:Readable.from([Buffer.from('short')])}});
  const request=await grant(h);await rejects(()=>h.native.preparedRead.transport.readAsset(request));
 }
});
