'use strict';
// Clearly synthetic SDK boundary. It loads the actual built Cuddler execute
// entry, but cannot prove native SQL, host authentication or Cloud behavior.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const path = require('node:path');
const candidate = path.resolve(__dirname, '..');
const keyA = 'fixture-only-protected-credential-A-not-an-api-key';
const keyB = 'fixture-only-protected-credential-B-not-an-api-key';
const initialRow = () => ({ id: 1, etag: '0'.repeat(64), payload: JSON.stringify({format:'cuddler.n8n.cas-ledger/1',slots:{}}) });
const clone = value => structuredClone(value);
const sha = value => crypto.createHash('sha256').update(value).digest('hex');

class SyntheticBoundary {
  constructor(options = {}) {
    this.row = initialRow(); this.events = []; this.jobs = new Map();
    this.fault = null; this.childId = 1; this.key = keyA;
    this.ledgerTableId = 'SyntheticIntegrationTable'; this.ledgerRowId = 1;
    this.workflowId = 'CuddlerIntegrationSyntheticWorkflow';
    this.mode = 'manual'; this.options = options;
    this.transport = {
      create: async request => {
        this.events.push({kind:'create',request:clone(request)});
        assert.equal(request.origin,'https://api.cuddler.ai'); assert.equal(request.method,'POST');
        assert.equal(JSON.stringify(request).includes(this.key),false,'raw credential escaped into transport');
        const identity = request.credentialBinding + ':' + request.idempotencyKey;
        if (!this.jobs.has(identity)) this.jobs.set(identity,{id:({video:'vid_',image:'img_',speech:'aud_',music:'mus_'})[request.media]+sha(identity).slice(0,request.media==='music'?20:24),status:'queued'});
        const job = clone(this.jobs.get(identity));
        if (this.fault === 'lost-create-response') { this.fault = null; throw new Error('synthetic lost response'); }
        return job;
      },
      get: async request => {
        this.events.push({kind:'get',request:clone(request)});
        assert.equal(request.origin,'https://api.cuddler.ai');assert.equal(request.method,'GET');
        assert.equal(JSON.stringify(request).includes(this.key),false,'raw credential escaped into transport');
        if (this.fault === 'job-query-error') throw new Error('synthetic upstream includes '+this.key);
        return {id:request.jobId,status:'succeeded'};
      },
    };
  }

  context(parameters, options = {}) {
    const boundary = this; const items = options.items || [{json:{}}];
    return {
      getInputData: () => items,
      getNodeParameter: (name,index,fallback) => {
        const value = typeof parameters === 'function' ? parameters(index,items[index]) : parameters;
        return Object.prototype.hasOwnProperty.call(value,name) ? value[name] : fallback;
      },
      getCredentials: async name => {
        boundary.events.push({kind:'credentials',name});
        if(options.onCredentials)await options.onCredentials();
        return {apiKey:boundary.key,ledgerTableId:boundary.ledgerTableId,ledgerRowId:boundary.ledgerRowId};
      },
      getWorkflow: () => ({id:boundary.workflowId}),
      getExecutionId: () => options.executionId || 'synthetic_execution_'+crypto.randomUUID(),
      getMode: () => boundary.mode,
      getNode: () => ({id:options.nodeId||'prepare-node',name:'Synthetic Cuddler',type:'CUSTOM.cuddler',typeVersion:1,position:[0,0],parameters:{}}),
      continueOnFail: () => !!options.continueOnFail,
      logger: {error:()=>{},warn:()=>{},debug:()=>{},info:()=>{}},
      helpers: {
        prepareBinaryData: async (bytes,filename,mimeType) => {
          boundary.events.push({kind:'binary-publish',bytes:bytes.length,filename,mimeType,sha256:sha(bytes)});
          if(options.onBinary) await options.onBinary();
          return {data:Buffer.from(bytes).toString('base64'),mimeType,fileName:filename,bytes:bytes.length};
        },
        httpRequest: async () => {throw new Error('unexpected live HTTP helper')},
        httpRequestWithAuthentication: async () => {throw new Error('unexpected authenticated HTTP helper')},
      },
      executeWorkflow: async (input,data,_unused,executionOptions) => {
        const workflow=input.code;
        assert.equal(typeof workflow,'object'); assert.equal(workflow.nodes.length,2);
        assert.equal(workflow.nodes[0].type,'n8n-nodes-base.executeWorkflowTrigger');
        const node=workflow.nodes[1]; assert.equal(node.type,'n8n-nodes-base.dataTable');
        const p=node.parameters; const childId=String(boundary.childId++);
        assert.equal(p.dataTableId.value,boundary.ledgerTableId);
        assert.equal(p.matchType,'allConditions');
        assert.ok(executionOptions.parentExecution.executionId);
        assert.equal(executionOptions.parentExecution.workflowId,boundary.workflowId);
        let rows=[];
        if(p.operation==='get'){
          boundary.events.push({kind:'store-read',payloadSha256:sha(boundary.row.payload)});
          if(boundary.fault==='read-error')throw new Error('synthetic store read error');
          rows=[clone(boundary.row)];
          if(boundary.fault==='readback-mismatch' && boundary.events.some(e=>e.kind==='store-write'))rows[0].payload=JSON.stringify({format:'cuddler.n8n.cas-ledger/1',slots:{}});
        } else if(p.operation==='update') {
          boundary.events.push({kind:'store-write',oldEtag:p.filters.conditions[1].keyValue,payloadSha256:sha(p.columns.value.payload)});
          assert.equal(p.filters.conditions.length,2);assert.equal(p.filters.conditions[0].keyName,'id');
          assert.equal(p.filters.conditions[0].keyValue,1);assert.equal(p.filters.conditions[1].keyName,'etag');
          if(boundary.fault==='write-error')throw new Error('synthetic store write error');
          if(boundary.row.etag===p.filters.conditions[1].keyValue){boundary.row={...boundary.row,...clone(p.columns.value)};rows=[clone(boundary.row)]}
          if(boundary.fault==='fail-read-after-write')boundary.fault='read-error';
        } else throw new Error('unexpected synthetic DataTable operation');
        return {executionId:childId,data:[rows.map(json=>({json}))]};
      },
    };
  }

  async invoke(parameters, options = {}) {
    const {Cuddler}=require(path.join(candidate,'dist/nodes/Cuddler/Cuddler.node.js'));
    const node=options.defaultTransport?new Cuddler():new Cuddler({recoveryTransport:this.transport, preparedRead:options.preparedRead});
    return node.execute.call(this.context(parameters,options));
  }

  count(kind){return this.events.filter(event=>event.kind===kind).length}
  sideEffects(){return this.events.filter(event=>['create','get'].includes(event.kind))}
}

module.exports={SyntheticBoundary,keyA,keyB,clone,sha,candidate};
