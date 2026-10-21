import type { IDataObject, IExecuteFunctions, INodeExecutionData, INodeType, INodeTypeDescription } from 'n8n-workflow';
import { NodeConnectionTypes, NodeOperationError } from 'n8n-workflow';
import { readOperation } from './transport';
import { installedProductionTransportFactory } from './recovery/InstalledProductionTransport';
import { preparedReadCapability } from './recovery/PreparedReadOperation';
import type { PreparedReadOptions } from './recovery/PreparedReadTransport';
import { preparedCancelCapability, CancelRefusal } from './recovery/PreparedCancelOperation';
import type { PreparedCancelOptions } from './recovery/PreparedCancelTransport';
import { assertRetryResume } from './recovery/RetryResumePolicy';
import { uploadCapability, snapshotUploadInputs, preflightUploads, UploadRefusal } from './recovery/UploadOperation';
import type { UploadOptions } from './recovery/UploadTransport';
import { preflightRecovery, recoveryOperation, snapshotRecoveryInputs } from './recovery/RecoveryOperation';
import { disabledRecoveryTransport, type RecoveryTransport } from './recovery/RecoveryTransport';

/** Trusted installed-code capabilities only; never workflow parameters or env. */
export interface TrustedItemTransports { recoveryTransport?: RecoveryTransport; preparedRead?: PreparedReadOptions; upload?: UploadOptions; preparedCancel?: PreparedCancelOptions }
export type TrustedItemTransportFactory = (context: IExecuteFunctions, itemIndex: number) => TrustedItemTransports;
export interface CuddlerOptions extends TrustedItemTransports { trustedTransportFactory?: TrustedItemTransportFactory }

export class Cuddler implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Cuddler',
    name: 'cuddler',
    icon: 'file:cuddler.svg',
    group: ['transform'],
    version: 1,
    description: 'Read the Cuddler catalog, estimate media and recover existing jobs',
    subtitle: '={{$parameter["resource"] + ": " + $parameter["operation"]}}',
    defaults: { name: 'Cuddler' },
    usableAsTool: true,
    inputs: [NodeConnectionTypes.Main],
    outputs: [NodeConnectionTypes.Main],
    credentials: [{ name: 'cuddlerApi', required: true, displayOptions: { hide: { operation: ['publicCatalog'] } } }],
    properties: [
      {
        displayName: 'Resource', name: 'resource', type: 'options', noDataExpression: true,
        options: [
          { name: 'Balance', value: 'balance' },
          { name: 'Catalog', value: 'catalog' },
          { name: 'Image', value: 'image' },
          { name: 'Music', value: 'music' },
          { name: 'Speech', value: 'speech' },
          { name: 'Upload', value: 'upload' },
          { name: 'Video', value: 'video' },
        ],
        default: 'catalog',
      },
      {
        displayName: 'Operation', name: 'operation', type: 'options', noDataExpression: true,
        displayOptions: { show: { resource: ['catalog'] } },
        options: [
          { name: 'Get Model', value: 'getModel', action: 'Get a model' },
          { name: 'Get Public Catalog', value: 'publicCatalog', action: 'Get public catalog' },
          { name: 'List Models', value: 'listModels', action: 'List models' },
        ],
        default: 'publicCatalog',
      },
      {
        displayName: 'Operation', name: 'operation', type: 'options', noDataExpression: true,
        displayOptions: { show: { resource: ['balance'] } },
        options: [{ name: 'Get', value: 'getBalance', action: 'Get balance' }],
        default: 'getBalance',
      },
      {
        displayName: 'Operation', name: 'operation', type: 'options', noDataExpression: true,
        displayOptions: { show: { resource: ['image'] } },
        options: [
          { name: 'Download Prepared File', value: 'downloadPreparedFile', action: 'Download an original image file' },
          { name: 'Estimate', value: 'estimateImage', action: 'Estimate an image' },
          { name: 'Execute Prepared', value: 'executePrepared', action: 'Execute a prepared image' },
          { name: 'Get', value: 'getImage', action: 'Get an existing image' },
          { name: 'Poll Prepared', value: 'pollPrepared', action: 'Poll an original image' },
          { name: 'Prepare', value: 'prepareMedia', action: 'Prepare a image' },
          { name: 'Read Prepared', value: 'readPrepared', action: 'Read a prepared image' },
        ],
        default: 'estimateImage',
      },
      {
        displayName: 'Operation', name: 'operation', type: 'options', noDataExpression: true,
        displayOptions: { show: { resource: ['music'] } },
        options: [
          { name: 'Cancel Prepared', value: 'cancelPrepared', action: 'Cancel an original queued music' },
          { name: 'Download Prepared File', value: 'downloadPreparedFile', action: 'Download an original music file' },
          { name: 'Estimate', value: 'estimateMusic', action: 'Estimate music' },
          { name: 'Execute Prepared', value: 'executePrepared', action: 'Execute a prepared music' },
          { name: 'Get', value: 'getMusic', action: 'Get existing music' },
          { name: 'Poll Prepared', value: 'pollPrepared', action: 'Poll an original music' },
          { name: 'Prepare', value: 'prepareMedia', action: 'Prepare a music' },
          { name: 'Read Prepared', value: 'readPrepared', action: 'Read a prepared music' },
        ],
        default: 'estimateMusic',
      },
      {
        displayName: 'Operation', name: 'operation', type: 'options', noDataExpression: true,
        displayOptions: { show: { resource: ['speech'] } },
        options: [
          { name: 'Cancel Prepared', value: 'cancelPrepared', action: 'Cancel an original queued speech' },
          { name: 'Download Prepared File', value: 'downloadPreparedFile', action: 'Download an original speech file' },
          { name: 'Estimate', value: 'estimateSpeech', action: 'Estimate speech' },
          { name: 'Execute Prepared', value: 'executePrepared', action: 'Execute a prepared speech' },
          { name: 'Get', value: 'getSpeech', action: 'Get existing speech' },
          { name: 'Poll Prepared', value: 'pollPrepared', action: 'Poll an original speech' },
          { name: 'Prepare', value: 'prepareMedia', action: 'Prepare a speech' },
          { name: 'Read Prepared', value: 'readPrepared', action: 'Read a prepared speech' },
        ],
        default: 'estimateSpeech',
      },
      {
        displayName: 'Operation', name: 'operation', type: 'options', noDataExpression: true,
        displayOptions: { show: { resource: ['upload'] } },
        options: [
          { name: 'Execute Prepared Upload', value: 'executePreparedUpload', action: 'Execute an original upload' },
          { name: 'Get', value: 'getUpload', action: 'Get an existing upload' },
          { name: 'Prepare Upload', value: 'prepareUpload', action: 'Prepare an upload' },
          { name: 'Read Prepared Upload', value: 'readPreparedUpload', action: 'Read an original upload' },
        ],
        default: 'getUpload',
      },
      {
        displayName: 'Operation', name: 'operation', type: 'options', noDataExpression: true,
        displayOptions: { show: { resource: ['video'] } },
        options: [
          { name: 'Cancel Prepared', value: 'cancelPrepared', action: 'Cancel an original queued video' },
          { name: 'Download Prepared File', value: 'downloadPreparedFile', action: 'Download an original video file' },
          { name: 'Estimate', value: 'estimateVideo', action: 'Estimate a video' },
          { name: 'Execute Prepared', value: 'executePrepared', action: 'Execute a prepared video' },
          { name: 'Get', value: 'getVideo', action: 'Get an existing video' },
          { name: 'Poll Prepared', value: 'pollPrepared', action: 'Poll an original video' },
          { name: 'Prepare', value: 'prepareMedia', action: 'Prepare a video' },
          { name: 'Read Prepared', value: 'readPrepared', action: 'Read a prepared video' },
        ],
        default: 'estimateVideo',
      },
      {
        displayName: 'ID', name: 'id', type: 'string', required: true, default: '',
        description: 'Existing model, job or upload ID. This operation never creates a replacement job.',
        displayOptions: { show: { operation: ['getModel', 'getVideo', 'getImage', 'getSpeech', 'getMusic', 'getUpload'] } },
      },
      {
        displayName: 'Estimate Parameters', name: 'estimateBody', type: 'json', default: '{}',
        description: 'JSON request parameters from the model documentation. Estimates do not start generation.',
        displayOptions: { show: { operation: ['estimateVideo', 'estimateImage', 'estimateSpeech', 'estimateMusic'] } },
      },
      {
        displayName: 'Request Body', name: 'requestBody', type: 'json', default: '',
        description: 'Complete original media request for Prepare. Optional on resume; if supplied it must match exactly. Stored encrypted in the protected ledger.',
        displayOptions: { show: { operation: ['prepareMedia', 'executePrepared', 'readPrepared', 'pollPrepared', 'downloadPreparedFile', 'cancelPrepared'] } },
      },
      {
        displayName: 'Business Batch ID', name: 'businessBatchId', type: 'string', default: '', required: true,
        description: 'Explicit stable business batch. A new intent requires a new batch; never derive this from item position or the current execution.',
        displayOptions: { show: { operation: ['prepareMedia', 'prepareUpload'] } },
      },
      {
        displayName: 'Stable Item ID', name: 'stableItemId', type: 'string', default: '',
        description: 'Stable business item within the batch. Required for Prepare; optional original-item check for resume.',
        displayOptions: { show: { operation: ['prepareMedia', 'executePrepared', 'readPrepared', 'pollPrepared', 'downloadPreparedFile', 'cancelPrepared', 'prepareUpload', 'executePreparedUpload', 'readPreparedUpload'] } },
      },
      {
        displayName: 'Operation Handle', name: 'operationHandle', type: 'string', default: '', required: true,
        description: 'Opaque handle from Prepare. This always resumes the original persisted operation.',
        displayOptions: { show: { operation: ['executePrepared', 'readPrepared', 'pollPrepared', 'downloadPreparedFile', 'cancelPrepared', 'executePreparedUpload', 'readPreparedUpload'] } },
      },
      {
        displayName: 'Input Binary Field', name: 'inputBinaryProperty', type: 'string', default: 'data', required: true,
        description: 'Native binary field to snapshot into host-managed storage',
        displayOptions: { show: { operation: ['prepareUpload'] } },
      },
      {
        displayName: 'Upload Kind', name: 'uploadKind', type: 'options', default: 'image',
        options: [{ name: 'Audio', value: 'audio' }, { name: 'Image', value: 'image' }, { name: 'Video', value: 'video' }],
        description: 'The reference media type; actual bytes, MIME and size are checked',
        displayOptions: { show: { operation: ['prepareUpload'] } },
      },
      {
        displayName: 'Output Binary Field', name: 'outputBinaryProperty', type: 'string', default: 'data', required: true,
        description: 'Host-managed binary field for the complete original file',
        displayOptions: { show: { operation: ['downloadPreparedFile'] } },
      },
      {
        displayName: 'Maximum File Bytes', name: 'maxFileBytes', type: 'number', default: 20971520,
        typeOptions: { minValue: 1, maxValue: 209715200 },
        description: 'Client download cap. An oversized file fails without creating another job.',
        displayOptions: { show: { operation: ['downloadPreparedFile'] } },
      },
    ],
  };

  execute: (this: IExecuteFunctions) => Promise<INodeExecutionData[][]>;

  constructor(options: CuddlerOptions = {}) {
    // This closure is supplied only by trusted installed code. No workflow JSON,
    // expression, environment variable or credential can select a transport.
    const factory = options.trustedTransportFactory ?? installedProductionTransportFactory;
    const directTransport = options.recoveryTransport;
    const directPreparedRead = options.preparedRead;
    const directUpload = options.upload;
    const directCancel = options.preparedCancel;
    const preparedRead = preparedReadCapability(directPreparedRead);
    const upload = uploadCapability(directUpload);
    const cancelPrepared = preparedCancelCapability(directCancel);
    this.execute = async function (this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
      const items = this.getInputData();
      const results: INodeExecutionData[] = [];
      let recoveryInputs;
      let uploadInputs;
      try {
        // Reject all prepare entries before snapshots, credentials or any await.
        if (this.getMode() === 'retry') for (let index = 0; index < items.length; index++) {
          const operation = this.getNodeParameter('operation', index) as string;
          if (operation === 'prepareMedia' || operation === 'prepareUpload') assertRetryResume('retry', operation);
        }
        recoveryInputs = snapshotRecoveryInputs(this);
        uploadInputs = snapshotUploadInputs(this);
        for (const input of [...recoveryInputs, ...uploadInputs]) if (input) assertRetryResume(this.getMode(), input.operation, input.opaque);
        await preflightRecovery(recoveryInputs);
        await preflightUploads(uploadInputs);
      } catch {
        throw new NodeOperationError(this.getNode(), 'Cuddler stable batch/item preflight failed. No operations were prepared.');
      }
      for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
        try {
          const operation = this.getNodeParameter('operation', itemIndex) as string;
          let json: IDataObject;
          let binary: INodeExecutionData['binary'];
          const recoveryInput = recoveryInputs[itemIndex];
          const uploadInput = uploadInputs[itemIndex];
          // A fresh factory instance per item prevents asset URL grants from
          // crossing items. Explicit offline constructor overrides win.
          const trusted = (recoveryInput || uploadInput) && factory ? factory(this, itemIndex) : undefined;
          const transport = directTransport ?? trusted?.recoveryTransport ?? disabledRecoveryTransport;
          const itemRead = trusted && directPreparedRead === undefined ? preparedReadCapability(trusted.preparedRead) : preparedRead;
          const itemUpload = trusted && directUpload === undefined ? uploadCapability(trusted.upload) : upload;
          const itemCancel = trusted && directCancel === undefined ? preparedCancelCapability(trusted.preparedCancel) : cancelPrepared;
          if (uploadInput) {
            json = await itemUpload(this, itemIndex, uploadInput);
          } else if (recoveryInput) {
            if (recoveryInput.operation === 'cancelPrepared') {
              json = await itemCancel(this, itemIndex, recoveryInput);
            } else if (['pollPrepared', 'downloadPreparedFile'].includes(recoveryInput.operation)) {
              const output = await itemRead(this, itemIndex, recoveryInput);
              json = output.json;
              binary = output.binary;
            } else {
              json = await recoveryOperation(this, itemIndex, recoveryInput, transport);
            }
          } else {
            const id = this.getNodeParameter('id', itemIndex, '') as string;
            const rawBody = operation.startsWith('estimate') ? this.getNodeParameter('estimateBody', itemIndex, '{}') : '{}';
            const body: unknown = typeof rawBody === 'string' ? JSON.parse(rawBody) : rawBody;
            if (!body || typeof body !== 'object' || Array.isArray(body)) throw new NodeOperationError(this.getNode(), 'Invalid JSON request', { itemIndex });
            json = await readOperation(this, operation, id, body as IDataObject);
          }
          results.push({ json, ...(binary ? { binary } : {}), pairedItem: { item: itemIndex } });
        } catch (error) {
          // Never attach provider/library errors: they may contain prompts,
          // keys, internal plans or signed URLs.
          const message = error instanceof CancelRefusal && /^[A-Z0-9_]{1,64}$/.test(error.code)
            ? `Cuddler cancel refused: ${error.code}` : error instanceof UploadRefusal && /^[A-Z0-9_]{1,64}$/.test(error.code)
            ? `Cuddler upload refused: ${error.code}` : 'Cuddler request failed. Check the original operation, protected ledger and credentials.';
          const failure = new NodeOperationError(this.getNode(), message, { itemIndex });
          if (!this.continueOnFail()) throw failure;
          results.push({ json: { error: failure.message }, pairedItem: { item: itemIndex } });
        }
      }
      return [results];
    };
  }
}
