import type { IDataObject, IExecuteFunctions, INodeExecutionData, INodeType, INodeTypeDescription } from 'n8n-workflow';
import { NodeConnectionTypes, NodeOperationError } from 'n8n-workflow';
import { canonical } from '../Cuddler/recovery/ImmutablePlan';
import { fail, localCredentialTest } from './TaskCallbackContract';
import { installedProductionTransportFactory } from '../Cuddler/recovery/InstalledProductionTransport';
import { preparedReadCapability } from '../Cuddler/recovery/PreparedReadOperation';
import type { PreparedReadOptions } from '../Cuddler/recovery/PreparedReadTransport';
import type { TrustedItemTransportFactory } from '../Cuddler/Cuddler.node';
import { CallbackWaitAuthority } from './CallbackWaitAuthority';

export interface CallbackWaitOptions { preparedRead?: PreparedReadOptions; trustedTransportFactory?: TrustedItemTransportFactory }

export class CuddlerTaskCallbackWait implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Cuddler Callback Wait', name: 'cuddlerTaskCallbackWait', icon: 'file:cuddler.svg',
    group: ['transform'], version: 1, subtitle: '={{$parameter["operation"]}}', usableAsTool: true,
    description: 'Bind a stable original job to one native Wait and acknowledge only durable completion',
    defaults: { name: 'Cuddler Callback Wait' }, inputs: [NodeConnectionTypes.Main], outputs: [NodeConnectionTypes.Main],
    credentials: [{ name: 'cuddlerApi', required: true }, { name: 'cuddlerTaskCallbackApi', required: true, testedBy: 'cuddlerTaskCallbackCredentialTest' }],
    properties: [
      { displayName: 'Operation', name: 'operation', type: 'options', noDataExpression: true, options: [
        { name: 'Complete Original Wait', value: 'completeOriginalWait', action: 'Complete an original wait' },
        { name: 'Deliver to Original Wait', value: 'deliverOriginalWait', action: 'Deliver to an original wait' },
        { name: 'Prepare Callback Scope', value: 'prepareCallbackScope', action: 'Prepare a callback scope' },
        { name: 'Recover Original Event', value: 'recoverOriginalEvent', action: 'Recover an original event' },
        { name: 'Register Original Wait', value: 'registerOriginalWait', action: 'Register an original wait' },
      ], default: 'prepareCallbackScope' },
      { displayName: 'Operation Handle', name: 'operationHandle', type: 'string', required: true, default: '={{$json.operationHandle}}', displayOptions: { show: { operation: ['registerOriginalWait'] } }, description: 'Original encrypted media handle, after its authenticated original job ID has been persisted' },
      { displayName: 'Delivery Handle', name: 'deliveryHandle', type: 'string', required: true, default: '={{$json.deliveryHandle}}', displayOptions: { show: { operation: ['deliverOriginalWait', 'recoverOriginalEvent'] } }, description: 'Same-execution authenticated Inbox claim, never a plain event or URL' },
      { displayName: 'Resume Proof', name: 'resumeProof', type: 'json', required: true, default: '={{$json.body}}', displayOptions: { show: { operation: ['completeOriginalWait'] } }, description: 'Encrypted proof delivered to the original native Wait, verified against its actual execution' },
      { displayName: 'One job and one native webhook Wait per execution. Freeze callbackUrl and callbackScope in the initial paid request before Prepare. Use a finite response-node Wait with fallback503. No waiter replacement, arbitrary URL or AI Tool alias is supported.', name: 'notice', type: 'notice', default: '' },
    ],
  };

  methods: NonNullable<INodeType['methods']> = { credentialTest: { cuddlerTaskCallbackCredentialTest: localCredentialTest } };

  execute: (this: IExecuteFunctions) => Promise<INodeExecutionData[][]>;

  constructor(options: CallbackWaitOptions = {}) {
    const directRead = options.preparedRead;
    const factory = options.trustedTransportFactory ?? installedProductionTransportFactory;
    this.execute = async function(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
      try {
        if (this.getNode().type !== 'n8n-nodes-cuddler.cuddlerTaskCallbackWait' || this.continueOnFail()) fail();
        const items = this.getInputData();
        if (items.length !== 1) fail();
        const operation = this.getNodeParameter('operation', 0);
        if (!['prepareCallbackScope', 'registerOriginalWait', 'deliverOriginalWait', 'completeOriginalWait', 'recoverOriginalEvent'].includes(String(operation))) fail();
        let value: unknown;
        if (operation === 'registerOriginalWait' || operation === 'deliverOriginalWait' || operation === 'recoverOriginalEvent') {
          value = this.getNodeParameter(operation === 'registerOriginalWait' ? 'operationHandle' : 'deliveryHandle', 0);
          if (typeof value !== 'string' || value.length > 16384) fail();
        } else if (operation === 'completeOriginalWait') {
          const input = this.getNodeParameter('resumeProof', 0);
          value = JSON.parse(canonical(typeof input === 'string' ? JSON.parse(input) as unknown : input)) as unknown;
        }
        // All workflow-supplied values are detached before credentials, storage or HTTP.
        const authority = await CallbackWaitAuthority.open(this, async (handle, media) => {
          // Constructor/installed-code capability only; only called after the
          // original mapping and current durable Inbox lease are authenticated.
          const trusted = directRead === undefined ? factory(this, 0) : undefined;
          return await preparedReadCapability(directRead ?? trusted?.preparedRead)(this, 0,
            { operation: 'pollPrepared', media, opaque: handle, expectedItem: '', body: undefined });
        });
        let output: IDataObject;
        if (operation === 'prepareCallbackScope') output = await authority.setup();
        else if (operation === 'registerOriginalWait') output = await authority.register(value as string);
        else if (operation === 'deliverOriginalWait') output = await authority.deliver(value as string);
        else if (operation === 'recoverOriginalEvent') output = await authority.recover(value as string);
        else output = await authority.complete(value);
        return [[{ json: output, pairedItem: { item: 0 } }]];
      } catch {
        throw new NodeOperationError(this.getNode(), 'CUDDLER_CALLBACK_WAIT_REJECTED');
      }
    };
  }
}
