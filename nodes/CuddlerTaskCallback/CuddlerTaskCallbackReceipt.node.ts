import type { IDataObject, IExecuteFunctions, INodeExecutionData, INodeType, INodeTypeDescription } from 'n8n-workflow';
import { NodeConnectionTypes, NodeOperationError } from 'n8n-workflow';
import { credentials, exact, fail, identity, LEASE_MS, localCredentialTest, RECEIPT_TTL, safeEvent, sameCredentials, type SafeEvent } from './TaskCallbackContract';
import { TaskCallbackProtector } from './TaskCallbackProtector';
import { BuiltinDataTableBridge } from '../Cuddler/recovery/BuiltinDataTableBridge';
import { TaskCallbackInbox, type Lease } from './TaskCallbackInbox';

export class CuddlerTaskCallbackReceipt implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Cuddler Task Callback Receipt', name: 'cuddlerTaskCallbackReceipt', icon: 'file:cuddler.svg',
    group: ['transform'], version: 1, description: 'Claim authenticated events and complete durable delivery before acknowledgement',
    usableAsTool: true,
    subtitle: '={{$parameter["operation"]}}',
    defaults: { name: 'Cuddler Task Callback Receipt' }, inputs: [NodeConnectionTypes.Main], outputs: [NodeConnectionTypes.Main, NodeConnectionTypes.Main],
    outputNames: ['Business Action', 'Acknowledgement'], credentials: [{ name: 'cuddlerTaskCallbackApi', required: true, testedBy: 'cuddlerTaskCallbackCredentialTest' }],
    properties: [
      { displayName: 'Operation', name: 'operation', type: 'options', noDataExpression: true, options: [
        { name: 'Claim Event', value: 'claimEvent', action: 'Claim an event' },
        { name: 'Complete Event', value: 'completeEvent', action: 'Complete an event' },
      ], default: 'claimEvent' },
      { displayName: 'Event Receipt', name: 'eventReceipt', type: 'string', default: '={{$json.eventReceipt}}', required: true, displayOptions: { show: { operation: ['claimEvent'] } }, description: 'Authenticated receipt from Cuddler Task Callback Trigger. Do not replace with a JSON event ID.' },
      { displayName: 'Delivery Handle', name: 'deliveryHandle', type: 'string', default: '={{$json.deliveryHandle}}', required: true, displayOptions: { show: { operation: ['completeEvent'] } }, description: 'Encrypted handle from Claim Event, kept in the same execution' },
      { displayName: 'Business consumers must be idempotent by eventId. A 60 second host-clock lease can be reclaimed after a crash. Connect Acknowledgement to Respond to Webhook using statusCode; never acknowledge the Business Action output.', name: 'notice', type: 'notice', default: '' },
    ],
  };

  methods: NonNullable<INodeType['methods']> = { credentialTest: { cuddlerTaskCallbackCredentialTest: localCredentialTest } };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    try {
      // Snapshot all parameter values before credentials, crypto or child executions await.
      const items = this.getInputData();
      const parameters = items.map((_, index) => {
        const operation = this.getNodeParameter('operation', index);
        if (operation !== 'claimEvent' && operation !== 'completeEvent') fail();
        const value = this.getNodeParameter(operation === 'claimEvent' ? 'eventReceipt' : 'deliveryHandle', index);
        if (typeof value !== 'string') fail();
        return { operation, value };
      });
      const workflowId = this.getWorkflow().id;
      const executionId = this.getExecutionId();
      const nodeId = this.getNode().id;
      if (this.getNode().type !== 'n8n-nodes-cuddler.cuddlerTaskCallbackReceipt' || !workflowId || !executionId || !nodeId || this.continueOnFail() || this.getNode().retryOnFail || this.getNode().continueOnFail || (this.getNode().onError && this.getNode().onError !== 'stopWorkflow')) fail();
      const captured = credentials(await this.getCredentials('cuddlerTaskCallbackApi'));
      const recheck = async () => {
        if (this.getWorkflow().id !== workflowId || this.getExecutionId() !== executionId || this.getNode().id !== nodeId ||
            !sameCredentials(captured, credentials(await this.getCredentials('cuddlerTaskCallbackApi')))) fail();
      };
      const outputs: INodeExecutionData[][] = [[], []];
      const bridge = new BuiltinDataTableBridge(this, []);
      for (let index = 0; index < parameters.length; index++) {
        const { operation, value } = parameters[index];
        const purpose = operation === 'claimEvent' ? 'ingress' : 'delivery';
        const binding = TaskCallbackProtector.peekBinding(value, purpose);
        if (binding.workflowId !== workflowId || binding.tableId !== captured.tableId || binding.rowId !== captured.rowId) fail();
        const protector = await TaskCallbackProtector.create(workflowId, binding.triggerNodeId, captured, binding.callbackDigest);
        const opened = await protector.open(purpose, value);
        await recheck();
        const inbox = new TaskCallbackInbox(bridge.capability(captured.tableId), protector, recheck);
        const ack = (event: SafeEvent, statusCode: 200 | 503, disposition: string) => {
          outputs[1].push({ json: { eventId: event.eventId, statusCode, disposition }, pairedItem: { item: index } });
        };
        if (operation === 'claimEvent') {
          exact(opened, 'event,verifiedAt,expiresAt');
          const event = safeEvent(opened.event);
          if (!Number.isSafeInteger(opened.verifiedAt) || Number(opened.verifiedAt) <= 0 || Number(opened.verifiedAt) > Date.now() + 1000 ||
              opened.expiresAt !== Number(opened.verifiedAt) + RECEIPT_TTL || Number(opened.expiresAt) <= Date.now()) fail();
          const result = await inbox.claim(event, executionId, Number(opened.verifiedAt));
          if (result.outcome !== 'accepted') { ack(result.event, result.outcome === 'duplicate' ? 200 : 503, result.outcome); continue; }
          const deliveryHandle = await protector.seal('delivery', { event: result.event, slot: result.slot, lease: result.lease });
          await recheck();
          if (!result.lease || result.lease.until <= Date.now()) fail();
          outputs[0].push({ json: { event: result.event as unknown as IDataObject, eventId: result.event.eventId, deliveryHandle }, pairedItem: { item: index } });
        } else {
          exact(opened, 'event,slot,lease');
          const event = safeEvent(opened.event);
          exact(opened.lease, 'token,executionId,until');
          if (typeof opened.slot !== 'string' || !/^[a-f0-9]{64}$/.test(opened.slot) || typeof opened.lease.token !== 'string' || !/^[a-f0-9]{64}$/.test(opened.lease.token) ||
              opened.lease.executionId !== executionId || !Number.isSafeInteger(opened.lease.until) || Number(opened.lease.until) <= Date.now() || Number(opened.lease.until) > Date.now() + LEASE_MS + 1000 || !identity(event)) fail();
          await inbox.complete(event, opened.slot, opened.lease as unknown as Lease, executionId);
          ack(event, 200, 'completed');
        }
      }
      await recheck();
      return outputs;
    } catch {
      throw new NodeOperationError(this.getNode(), 'CUDDLER_TASK_CALLBACK_REJECTED');
    }
  }
}
