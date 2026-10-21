import type { IHookFunctions, INodeType, INodeTypeDescription, IWebhookFunctions, IWebhookResponseData } from 'n8n-workflow';
import { NodeConnectionTypes } from 'n8n-workflow';
import { callbackScope, credentials, digest, fail, localCredentialTest, RECEIPT_TTL, sameCredentials, verify } from './TaskCallbackContract';
import { TaskCallbackProtector } from './TaskCallbackProtector';

/** Stock receiver only. Local route ownership remains with n8n; no external lifecycle hooks. */
export class CuddlerTaskCallbackTrigger implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Cuddler Task Callback Trigger', name: 'cuddlerTaskCallbackTrigger',
    icon: 'file:cuddler.svg', group: ['trigger'], version: 1,
    description: 'Verify an existing per-task callback and expose only a protected event receipt',
    subtitle: 'Per-task callback',
    defaults: { name: 'Cuddler Task Callback' }, inputs: [], outputs: [NodeConnectionTypes.Main],
    credentials: [{ name: 'cuddlerTaskCallbackApi', required: true, testedBy: 'cuddlerTaskCallbackCredentialTest' }],
    webhooks: [{ name: 'default', httpMethod: 'POST', path: 'events', responseMode: 'responseNode', responseCode: '={{503}}' }],
    properties: [{ displayName: 'Manually activate this receiver and put its stable production URL in each original job webhook_url. Connect Receipt completion to Respond to Webhook. No external endpoint is registered; test URLs are refused.', name: 'notice', type: 'notice', default: '' }],
  };

  methods: NonNullable<INodeType['methods']> = { credentialTest: { cuddlerTaskCallbackCredentialTest: localCredentialTest } };

  // This provider takes webhook_url on each original job, not a subscription.
  // These hooks describe local manual setup only; they never manage remote state.
  webhookMethods: NonNullable<INodeType['webhookMethods']> = {
    default: {
      async checkExists(this: IHookFunctions): Promise<boolean> {
        const captured = credentials(await this.getCredentials('cuddlerTaskCallbackApi'));
        return captured.callbackUrl === this.getNodeWebhookUrl('default');
      },
      async create(): Promise<boolean> { return fail(); },
      async delete(): Promise<boolean> { return true; },
    },
  };

  async webhook(this: IWebhookFunctions): Promise<IWebhookResponseData> {
    try {
      const workflowId = this.getWorkflow().id;
      const triggerId = this.getNode().id;
      const callback = this.getNodeWebhookUrl('default');
      const served = this.getWebhookResourceUrl('default');
      const request = this.getRequestObject() as ReturnType<IWebhookFunctions['getRequestObject']> & { rawBody?: unknown };
      const settings = this.getWorkflowSettings() as Record<string, unknown>;
      const headers = this.getHeaderData();
      const signature = headers['cuddler-signature'];
      const eventId = headers['cuddler-event-id'];
      if (!workflowId || !triggerId || !callback || served !== callback || this.getMode() !== 'webhook' || settings.engineType === 'v2' ||
          request.method !== 'POST' || headers['content-type'] !== 'application/json' ||
          (headers['content-encoding'] !== undefined && headers['content-encoding'] !== 'identity') ||
          !Buffer.isBuffer(request.rawBody) || request.rawBody.length > 1024 * 1024 ||
          typeof signature !== 'string' || typeof eventId !== 'string') fail();
      // Detach mutable host bytes and headers before the first credential/crypto await.
      const raw = Buffer.from(request.rawBody);
      const captured = credentials(await this.getCredentials('cuddlerTaskCallbackApi'));
      if (captured.callbackUrl !== callback) fail();
      const recheck = async () => {
        if (this.getWorkflow().id !== workflowId || this.getNode().id !== triggerId || this.getMode() !== 'webhook' ||
            this.getNodeWebhookUrl('default') !== callback || this.getWebhookResourceUrl('default') !== callback ||
            !sameCredentials(captured, credentials(await this.getCredentials('cuddlerTaskCallbackApi')))) fail();
      };
      const event = await verify(raw, signature, eventId, captured.signingSecret);
      if (event.callbackScope !== await callbackScope(captured, workflowId)) fail();
      const protector = await TaskCallbackProtector.create(workflowId, triggerId, captured, await digest(callback));
      const verifiedAt = Date.now();
      const eventReceipt = await protector.seal('ingress', { event, verifiedAt, expiresAt: verifiedAt + RECEIPT_TTL });
      await recheck();
      return { workflowData: [[{ json: { event: event as unknown as Record<string, never>, eventId: event.eventId, eventReceipt } }]] };
    } catch {
      // Never throw request/raw/header/cause into the upstream reporter/error execution.
      const response = this.getResponseObject();
      response.status(401).json({ error: 'CUDDLER_TASK_CALLBACK_REJECTED' });
      return { noWebhookResponse: true };
    }
  }
}
