import type { ICredentialType, INodeProperties, Icon } from 'n8n-workflow';

export class CuddlerTaskCallbackApi implements ICredentialType {
  name = 'cuddlerTaskCallbackApi';
  displayName = 'Cuddler Task Callback API';
  icon: Icon = 'file:../nodes/CuddlerTaskCallback/cuddler.svg';
  documentationUrl = 'https://cuddler.ai/docs/api';
  properties: INodeProperties[] = [
    { displayName: 'API Key', name: 'apiKey', type: 'string', typeOptions: { password: true }, required: true, default: '', description: 'Existing organization key, used locally to protect this workflow inbox. Never sent by these callback nodes.' },
    { displayName: 'Signing Secret', name: 'signingSecret', type: 'string', typeOptions: { password: true }, required: true, default: '', description: 'Complete existing whsec_ secret for the same organization. No endpoint is created or modified.' },
    { displayName: 'Inbox Table ID', name: 'ledgerTableId', type: 'string', required: true, default: '', description: 'Existing project Data Table with string etag and payload columns. Use a separate row from the media ledger.' },
    { displayName: 'Inbox Row ID', name: 'ledgerRowId', type: 'string', required: true, default: '', description: 'Existing row initialized to the exact documented inbox format. No upsert or account resource is created.' },
    { displayName: 'Wait Map Table ID', name: 'waitTableId', type: 'string', required: true, default: '', description: 'Existing project Data Table with etag and payload columns, separate from the inbox/media row' },
    { displayName: 'Wait Map Row ID', name: 'waitRowId', type: 'string', required: true, default: '', description: 'Existing row initialized to the native cas-ledger format. Changes invalidate the original frozen callback scope.' },
    { displayName: 'Production Callback URL', name: 'callbackUrl', type: 'string', required: true, default: '', description: 'Exact production URL shown by this Trigger. Freeze it in the original job webhook_url before submitting.' },
  ];
}
