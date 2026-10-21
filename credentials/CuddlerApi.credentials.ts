import type { IAuthenticateGeneric, ICredentialTestRequest, ICredentialType, INodeProperties, Icon } from 'n8n-workflow';

export class CuddlerApi implements ICredentialType {
  name = 'cuddlerApi';
  displayName = 'Cuddler API';
  icon: Icon = 'file:../nodes/Cuddler/cuddler.svg';
  documentationUrl = 'https://cuddler.ai/docs/api';
  properties: INodeProperties[] = [
    {
      displayName: 'API Key',
      name: 'apiKey',
      type: 'string',
      typeOptions: { password: true },
      required: true,
      default: '',
      description: 'Organization API key. Test keys support catalog, estimates and balance only.',
    },
    {
      displayName: 'Ledger Table ID', name: 'ledgerTableId', type: 'string', default: '',
      description: 'Existing project-authorized Data Table with string etag and payload columns. Required for prepare/resume operations.',
    },
    {
      displayName: 'Ledger Row ID', name: 'ledgerRowId', type: 'string', default: '',
      description: 'Existing unique ledger row. Keep this binding stable across retries and key rotation; changes require controlled migration.',
    },
  ];
  authenticate: IAuthenticateGeneric = {
    type: 'generic',
    properties: { headers: { Authorization: '=Bearer {{$credentials.apiKey}}' } },
  };
  test: ICredentialTestRequest = {
    request: { baseURL: 'https://api.cuddler.ai', url: '/v1/balance', disableFollowRedirect: true },
  };
}
