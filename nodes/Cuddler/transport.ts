import type { IDataObject, IExecuteFunctions, IHttpRequestOptions } from 'n8n-workflow';

const BASE_URL = 'https://api.cuddler.ai/v1';

export type ReadOperation =
  | 'publicCatalog' | 'listModels' | 'getModel' | 'getBalance'
  | 'estimateVideo' | 'estimateImage' | 'estimateSpeech' | 'estimateMusic'
  | 'getVideo' | 'getImage' | 'getSpeech' | 'getMusic' | 'getUpload';

const READ_PATHS = {
  publicCatalog: '/catalog', listModels: '/models', getBalance: '/balance',
  getModel: '/models/', getVideo: '/videos/', getImage: '/images/',
  getSpeech: '/speech/', getMusic: '/music/', getUpload: '/uploads/',
} as const;
const ESTIMATE_PATHS = {
  estimateVideo: '/videos/estimate', estimateImage: '/images/estimate',
  estimateSpeech: '/speech/estimate', estimateMusic: '/music/estimate',
} as const;

export async function readOperation(
  context: IExecuteFunctions,
  operation: string,
  id: string,
  body: IDataObject,
): Promise<IDataObject> {
  let path: string;
  let method: 'GET' | 'POST' = 'GET';
  if (Object.prototype.hasOwnProperty.call(ESTIMATE_PATHS, operation)) {
    path = ESTIMATE_PATHS[operation as keyof typeof ESTIMATE_PATHS];
    method = 'POST';
  } else if (Object.prototype.hasOwnProperty.call(READ_PATHS, operation)) {
    path = READ_PATHS[operation as keyof typeof READ_PATHS];
    if (path.endsWith('/')) {
      if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(id)) {
        throw new Error('Provide a valid model, job or upload ID from an existing API response');
      }
      path += encodeURIComponent(id);
    }
  } else {
    throw new Error('Generation is unavailable until durable operation-plan storage is verified');
  }
  const options: IHttpRequestOptions = {
    url: `${BASE_URL}${path}`,
    method,
    json: true,
    timeout: 30000,
    disableFollowRedirect: true,
    ...(method === 'POST' ? { body } : {}),
  };
  const result: unknown = operation === 'publicCatalog'
    ? await context.helpers.httpRequest(options)
    : await context.helpers.httpRequestWithAuthentication.call(context, 'cuddlerApi', options);
  if (!result || typeof result !== 'object' || Array.isArray(result)) {
    throw new Error('Cuddler returned an invalid JSON response');
  }
  return result as IDataObject;
}
