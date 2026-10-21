import type { IExecuteFunctions } from 'n8n-workflow';
import { createNativeHttpTransports, type NativeTransportOptions } from './NativeHttpTransport';
import { createNativeUploadHttpTransport, type NativeUploadTransportOptions } from './NativeUploadHttpTransport';

/** Installed-code policy only. No workflow parameter or environment activation. */
export interface NativeCuddlerTransportOptions {
  media?: NativeTransportOptions;
  upload?: NativeUploadTransportOptions;
}

/**
 * The ordinary installed factory selects this composition once per item using
 * its source policy. Trusted constructor overrides remain isolated test inputs.
 * Media and upload factories own separate URL grants and credential domains;
 * workflow parameters and credentials cannot replace the installed host policy.
 */
export function createNativeCuddlerHttpTransports(
  context: IExecuteFunctions,
  itemIndex: number,
  options: NativeCuddlerTransportOptions = {},
): ReturnType<typeof createNativeHttpTransports> & { upload: ReturnType<typeof createNativeUploadHttpTransport> } {
  return {
    ...createNativeHttpTransports(context, itemIndex, options.media),
    upload: createNativeUploadHttpTransport(context, itemIndex, options.upload),
  };
}
