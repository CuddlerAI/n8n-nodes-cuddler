import type { IExecuteFunctions } from 'n8n-workflow';
import { createNativeCuddlerHttpTransports } from './NativeCuddlerHttpTransports';
import { createNativeOriginalJobReadTransport } from './NativeHttpTransport';

/**
 * Installed-source policy only. Selects the complete fixed-origin protected
 * API adapter with a separate exact storage-host policy.
 * Asset requests still require their separate exact installed host contract.
 * Workflow parameters, credentials and environment cannot replace this policy.
 * Neither the policy nor its host sets are exported.
 */
const policy: Readonly<{
  protectedApiEnabled: boolean;
  originalJobReadsEnabled: boolean;
  mediaAssetHosts: readonly string[];
  uploadAssetHosts: readonly string[];
}> = Object.freeze({
  protectedApiEnabled: true,
  originalJobReadsEnabled: true,
  // Public deployment SSOT: scripts/deploy/cloud-run-env.sh R2_ACCOUNT_ID;
  // apps/api/src/lib/private-storage.ts presigns private GET/PUT on this host.
  // Bucket/key are paths. Never derive extra trust from workflow/env/API input.
  mediaAssetHosts: Object.freeze(['4e8dd0f84c1d428e392189a299a9fa2c.r2.cloudflarestorage.com']),
  uploadAssetHosts: Object.freeze(['4e8dd0f84c1d428e392189a299a9fa2c.r2.cloudflarestorage.com']),
});

/**
 * Called once per recovery/upload item by the ordinary zero-argument node.
 * With both policies OFF, returns no capability before any context access. Asset
 * hosts must come from the deployed API's exact storage contract; none are
 * guessed here. API calls have a fixed origin and must not be gated by asset
 * host availability. Empty host lists refuse asset GET/PUT in their adapters.
 * The independent original-job read policy returns only protected GET
 * status reads, without any paid write, cancel, upload or asset capability.
 */
export function installedProductionTransportFactory(
  context: IExecuteFunctions,
  itemIndex: number,
): Partial<ReturnType<typeof createNativeCuddlerHttpTransports>> | undefined {
  if (policy.protectedApiEnabled) {
    return createNativeCuddlerHttpTransports(context, itemIndex, {
      media: { trustedAssetHosts: policy.mediaAssetHosts },
      upload: { trustedAssetHosts: policy.uploadAssetHosts },
    });
  }
  if (policy.originalJobReadsEnabled) {
    return {preparedRead:createNativeOriginalJobReadTransport(context,itemIndex)};
  }
  return undefined;
}
