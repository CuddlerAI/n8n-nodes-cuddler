/** SDK retry mode never supplies a new business intent or a trusted retryOf.
 * A retained authenticated handle is the recovery capability. This synchronous
 * guard checks only its envelope syntax; the normal AEAD/ledger/plan checks
 * remain mandatory before any transport call. Never inspect input JSON retryOf.
 */
export function assertRetryResume(mode: string, operation: string, opaque?: unknown): void {
  if (mode !== 'retry') return;
  if (operation === 'prepareMedia' || operation === 'prepareUpload') throw new Error('Retry preparation requires an original persistent handle');
  const prefix = ['executePrepared', 'readPrepared', 'pollPrepared', 'downloadPreparedFile'].includes(operation)
    ? 'cuddler-op1.' : ['executePreparedUpload', 'readPreparedUpload'].includes(operation) ? 'cuddler-up1.' : undefined;
  if (prefix && (typeof opaque !== 'string' || opaque.length > 16384 || !opaque.startsWith(prefix) ||
      !/^[A-Za-z0-9_-]+$/.test(opaque.slice(prefix.length)))) throw new Error('Original persistent operation handle is required for retry');
}
