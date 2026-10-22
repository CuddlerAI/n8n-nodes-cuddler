# Cuddler nodes for n8n

Source for Cuddler media operations, original-job recovery, binary files, uploads, and per-task callbacks in n8n. Licensed under MIT.

[`n8n-nodes-cuddler@0.1.0`](https://www.npmjs.com/package/n8n-nodes-cuddler/v/0.1.0) was published on October 22, 2026 through [the protected GitHub Actions release](https://github.com/CuddlerAI/n8n-nodes-cuddler/actions/runs/39775616129), with npm provenance for commit `b0baed12afec441d3f87b912703eb716ea8b512f`. Registry signatures, the official n8n scanner, clean installation and free original-job/file checks passed; see the [release acceptance receipt](https://github.com/CuddlerAI/Cuddler/issues/6455#issuecomment-6340616676). Real video/callback acceptance was completed separately in [the technical receipt](https://github.com/CuddlerAI/Cuddler/issues/6454#issuecomment-6313422437).

**n8n Creator verification and actual n8n Cloud installation remain pending.** A public npm package and a successful scanner report do not mean that n8n has verified the integration. Current documentation may include post-release status updates; the immutable 0.1.0 source is the provenance-linked commit above.

## Compatibility and installation

The integration targets n8n 2.43.7. It uses n8n's native Data Tables, native Wait, and managed binary data. No external runtime dependency is bundled. Compatibility with other n8n versions requires separate verification.

For self-hosted installation, open **Settings → Community nodes → Install**, enter `n8n-nodes-cuddler@0.1.0`, review the community-node installation notice, and install. Version 0.1.0 was installed and tested on n8n 2.43.7. Cloud installation requires n8n verification first and is not yet available as a verified integration. For source development, use Node.js 22 or 24 and npm, then run:

```sh
npm ci --ignore-scripts
npm run lint
npm run build
npm test
npm run check:package
```

These commands build and check the source; they do not publish a package, configure an n8n host, provision credentials, or run a paid generation. A self-hosted installation must use the final reviewed package and the host's supported community-node installation procedure. n8n Cloud installation additionally requires n8n's own approval.

## Included nodes

- **Cuddler:** catalog and estimates; protected image, video, speech, and music requests; original-job reads and bounded polling; file download; uploads; queued video/speech/music cancellation; and recovery of the original request after a lost response.
- **Cuddler Task Callback Trigger:** verifies signed per-task events and produces a narrow encrypted receipt.
- **Cuddler Task Callback Receipt:** durably claims or completes a callback event.
- **Cuddler Callback Wait:** prepares a callback scope, registers one original Wait, delivers to that Wait, or recovers consumption of the original terminal-job event.

The API origin is fixed to `https://api.cuddler.ai`. Media downloads and signed upload PUTs use a separate, exact Cuddler storage-host policy. Workflow parameters, credentials, and environment variables cannot select arbitrary API or asset hosts. Image cancellation is unavailable.

## Credentials and durable storage

Select the same existing organization API key in the **Cuddler API** and **Cuddler Task Callback API** credentials. Store the callback signing secret only in the callback credential. Do not paste keys, signing secrets, or resume URLs into workflow JSON or ordinary node fields. This package does not create organization keys or obtain a signing secret.

Provision three separate rows in a project-accessible native n8n Data Table. Each row needs the string columns `etag` and `payload`; select rows by their system IDs:

1. Original media ledger.
2. Callback Inbox.
3. Original Wait map.

For a newly provisioned media-ledger or Wait-map row, use:

```json
{"format":"cuddler.n8n.cas-ledger/1","slots":{}}
```

For a newly provisioned callback Inbox row, use:

```json
{"format":"cuddler.n8n.per-task-inbox/2","slots":{}}
```

Set an initial 64-hex `etag`, such as 64 zeroes, only when provisioning a new empty row. Do not overwrite populated rows, share a row between these roles, or reuse an older Inbox format as v2. There is no automatic migration, row creation, upsert, deletion, eviction, or waiter replacement. Missing, ambiguous, tampered, or full storage fails closed. Updates require the original row ID and etag followed by a complete readback.

## Original requests and recovery

Use stable business batch/item identities. Prepare and persist the full original request, then execute its encrypted handle with the original organization credential. Keep that handle and the original ledger row for reads, polling, files, cancellation, retries, and recovery. A plain JSON receipt, changed item position, or new execution ID cannot authorize a replacement paid operation.

After a lost response, recover the original request using its unchanged body and idempotency key. Once the original job ID is known, query that job. Changing the request, model, or approved budget represents new intent and cannot mutate an existing protected plan. Cancellation applies only to an original queued video, speech, or music job.

Uploads preserve the original binary bytes/digest, allocation ID, body, and idempotency key. The signed PUT uses the original finite upload window and required content headers without the organization Authorization header or Cookie. An expired upload refuses reuse. Downloaded files become n8n-managed binary data for downstream nodes; signed URLs are not permanent file references.

## Per-task callbacks

Use the stable **production** Trigger URL in the callback credential; temporary test URLs are rejected. The existing Cuddler per-task callback contract requires a supported HTTPS URL and matching signing secret. This integration does not create or manage organization webhook subscriptions.

Run **Prepare Callback Scope** before preparing the original media request. Include its URL and non-secret scope digest in the initial request body:

```json
{
  "webhook_url": "<setup.callbackUrl>",
  "metadata": { "cuddler_callback_scope": "<setup.callbackScope>" }
}
```

The signed event must carry this exact scope field. Do not append callback fields after Prepare, change callback identity during a retry, or attach an old job to a new waiter.

The original branch and receiver branch belong to the same workflow definition and execute separately:

1. Prepare callback scope, prepare the original media request, and execute its handle.
2. Register the original Wait only after the original job ID is known. Enter the single finite native Wait only when `registered` is true; otherwise recover through the unchanged original handle.
3. Use native Wait with POST, response-node mode, one Wait stage, and a finite limit of 1–300 seconds. The example uses 90 seconds.
4. The receiver verifies exact signed bytes, timestamp, event identity, and scope; it then durably claims the event before delivering to the captured original Wait.
5. Only completed Inbox state with durable readback authorizes callback ACK 200. A successful HTTP response, a lost response, or HTTP 409 alone does not prove consumption. Pending events return 503.
6. If the original Wait was consumed and its completion step crashed, use **Recover Original Event** with the current receiver's valid encrypted delivery handle. It authenticates the original job, matches the signed terminal status, and durably records completion. It creates no new job or Wait.

Preserve the original execution and Wait map. Recovery can acknowledge original terminal-job consumption, but cannot resume an ended execution or guarantee exactly-once downstream business effects. Free original-job GETs may repeat after a lost response. An event arriving before the original map exists remains pending while the original create response is recovered.

**Keep Respond to Webhook pinned to `typeVersion: 1` for this n8n version.** Set a JSON body and `options.responseCode = {{$json.statusCode}}`. Newer versions of that stock node reject a custom Trigger ancestor in the target host. Native Wait normalization may omit its hidden response-code field and fall back to HTTP 200; that response does not authorize an event ACK. The explicit pending response remains 503 until durable Inbox completion is read back.

## Example workflow

See [the example instructions](examples/README.md) and [the inactive original-video workflow](examples/single-original-video-callback-wait.json).

The template contains placeholders and is inactive. Select existing credentials and the three distinct rows; replace model/request fields with catalog-valid values and use stable business/item identities. Replace `REPLACE_WITH_APPROVED_MAX_USD` with an explicitly approved, server-validated decimal USD string cap. The placeholder is deliberately nonnumeric and provides no spendable default. Actual generation requires separately authorized resources and budget.

Only terminal succeeded status proceeds to **Download Prepared File** using the original Execute handle. Pending, retry-later, failed, and canceled states retain the original identity and finish without a new job or Wait.

## Privacy and operational limits

The callback event projection retains only event ID/type/time, media ID/status, and scope, accompanied by encrypted receipts. It omits prompt, output URL, provider error, arbitrary metadata, balance amounts, and credentials from those callback outputs. Ordinary balance and estimate operations still return their intended results. Protected recovery returns only the data authorized for the selected original operation. Resume capabilities are encrypted before entering Data Table child parameters. HTTP failures use fixed errors rather than raw request URLs or causes.

n8n still keeps its own execution-control metadata and may parse authorized request bytes in host memory. Configure host access, execution retention, logs, and request instrumentation appropriately. This source does not claim zero tokens in host state, zero parsing, or exactly-once arbitrary workflow effects.

The linked technical receipt covers one real video task, signed HTTPS callback/original-Wait recovery and settlement. The release receipt covers the official 0.1.0 registry package, provenance, self-hosted installation and free original-job/file checks. Other media creation paths have synthetic coverage, not live generation evidence. Creator verification and actual Cloud execution remain pending; do not infer either from these receipts.

## License

[MIT](LICENSE), including the retained n8n starter attribution.

## Development and release

Pull requests, becoming ready for review, and pushes to `main` or `codex/initial-community-nodes` trigger build, lint, tests and an audit of an actual npm archive when GitHub Actions permits the run. The feature-branch trigger permits hosted checks in the contributor fork; record that result separately from canonical repository checks. These checks use synthetic HTTP/storage boundaries; they do not call a provider or validate a real organization job. Package verification uses the system BSD/GNU `tar` command.

Publication requires an explicit manual run of `publish.yml` from canonical `main`, a reviewed SHA/version and redacted real-acceptance receipt, a configured `npm-release` reviewer gate, and an existing authorized npm trusted publisher. The guard refuses forks, unprotected environments, moved commits, absent package control and npm token fallback. No push, PR or tag publishes automatically. Adding this workflow does not configure or authorize a publisher.

See [the release checklist](docs/release.md) for first-package bootstrap, publisher configuration, evidence review and the separate npm, Creator submission/approval and actual Cloud installation checks. The repository owner reviews and merges contributions; a merged PR does not publish a package.
