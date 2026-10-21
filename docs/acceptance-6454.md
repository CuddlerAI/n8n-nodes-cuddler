# n8n #6454 acceptance continuation

This continuation changes the example and offline observation tooling only. Runtime nodes, credentials, authentication, paid-request construction, and the published package version are unchanged. No package is published by these changes.

## Dynamic template

The callback example now uses native Edit Fields assignments for `request.*`, followed by a simple `$json.request` reference. It configures the stable business/item identity once and carries the original item through read/download. This avoids complex object-literal evaluation that fails with `invalid syntax` in the acceptance host's stock Set node as well as Cuddler Prepare. The expression engine and sandbox remain unchanged. Keep `max_usd` typed as a string.

Real n8n 2.43.7 free execution 121 successfully traversed Callback Scope → Configure Original Request → Prepare. Two concurrent free executions 126/127 selected the same operation for each stable item; reversing input order in execution 144 retained those identities. These are actual native Data Table/SQLite runs, not the synthetic store described below.

## Frozen shared contract observation

Use the specification, vectors, and checker together from Cuddler commit `2ac842fe0ae242f25a1fec3f97df4a048bdad263`. Do not point the runner at unreviewed files: the checker module is imported as executable code.

```sh
npm run build
node scripts/observe-cloud-recovery.cjs /path/to/frozen-contract /path/to/report.json
node /path/to/frozen-contract/scripts/platform-cloud-recovery-conformance.mjs /path/to/report.json
```

The observer runs the actual built Cuddler entry, ProtectedPlanStore, native Data Table CAS adapter, upload/file/cancel operations, and callback Trigger/Receipt nodes. It reuses the existing synthetic SDK boundary. A fake provider records job identity, one hold, provider submission and capture/release; record fields and counters come from calls and observed results. No live key, network, paid task, expected-result oracle or copied checker answer is used. Unknown/failed observations remain failures. Error codes are normalized from the deliberately injected fault plus an observed candidate rejection, because the public node intentionally redacts internal errors.

The fixture's pre-existing-job and upload cases seed state before starting observation. The report covers the requested recovery window, not that synthetic setup. The item-reorder scenario shifts the original recoverable item past an unrelated catalog item and checks its returned paired item index; the suite and actual host separately cover two stable prepared items. Source-version aliases identify the retained native blob; its actual bytes are SHA-256 checked by the candidate. Synthetic ledger/storage results do not establish production durability, supplier billing or n8n Cloud availability.

### Remaining shared-checker mismatch

All 84 scenarios execute their candidate assertions. The frozen checker still rejects the full report. Per-case diagnostic checks isolate three cases: video/speech/music `queued-cancel`.

- Frozen fixture requires exactly one query and two canceled results.
- Candidate safely performs `GET queued → POST cancel → GET canceled` when the user explicitly repeats cancellation. It returns two canceled results, makes only one cancel POST, releases once, and creates no replacement.
- Therefore the actual trace has **two queries**. Do not omit the second query, synthesize another POST or remove the safety read merely to satisfy a trace counter.

The shared-contract owner (#5811 / frc) should confirm this recovery behavior and issue a versioned fixture/checker correction permitting the second original-job GET. Until the exact frozen checker passes or the owner explicitly resolves this discrepancy, do not label the report fully conformant or close #6454.

## Existing live evidence and boundaries

The unchanged runtime candidate `57da9ae9104ac7d04f376b345e20fad760067a7e` ran one authorized Seedance 2.0 mini video. A genuine signed HTTPS callback resumed the original Wait; exact-event redelivery was deduplicated. A local stable-item placeholder omission was corrected and native retry 101 of execution 30 delivered a 646,871-byte MP4. The platform ledger showed one $0.099456 capture, release of the unused hold, $4.900544 balance and zero held. This continuation creates no additional paid generation.

Controlled host evidence and exact task IDs stay with the acceptance operator. Do not copy raw execution JSON, handles, signed URLs or credentials into this repository. Provider-reported cost is not an independent supplier-invoice audit. Actual Cloud installation and npm/Creator review belong to #6455; they are not grounds to claim this local host was Cloud. Keep the issue open pending the shared-checker resolution and final review.
