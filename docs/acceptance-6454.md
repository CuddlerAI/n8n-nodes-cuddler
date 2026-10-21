# n8n #6454 acceptance continuation

This continuation changes the example and offline observation tooling only. Runtime nodes, credentials, authentication, paid-request construction, and the published package version are unchanged. No package is published by these changes.

## Dynamic template

The callback example now uses native Edit Fields assignments for `request.*`, followed by a simple `$json.request` reference. It configures the stable business/item identity once and carries the original item through read/download. This avoids complex object-literal evaluation that fails with `invalid syntax` in the acceptance host's stock Set node as well as Cuddler Prepare. The expression engine and sandbox remain unchanged. Keep `max_usd` typed as a string.

Real n8n 2.43.7 free execution 121 successfully traversed Callback Scope → Configure Original Request → Prepare. Two concurrent free executions 126/127 selected the same operation for each stable item; reversing input order in execution 144 retained those identities. These are actual native Data Table/SQLite runs, not the synthetic store described below.

## Frozen shared contract observation

Use the specification, vectors, and checker together from corrected Cuddler candidate commit `e8d191a1d6963701c436e94874bdcd6420c5a196` (pending shared-repository review/merge). The historical frozen baseline is `2ac842fe0ae242f25a1fec3f97df4a048bdad263`. Do not point the runner at unreviewed files: the checker module is imported as executable code.

```sh
npm run build
node scripts/observe-cloud-recovery.cjs /path/to/frozen-contract /path/to/report.json
node /path/to/frozen-contract/scripts/platform-cloud-recovery-conformance.mjs /path/to/report.json
```

The observer runs the actual built Cuddler entry, ProtectedPlanStore, native Data Table CAS adapter, upload/file/cancel operations, and callback Trigger/Receipt nodes. It reuses the existing synthetic SDK boundary. A fake provider records job identity, one hold, provider submission and capture/release; record fields and counters come from calls and observed results. No live key, network, paid task, expected-result oracle or copied checker answer is used. Unknown/failed observations remain failures. Error codes are normalized from the deliberately injected fault plus an observed candidate rejection, because the public node intentionally redacts internal errors.

The fixture's pre-existing-job and upload cases seed state before starting observation. The report covers the requested recovery window, not that synthetic setup. The item-reorder scenario shifts the original recoverable item past an unrelated catalog item and checks its returned paired item index; the suite and actual host separately cover two stable prepared items. Source-version aliases identify the retained native blob; its actual bytes are SHA-256 checked by the candidate. Synthetic ledger/storage results do not establish production durability, supplier billing or n8n Cloud availability.

### Repeated cancellation and corrected baseline

All 84 scenarios execute their candidate assertions and pass the corrected checker. The historical frozen checker rejected the full report: video/speech/music `queued-cancel` expected exactly one query, while the candidate performs `GET queued → POST cancel → GET canceled` across two explicit cancellation operations.

The correction permits 1–2 queries only in those three scenarios. Other query counts remain exact; two canceled results, one original job, one hold/release and zero upstream submissions/captures remain required. The observer additionally asserts exactly one cancel POST and one server job. The shared checker has 15 passing regressions, including a third/zero query, a replacement job, extra creation, duplicate release/cancel result, and extra provider submission. No extra owner-confirmation step is needed; the versioned correction still requires normal code review and merge.

Retain the old failed report and regenerate observations against all three normative files from the corrected commit, with its new fixture digest. Passing the corrected candidate does not claim that the old frozen checker passed, or that an unmerged baseline is already the shared default.

## Existing live evidence and boundaries

The unchanged runtime candidate `57da9ae9104ac7d04f376b345e20fad760067a7e` ran one authorized Seedance 2.0 mini video. A genuine signed HTTPS callback resumed the original Wait; exact-event redelivery was deduplicated. A local stable-item placeholder omission was corrected and native retry 101 of execution 30 delivered a 646,871-byte MP4. The platform ledger showed one $0.099456 capture, release of the unused hold, $4.900544 balance and zero held. This continuation creates no additional paid generation.

Controlled host evidence and exact task IDs stay with the acceptance operator. Do not copy raw execution JSON, handles, signed URLs or credentials into this repository. Provider-reported cost is not an independent supplier-invoice audit. Actual Cloud installation and npm/Creator review belong to #6455; they are not grounds to claim this local host was Cloud. Keep the issue open until the shared correction and final n8n candidate finish review and integration. No npm publication or actual n8n Cloud acceptance is claimed.
