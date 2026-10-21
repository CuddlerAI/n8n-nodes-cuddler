# Release checklist

The candidate is `n8n-nodes-cuddler` version `0.1.0`, MIT, from [CuddlerAI/n8n-nodes-cuddler](https://github.com/CuddlerAI/n8n-nodes-cuddler). A PR, local tests, hosted CI, real workflow acceptance, npm publication, Creator approval and an actual Cloud execution are separate evidence layers. This checklist and `publish.yml` prepare a guarded release; they do not establish that any pending layer has passed.

## Validate the exact candidate

- Review the final canonical main SHA, package name/version, repository URL, license and retained n8n starter attribution. Record canonical hosted CI separately from a fork run. CI also supports pushes to the initial contribution branch and PR `ready_for_review`; GitHub may still require repository configuration or fork-run approval.
- Run normal lint/build/tests and `npm run check:package` against that source. The package audit inspects an actual archive, including metadata, paths and every member's bytes. Fixtures and development tools must not ship.
- In the authorized existing stock n8n host, verify the final candidate's original request execution, response-loss recovery, original-job reads, binary upload/download, applicable queued cancellation and signed callback/original-Wait recovery. Verify a consumable file and the original task's single settlement using protected platform/provider receipts. Reuse existing tasks/free operations; do not create a paid task merely for release verification or expand an existing budget.
- Write a redacted acceptance receipt binding the source SHA, package version, host/version, workflow/execution/job identity, applicable recovery/file results and settlement conclusion. Link it from a canonical GitHub issue/comment or Actions run and record its SHA-256. Keep credentials, prompts, signed URLs and raw billing records out of the receipt, workflow inputs and public logs. A link and digest alone do not prove the receipt's contents; the protected release reviewer must inspect them.

## Establish the existing npm publisher and package control

`npm whoami` tests an interactive login, not OIDC publishing permission. A registry 404 does not establish ownership of the name. Use the existing authorized npm maintainer account, securely restore its login/2FA if necessary, and confirm its identity and publication rights. This repository does not create accounts, tokens or trusted-publisher settings.

Trusted publishing is configured in an existing package's settings. Do not assume that an OIDC workflow can claim a nonexistent package name. Before dispatching the release:

1. The existing npm maintainer must resolve the first-package bootstrap with npm's supported current flow. [Staged publishing](https://docs.npmjs.com/staged-publishing/) supports new packages, requires an authorized publishing account with 2FA, and creates a **public `0.0.0-stage` placeholder**; its staged content remains unpublished until maintainer approval. Staging requires npm 11.15.0 or later. It is a real external write, not a private dry run. An ordinary initial publication is also a real public release.
2. Review the exact bootstrap artifact/version and name-control procedure before that write. Preserve `0.1.0` for the final canonical, provenance-bearing release; do not publish it locally to work around missing OIDC setup. A staged bootstrap, its approval/rejection and the final release must have distinct receipts. If the current npm flow cannot establish the package/settings without consuming the reviewed release version, resolve that with the maintainer before proceeding. This workflow intentionally refuses an absent package or an already-published version.
3. Once package control is established, have its existing maintainer open **Package → Settings → Trusted publishing**. Configure GitHub organization **`CuddlerAI`**, repository **`n8n-nodes-cuddler`**, workflow filename **`publish.yml`**, environment **`npm-release`**. The case-sensitive repository must match `package.json.repository.url`. Creating this relationship grants publication authority; prepare and review its exact fields before an authorized account holder saves it.
4. Explicitly allow **`npm publish`** for this publisher. New publisher configurations default to stage-only; `npm stage publish` permission does not allow direct publication. No stage or token fallback is wired into this workflow. Recheck any publisher expiry shown by npm and its current official documentation before dispatch.

The workflow uses GitHub-hosted Ubuntu, Node.js 24 and pinned npm 11.15.0, meeting the [trusted-publishing minimum](https://docs.npmjs.com/trusted-publishers/) of Node 22.14.0/npm 11.5.1. It requests OIDC and publishes with `--provenance` from the public canonical source. npm credentials are not stored or supplied. [Provenance](https://docs.npmjs.com/generating-provenance-statements/) proves the source/build linkage; it does not prove runtime correctness.

## Configure the canonical release gate

The canonical repository administrator must configure **`npm-release`** with at least one required reviewer. The reviewer must be able to inspect the redacted real-acceptance receipt, final source/CI and the exact publisher fields. Limit its deployment branches to main and use prevention of self-review where appropriate. No environment or reviewer settings are created by this source change.

`scripts/release-guard.mjs` reads the canonical GitHub environment and current main via fixed official API URLs using the job's read-only GitHub token. It fails closed if `npm-release` is missing, the API cannot be read, required reviewers are absent/hidden, main moved, or any source/context check differs. A new unprotected environment automatically created by a workflow reference cannot pass this guard. GitHub enforces its configured reviewer approval before the publish job starts; the guard checks that reviewer protection is actually configured.

The job has only `contents: read`, `actions: read` for environment inspection, and `id-token: write`, pinned checkout/setup actions, no dependency cache, no checkout credentials and explicit empty user/global npm configuration. It refuses project `.npmrc` and npm token/password environment fallback. Do not add a token to bypass an OIDC or API failure.

## Manually release the reviewed main commit

After real acceptance, canonical CI, package control, publisher configuration and protected reviewer setup are all ready:

1. Open canonical Actions → **Publish npm package** → **Run workflow**, selecting **main**. The workflow must first have been merged into the default branch; it cannot be dispatched from an unregistered contribution workflow.
2. Supply `expected_sha` as the reviewed lowercase 40-character SHA; `expected_version` as the exact `package.json` version; `acceptance_url` as the canonical GitHub issue/comment or Actions-run URL carrying the redacted receipt; and `acceptance_sha256` as that receipt's lowercase 64-character SHA-256. Inputs are visible in workflow metadata: never put secrets or signed links there.
3. The required environment reviewer inspects those source/acceptance/publisher facts and approves the concrete release. The automated format checks do not decide whether real acceptance passed.
4. The job checks canonical public main/workflow/runner/SHA/version identity, empty npm configuration, configured reviewers and current main. It then installs locked dependencies, runs lint/build/tests, and audits the actual archive. Immediately before `npm publish --provenance`, it repeats the guard, verifies the supported npm/OIDC environment and existing public package/unused version. A failed check stops publication. A moved main needs a newly reviewed dispatch, not an arbitrary checkout SHA.

Only manual dispatch is configured. PRs, feature branches, main pushes and tags never publish. Fork dispatches and non-main refs cannot enter the job. A workflow success is a publication result only if npm and provenance are subsequently verified.

## Verify npm, Creator and Cloud separately

- **Public npm:** verify the actual name/version, tarball/integrity and provenance subject/source/workflow against the approved main SHA. Perform a clean installation from the official registry, then repeat the original-job/file workflow with free reads. Keep the real registry and execution receipts; local packing or a successful upload is insufficient.
- **Creator scan/submission:** use the existing Creator account and actual npm URL. The official CLI scanner currently consumes a published npm package, not a local archive. After publication, inspect its actual report and require `passed: true`; an exit status of zero alone does not prove a pass. Resolve reported blockers and retain the report. Scanner acceptance is not human verification. Provide truthful author/official-representative declarations, public source/provenance and supported-feature evidence. Follow [n8n's verification process](https://docs.n8n.io/integrations/creating-nodes/deploy/submit-community-nodes/) and record submission, pending review, requests for changes and approval separately. Submit only the actual published package; do not represent a Draft PR, bootstrap placeholder or local tarball as a published integration.
- **Actual Cloud:** after n8n verification/admission and with an existing authorized Cloud workspace, install the verified public version and verify node/credential loading plus the documented original-job recovery, callback/Wait and binary-file delivery. Bind the Cloud host, version and execution receipt to that package. Approval or appearance in search does not itself prove this workflow runs in Cloud.

Leave each layer pending until its matching evidence exists. External review dates are controlled by n8n; do not infer approval or Cloud acceptance from an npm release or scanner result.
