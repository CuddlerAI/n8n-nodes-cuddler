# Release checklist

The initial source targets `n8n-nodes-cuddler` version `0.1.0` from [CuddlerAI/n8n-nodes-cuddler](https://github.com/CuddlerAI/n8n-nodes-cuddler). This repository currently has no active publication workflow. PR checks, merging to main, a public source repository and a local npm archive each represent separate steps; none publishes or certifies the package.

## Validate the release candidate

- Review the exact main commit and final package name, version, repository URL and MIT notices. Keep the n8n starter attribution.
- Run the normal build, lint, tests and `npm run check:package` against that commit. Inspect the actual archive; test fixtures and development tools must not be bundled.
- Verify this package in the target stock n8n host with authorized existing organization credentials: original request execution, response-loss recovery, original-job reads, binary upload/download, applicable queued cancellation and signed callback/Wait recovery. Confirm the original task and single settlement using protected evidence. Reuse existing tasks and free operations where possible. Local synthetic checks do not replace this step.
- Keep credentials, signed URLs, prompts and raw billing records out of repository files, CI logs and public release records. Record only the source/package identity and a redacted acceptance summary.

## Bind the publisher and provenance

The intended npm name is `n8n-nodes-cuddler`. A public registry lookup returned not found when this source was prepared; that does not establish ownership or publication rights. Use the existing authorized npm maintainer to establish control of the intended package and choose the supported first-publication procedure. Do not create a token or trusted publisher implicitly.

For GitHub-hosted trusted publishing, bind the exact canonical repository and workflow using npm's current [trusted-publisher instructions](https://docs.npmjs.com/trusted-publishers/). Review the allowed action: a newly configured publisher may be stage-only, while direct publication requires the corresponding allowed action. A [staged publication](https://docs.npmjs.com/staged-publishing/) is a real public write, and first-package staging may expose a `0.0.0-stage` package; it is not a private dry run.

Before activating a publisher workflow, restrict it to this canonical repository, an explicitly accepted immutable main commit and the appropriate protected release environment. Use minimal permissions, pinned actions, supported npm, and provenance from the same public source commit. Do not publish on a pull request, an arbitrary input SHA, a fork, or an automatic main push. No token fallback is prepared here. Recheck the official provenance and publishing instructions at release time.

## Verify the public package and n8n admission

After actual publication, verify the public name/version, archive, provenance and a clean installation from the official npm registry. Repeat original-job recovery and file delivery without creating another paid task solely for release verification.

Use the existing n8n Creator account to submit the actual npm URL and truthful author/official-representative declarations. Record submission, pending review and approval separately. After approval, install in an authorized real n8n Cloud workspace and verify node/credential loading and the documented original-job/file workflow. Neither a local scanner pass nor npm availability implies Creator approval or Cloud compatibility.
