# Original-video callback example

[single-original-video-callback-wait.json](single-original-video-callback-wait.json) is an inactive 19-node workflow template for the unpublished Cuddler source candidate. Importing it does not run a generation or prove runtime, provider, or n8n Cloud compatibility.

Before use:

1. Follow the [main setup instructions](../README.md) for the target n8n version, credentials, production callback URL, signing secret, and three separate native Data Table rows.
2. Select the existing credentials and rows through n8n's UI. Never paste credentials into workflow JSON.
3. Replace all model, request, and stable business/item identity placeholders with valid values. Replace `REPLACE_WITH_APPROVED_MAX_USD` with an explicitly approved decimal USD string cap accepted by the server. It is a nonnumeric placeholder, not a usable budget or default spending allowance.
4. Keep the workflow inactive until the exact package, callback reachability, existing organization resources, and real-use budget have been approved and verified.

The original branch freezes the callback URL and scope before Prepare, retains the original encrypted operation handle, and enters one finite native Wait. Poll Prepared returns the original job's narrow status. Only terminal succeeded status enters Download Original Video, which uses the unchanged Execute handle and returns native `data` binary for downstream nodes. Pending, retry-later, failed, and canceled states finish without a replacement job or Wait.

The receiver first attempts durable delivery to the original Wait. A completed event takes the acknowledgement response. A pending event uses its current encrypted Claim delivery handle for protected original-job recovery; it cannot substitute workflow JSON, create another job, or replace an ended Wait.

Keep both Respond to Webhook nodes at `typeVersion: 1` for n8n 2.43.7. Native Wait requests POST, response-node mode, and a 90-second limit. The stock host can omit its hidden response code and fall back to HTTP 200, so HTTP success or response loss is not evidence that an event was consumed. Only durable Inbox completion and readback authorizes ACK 200; explicit pending responses use 503.

The template has not performed real API/provider or billing work. Use the original persisted request and handle for retries, status reads, and files. Do not treat an imported template, local source checks, or evidence for an earlier package as acceptance of the final installed package.
