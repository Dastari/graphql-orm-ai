# Attachment Intake

Attachment bytes live in a deployment-selected `graphql-orm-storage`
`BlobStore`; AI-owned ORM rows hold lifecycle metadata. The crate does not add
a generic GraphQL upload/download endpoint, expose a blob key, or let a model
choose a storage path.

## Intake sequence

1. An authenticated owner calls `createAiAttachmentUpload` for an active
   session with a display filename, optional declared MIME, and exact expected
   byte count.
2. The service sanitizes the display filename, generates a random scope-bound
   quarantine key and a high-entropy expiring token, stores only the token's
   SHA-256, and commits a protected `attachment_upload_created` cursor event.
3. A host-owned authenticated streaming HTTP handler receives the body. It
   passes the current `AuthPrincipal`, attachment ID, token as `SecretString`,
   and `StorageByteStream` to `AiAttachmentUploadService::upload`.
4. The service atomically consumes the one-use token, writes under a
   create-if-absent quarantine key, and compares exact expected/stored size and
   lowercase SHA-256.
5. The configured `AiAttachmentScanner` consumes a fresh complete object stream
   and attests detected MIME, bytes, hash, scanner version, and clean/reject
   verdict. Truncation, timeout, unsupported parsing, or unavailable signatures
   must fail closed.
6. A clean report passes through the separate
   `AiAttachmentAcceptancePolicy`. This host policy can narrow accepted MIME,
   size, scope, and principal; it cannot override a scanner rejection or the
   deployment hard size cap.
7. Only accepted bytes are copied to a new random final key. The quarantine
   object is deleted before metadata becomes `ready`.
8. The owner calls `finalizeAiAttachmentUpload`. A transaction rechecks owner,
   session and scope policy, changes the object to `released`, and commits the
   protected `attachment_released` event.
9. `sendAiMessage` may link only a released, clean, same-owner, same-session
   attachment. The normal message mutation remains authoritative.

Ticket possession never substitutes for current authentication. Put the token
in a protected request header, not a URL; do not log or persist it. A wrong
token does not consume the ticket. A successful claim consumes it before bytes
cross storage, preventing concurrent reuse.

## Host wiring

Construct `OrmAiAttachmentService` with:

- the same ORM database and ordinary `AiAccessPolicy` as sessions;
- the scope content-protection resolver and protector;
- an exact reviewed `BlobStore` implementation;
- a full-object malware/content scanner;
- a fail-closed post-scan acceptance policy; and
- a trusted `agql-auth::Clock`.

Install one clone as `Arc<dyn AiAttachmentService>` in the GraphQL schema and
use the same service through `AiAttachmentUploadService` in the streaming
handler. Run that service as `AiAttachmentCleanupService` from a trusted
host-owned scheduler; it is intentionally not a GraphQL operation. If
provider-persistent artifacts are enabled, install the reviewed exact-reference
boundary with `with_provider_file_deletion_service`. For OpenAI artifacts,
`OpenAiFileDeletionService::new` binds one exact logical profile to the fixed
official Files endpoint and its just-in-time secret/configuration. A host with
multiple profiles or providers must supply a reviewed router that preserves
that exact binding. Deployment
hard defaults are 25 MiB, 255 UTF-8 filename bytes, a ten-minute ticket, and a
one-hour uninterrupted processing lease. Cleanup defaults to 50 rows and a
five-minute claim lease. Validated limits may be stricter or up to the
documented hard maxima. Scope/user quota configuration remains a future
authenticated GraphQL policy surface and cannot widen deployment limits.

Compose `AiAttachmentQueryRoot` and `AiAttachmentMutationRoot`. They provide
bounded keyset metadata, ticket creation, clean release, and unlinked removal.
The complete configured naming convention, including PascalCase, is applied to
roots, arguments, inputs, outputs, and ticket fields.

Large bytes do not pass through GraphQL JSON. Ordinary views exclude blob keys,
token hashes, checksums, scanner versions, policy versions, and provider file
references. Filename and MIME metadata can still be sensitive and attachment
session events are protected before commit.

## States and failure behavior

`pending_upload` becomes `uploading` only after exact owner/token validation.
A clean accepted upload becomes `ready`, and explicit GraphQL finalization
becomes `released`. Scanner or policy denial becomes `rejected`; an unavailable
external boundary becomes `failed`. Neither state is eligible for message
linkage.

Removal first marks an unlinked object `deleting`, deletes its blob, then
commits `deleted` plus a protected cursor event. A storage failure leaves the
closed `deleting` state for future cleanup rather than falsely claiming the
object is gone. Once a message links an attachment, this mutation refuses to
remove it and break transcript integrity.

The bounded cleanup worker processes deleting-session artifact claims before
parent attachment candidates, then selects only durable `pending_upload`,
`uploading`, `deleting`, or cleanup states. It reloads and CAS-claims each row
with a new generation and expiring lease before touching any exact stored
reference.
Deletion is treated as successful only when absence is confirmed. Ambiguous
storage errors retain the references, append a redacted failed audit fact, and
enter capped retry backoff. A worker crash leaves a reclaimable lease; an old
worker cannot finalize after another generation wins. Expired tickets are
soft-deleted, while interrupted uploads remain failed metadata for owner
inspection. The worker never lists a prefix or reads attachment content.

An artifact claim independently re-proves its exact parent, deleting session,
current retention policy, and deletion cutoff. It confirms an exact local blob
is absent before invoking an optional provider boundary. Provider expiry is not
absence. `AiProviderFileDeletionService::delete_and_confirm_absent` may return
success only after authoritative absence of the exact opaque reference; a
missing service or ambiguous response retains the local/provider references
and protected derivative for retry. Successful cleanup clears those values and
writes a tombstone. Session retention must physically delete that artifact row
before it can request cleanup of the parent attachment or scrub the linked
message.

The native OpenAI boundary accepts only an exact `provider_file` artifact bound
to its configured OpenAI profile and a validated `file-...` identifier. It
sends the official [delete file](https://developers.openai.com/api/reference/resources/files/methods/delete)
request, validates the exact `{id, object: "file", deleted: true}`
acknowledgement, then uses [retrieve file](https://developers.openai.com/api/reference/resources/files/methods/retrieve)
for the same ID and requires not found. An initial exact delete not-found is an
idempotent absence proof. Redirects, another family/profile, unexpected success
shapes, oversized responses, and non-not-found retrieval results fail closed.
The adapter never lists, uploads, searches, or reads file content.

Restore reconciliation must keep runtime startup closed until AI migrations
are applied. Nullable legacy `uploading` rows fall back to their expired ticket
deadline, and legacy `deleting` rows with no processing deadline are eligible
for fenced cleanup. Do not manually mutate rows or list/delete arbitrary
storage prefixes from application code.

## Provider boundary

Release is not provider egress permission. Build each
`ModelInputBlock::Attachment` from the released row's opaque ID, detected MIME,
exact raw byte count, and lowercase SHA-256. Its separate `ImageAnalysis` or
`ProviderFile` manifest must contain the canonical user-provided source from
`ModelInputBlock::attachment_egress_reference`. The inference and capability
manifests must cover the full estimated request, including Base64 expansion.

Install the attachment service on `AiProviderCallExecutor` with
`with_attachment_resolver` and deployment-owned
`AiProviderAttachmentResolutionLimits`. The executor first obtains atomic
budget and audited exact egress proofs, then supplies a freshly resolved
principal to `AiProviderAttachmentResolver`. `OrmAiAttachmentService` reopens
only a current same-owner, same-session, message-linked, released, clean,
complete object. It verifies object metadata, bounded stream length and
SHA-256, reloads the row after storage I/O, and fails if the row or object facts
changed. The executor rehydrates and reauthorizes again after that potentially
slow read and before marking budget capacity uncertain for transport.

`AiResolvedProviderAttachment` redacts bytes from `Debug` and validates its
request binding, but is not authorization proof by itself. Do not construct it
from a caller-provided URL/key, persist it, cache it between turns, or expose it
through GraphQL. `ProviderRequestContext` requires exact one-to-one coverage,
so missing, duplicate, swapped, or extra content fails closed.

The native OpenAI adapter sends supported image inputs as inline Responses
`input_image` data URLs and other accepted files as inline `input_file` data.
This follows OpenAI's [image input](https://developers.openai.com/api/docs/guides/images-vision)
and [file input](https://developers.openai.com/api/docs/guides/file-inputs)
formats while avoiding provider-persistent file IDs. PNG, JPEG, WEBP, and GIF
are the adapter's accepted image MIME types; host scanning/acceptance must
reject animated GIFs. Direct files must be under 50 MiB each, and all inline
image/file content must be no more than 50 MiB combined. The executor's safer
default is at most eight attachments, 25 MiB each, and 50 MiB total. Hosts
should normally narrow those limits and MIME acceptance. Inline input creates
no provider-side delete obligation. The native OpenAI cleanup adapter can
safely retire exact profile-bound references created by a host-owned
persistent-file lifecycle, but this crate still does not upload, search, or
otherwise create provider file objects.

The separate [provider-persistent file contract](provider-files.md) defines the
required upload, index, logical-use, cost, cleanup, and restore evidence. Until
that complete graph exists, `ModelBuiltinTool::FileSearch` is rejected and a
raw provider store ID cannot be adopted as authority.
