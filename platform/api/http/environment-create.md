# Environment create ownership

`POST /api/environment/v1/environments` returns a confirmed-running result as
JSON inside the normal response envelope, or as the final SSE event:

```json
{"environmentId":"tenant-worker","status":"running","requestId":"create-request","newlyCreated":true}
```

`newlyCreated` comes from the node's creation result. It is true only when that
logical create confirms a new instance, and false when an existing successful
Environment ID or Request ID supplies a reused result. A cached response for
the same logical request retains its original ownership bit. Recovery after a
lost response may conservatively return false. `requestId` echoes the current
request and is not evidence of creation ownership.

Clients may automatically delete during local initialization rollback only
when `newlyCreated` is exactly boolean true. Reuse, attachment, missing or
malformed ownership must preserve the remote workload and close local clients.
This rule does not restrict explicit deletion or change detached lifetimes.
