> Historical candidate06 remediation design. The 2026-10-26 unified-asynchronous candidate supersedes the algorithm-selection and legacy-continuation descriptions below; current execution and resource configuration are in [read-concurrency.md](read-concurrency.md). Original evidence remains frozen.

# Native remote read: async equivalence remediation

This is the isolated 2026-10-26 remediation of the frozen concurrency candidate. Validation and performance acceptance are pending; this document does not claim delivery or enablement. Semantic/performance reference: PR95 `b7894c4f13ca1bf161c336eca9b92f938f579c31`. Read CRC remains off, write CRC on, and TLS unchanged.

## One request, then a backlog

A FUSE callback admits an owned dispatch ticket and hands off its reply. Eligible native reads start on the existing async runtime. Metadata and network waits suspend futures without occupying dispatch workers. A sparse request acquires its peer turn and global RPC credit directly. It allocates its final reply buffer only when selected to run and sends its actual requested range; small reads remain small and original requests are never split just to create concurrency.

When the same A is busy, ready reads carry lightweight descriptors in its bounded queue. The driver acquires that peer's turn and a global RPC credit, then selects already queued, compatible same-handle/context ranges within operation, wire-byte and reply-capacity limits. It never waits for future requests to fill a group. Each peer has at most one active batch/response stream. Different peers can use the existing global RPC budget. A unary request already sent cannot accept later operations.

A uses the existing blocking pool for real positioned reads, with an independent shared disk-depth semaphore. One operation goes directly through this blocking boundary. Multiple operations use a bounded ordered read window; genuine children keep later IO progressing while the producer waits for response-stream capacity. Disk permits are released after real reads, before sending. Bodies remain shared frame slices. One existing server-streaming response carries all range headers, bodies and completions; this change adds no return channel.

B fills each selected request's own final buffer. Existing header, length, completion and stream EOF rules gate grouped publication. A fixed reply executor sends ReplyData and retires the dispatch turn. It never waits for network IO. Cancellation does not retire a dispatch lifetime while descendant metadata or blocking work still runs.

## Reuse rather than more business work

The existing version and source caches remain authoritative. Native source lookup uses a stable window of the immutable layout within the existing metadata advisory quota, so a later concurrent offset winning initialization does not omit earlier offsets and force an extra query. It changes no body range or data prefetch setting. A small bounded keyed coordination helper coalesces concurrent cold initialization; followers recheck the real cache after acquiring the key. B version/source leaders retain their originating lifetime and have an independent configured deadline, so one waiting caller timing out cannot cancel shared initialization or allow a duplicate successful cold query. A open initialization similarly holds its service credit inside the real blocking operation. Keys include immutable version/layout and existing caller/context identity. Failures are not cached as successful authority and recovery remains on the original version.

Native reply and A send pools retain initialized buffers under their existing byte limits. Borrowed plus idle backing capacity share one budget. The last shared owner returns storage; a waiting borrower uses async notification and its original deadline. Incompatible idle storage can be reclaimed before allocating another shape. Fixed-size repeated working sets reuse initialized memory; varying shapes can still allocate and zero when necessary. This is not end-to-end zero copy: B frame assembly and kernel/transport transfers remain.

The feature remains disabled by default. `fuse_workers`, `fuse_max_tasks`, RPC/pending limits, reply bytes, disk depth, group bound and timeout keep their existing names/defaults. No adaptive algorithm, prefetch or library replacement is introduced. Same-fh barriers, immutable open views, authorization, quarantine/reader invalidation, EOF and close/GC semantics are retained with their existing documented admission gaps.

## Minimal programming pattern

The reusable part is ownership across futures, not a new executor. Use the runtime's futures, semaphores, bounded queues and blocking bridge. `ReadKeyGates` is a small shared coordination helper used by real cold-cache callers; file ordering and reply publication stay domain specific.

```text
on_request(request, reply):
    ticket = dispatch.admit(request.order_key)          # immediate bounded admission
    dispatch.start_async_when_eligible(ticket, async:
        plan = await prepare_using_shared_cold_cache(request, ticket.lifetime)
        if peer_idle_and_no_backlog(plan.peer):
            result = await execute_one(plan)           # direct sparse path
        else:
            result = await bounded_peer_queue.submit(plan)  # descriptors only
        await reply_queue.send(reply, result, ticket)  # ticket still admitted
    )
    return ()                                          # callback ends; data reply is later

peer_driver(peer):
    while ready_work_exists(peer):
        peer_turn = await peer.one_stream_credit()
        rpc_credit = await global_rpc_budget.acquire()
        group = take_existing_compatible_work(peer, bounds)  # no fill timer
        buffers = await shared_pool.borrow_for(group, original_deadlines)
        result = await one_response_stream(group, buffers)
        complete_original_requests(result)             # full protocol checks

read_on_A(range, lifetime):
    body_credit = await existing_byte_budget.reserve(range.capacity)
    buffer = await bounded_initialized_pool.borrow(range.length)
    disk_credit = await global_disk_budget.acquire()
    body = await spawn_blocking(
        positioned_read(range, buffer, disk_credit, lifetime)
    )                                                  # owns credit until syscall ends
    await response_stream.send_shared_frames(body, body_credit)

reply_executor(completion):
    completion.reply.send(completion.result)
    completion.ticket.finish()                         # actual completion only
```

Error, timeout and cancellation paths must retain the same owners until genuine work ends, send at most one reply, and release credits on all exits. A semaphore bounds work; it does not provide a cache, file authority or a guarantee that dropping a future cancels a syscall.

## Evidence and interpretation

`afs_dfs_read_buffer_capacity_bytes{role,state}` reports pool-owned total, active, idle, limit and peak backing bytes. Active plus idle equals total; total peak must fit its configured pool budget. It is not additional memory to sum with the active byte-credit gauge. Legacy detached Vec ownership and transport/kernel buffers are outside this pool gauge. Allocation/zero/reuse counters describe actual buffer operations.

Read pipeline durations overlap. `source_io` measures positioned read calls rather than hardware queue depth; `send_wait` covers response production and enqueueing rather than pure network stall. Peaks are lifetime maxima, not subtractable counters. Full application open/read/EOF/close wall time is the performance result.

The frozen cohort has 48 slots: 18 serial (pure PR95, observed PR95, candidate; warm/guest-cold; three rounds) and 30 with 16 application producers (pure PR95, one-stream D1G1/D1G4/D2G4/D4G4; both caches; three rounds). The primary candidate is fixed before sampling to one stream per peer, G4/D4. Both serial and parallel must satisfy paired median candidate/pure-PR95 ratio <=1.00, every pair <=1.05, and baseline max/min <=1.10 in each cache condition. No replacement slots, posthoc winner, or performance repair round is allowed.

Functional evidence, the known baseline DataLoss/InvalidArgument assertion, original ignores, and inherited CRC-off/fstat/mmap/migration/mixed-version/recovery/stability gaps remain separate from performance acceptance. Final results are recorded in the task evidence ledger.
