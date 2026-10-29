# Ordinary DFS FUSE write admission

Ordinary explicit-offset writes to a local-owner native DFS file return after the
mount owns the entire request in its bounded input queue. This is the normal
behavior, with no enable/disable switch or experimental environment input.
`write()` success means memory ownership, not persistence. A process or host crash
can lose acknowledged writes which have not completed a successful sync.

One mount owns at most 2080MiB of payload and 4096 waiting requests: 2GiB is normal admitted work, and an additional 32MiB is reserved for owned requests awaiting admission. The reserve never reduces the normal admission window. Executing
requests and failed retained bodies remain charged. Normal capacity backpressures the application by retaining its owned input and withholding the reply, while returning the receive callback. Exhausting the total body/entry bound returns EAGAIN before input ownership or success; allocation failure returns ENOMEM. When failed retained bodies prevent
capacity recovery, an unrelated admission returns EAGAIN rather than blocking the
callback that must sync or close those bodies. Allocator metadata and downstream
DFS buffers are additional memory. The total limit is an internal constant, not a deployment setting. Promotion retains the same Vec, with no additional payload copy. No per-file thread or 2GiB up-front allocation is introduced.

The FIFO body consumer preserves write order, including overlapping writes and
writes through multiple handles. It calls the existing complete DFS backend,
including write CRC, replication, catalog and Meta work. File callbacks fence only
the addressed inode; cross-file copy acquires both inode gates as one sorted set.
Same-inode dispatcher turns include deferred read completion across handles.
Namespace callbacks retain a conservative mount-wide barrier. Gates survive the
real dispatched operation, rather than just its enqueue. Fixed local replies
retain validation without acquiring these body/dispatcher barriers.

Fsync/flush/release register parked recovery continuations outside normal body
and dispatcher admission. A file prefix settles with success or a recorded error;
it need not become clean before recovery can observe failure. A continuation holds
its file gate through actual backend completion. Ordinary control admission has a
4096-operation bound; RELEASE lifecycle capacity belongs to already-existing FUSE
handles and is not rejected by that ordinary bound. Ready continuations reuse
existing workers; no per-request thread is allocated and workers do not wait for
front-queue predecessors. Shared worker/disk service and the single body worker
can still queue unrelated work; no backend write concurrency is added here.

When no write bodies are pending, ordinary reads retain the existing concurrent
read dispatcher. Append, O_SYNC/O_DSYNC, remote-owner writes, kernel writeback and
OwnerFS retain their previous reply points; append/sync requests drain preceding
ordinary writes first. Runtime request flags are checked in addition to the open
handle capability.

A failed job retains its body and short-write progress. Later writes of the same
inode observe the error; another inode can continue. An explicit fsync/flush/close
retries that inode's retained work once, then executes the original backend sync
or close. Failure is not silently converted into success. Failed close still
releases its fd and retained bodies; other opens retain the inode error until a
successful explicit retry or their last close. Successful fsync/close keep the
existing persistence and required-replica guarantees. These guarantees require
checking the syscall result; unsynced write success alone is insufficient.

Fixed local replies do not wait for queued bodies, consume errors or trigger
retry. DFS's exact `security.capability` query keeps its inode validation and
original EOPNOTSUPP. Other xattrs and other backends retain their existing checks.
Unsupported locks/access and pure parameter rejections keep their old responses.
The USRBIO fd-control path is preserved and fenced before transfer, rather than
being disabled for the mount. Unmount drains runnable work, reports retained
errors, releases all captured bodies and joins the queue worker.

The frontend body queue remains one FIFO with one body consumer. This change
adds neither per-file body queues nor round-robin scheduling. Removing unrelated
callback barriers can change when requests arrive; small-file completion is not
guaranteed to improve. Record all file endpoints and the whole workload end,
rather than treating one endpoint's latency as a complete throughput judgment.

The package excludes zero-copy ownership redesigns, inline write shortcuts,
region parallelism, object-size changes, O_DIRECT/huge-page experiments,
persistence-barrier reductions and multi-host sharding. The per-mount 2GiB normal
limit is charged on demand and is not an up-front allocation or a bound for all
process memory. Downstream buffers, allocator metadata and concurrent mounts add
to the memory requirement. The additional 32MiB reserve never reduces normal
admission. No enable/disable or queue-size configuration is introduced.

Historical cohorts, exact validation commands and final comparison results are
recorded in `afs-write-early-reply-20261029.md`. Historical 2GiB private-pipeline
results and different workload definitions must not be combined into one gain.
Local Linux/FUSE validation does not certify remote CI, all deployment modes or
long-running failure recovery.
