# Native fixed-open remote read candidate

Experimental local candidate under validation; not submitted or performance accepted.

The baseline is the frozen buffer-reuse candidate on PR95 eb23a362. Remote read
CRC remains `off`; write CRC, mTLS, 64KiB frames, demanded range size, prefetch and
concurrency settings are preserved. Other layouts/backends use their prior path.

For an eligible single native CRC-format range with an open-to-close reference,
B retains bounded fixed-version routes. A binds a first full request to its
Meta-authorized open context. Further requests carry a compact context reference
and range; source-page TTL does not expire a connected open view. The observed
transport connection is bound at setup; reconnect re-establishes authority. A pins at most
64 immutable descriptors per retained context, retaining the existing payload
budget and buffer owner. Open-context metadata stays within the existing 32MiB
cache budget; B route metadata has a 4MiB/512-entry bound.

`x-afs-open-fast: 1` is an explicit off-only capability request/acknowledgement.
An authorizer without this capability and old peers retain the prior descriptor
lane. The new lane omits CRC descriptor JSON, per-frame grant validation and the
second authorization-to-response-header rendezvous. Authorization errors remain
terminal gRPC stream errors before any body is sent. Length/order/completion and
context range limits remain enforced.

Only an error or missing context enters recovery. A compact context miss permits
at most one full-setup retry; range recovery can refresh sources for the same
version and try its replicas. Recovery never looks up a fresh inode head. Close
retires cached routes/readers; admitted owners retain their byte leases until
consumed. Authorized GC also retires weakly registered descriptor caches, so an
abandoned context cannot indefinitely retain an unlinked file's disk space. CRC off does not detect silent body corruption.

Fixed-open read handles use `FOPEN_DIRECT_IO`, so a new open/truncate updating
the shared kernel inode cannot clip an older handle's read/pread EOF. Backend
attributes use the fixed version when a handle is supplied, but actual Linux
fstat may use the shared inode and report the newer length. Per-fd fstat and mmap
snapshots are not guaranteed by this candidate. This is handle policy, not a
mount/readahead tune. The actual mount failure of an additional fstat assertion
is retained separately from read/pread EOF validation.

The common baseline/candidate counters `afs_dfs_read_checks_total{stage=...}`
count executed control operations with prebound atomic counters. They add no
CPU clocks, per-request trace journal, or perf sampler to the formal cohort.

Functional gates cover fixed-version open/close, EOF after a shorter new open,
GC pin retention/release, compact context loss through real gRPC, same-version
source failure recovery, range rejection, pinned-path replacement, and byte-lease
cleanup. Formal measurements may start only after required Linux and actual
FUSE/GC gates pass. The fixed nine slots have no replacements or extra samples.
