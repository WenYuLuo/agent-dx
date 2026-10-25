# Controlled USRBIO probes

`../usrbio_pipeline_probe.rs` runs the final same-RAM comparison. It uses the
product SDK in `sdk` mode and the frozen `native.rs` client for controlled
minimum-ring modes (`wire_async`, `wire_pipeline_async` and blocking diagnostics).
The reference is derived from d24c158; it is test code for a bounded, fully
verified workload, not a second supported SDK or cancellation-safe public API.

The final runner, matrix, payload checks, source/binary identities and results
are linked from `docs/testing/2026-10-25-usrbio-self-optimization.md` at the repo
root. Use the async reference for an equivalent notification contract. Blocking
modes and same-process copy diagnostics have different boundaries.

`../entrance_probe.rs` is the preceding same-Backend FUSE/USRBIO diagnostic.
`sync_probe.c` is its preload-assisted measurement helper. `direct_hook.rs` and
`patch_build_copy.py` belong to the prior backend/sync ablation: the patch script
explicitly targets `/workspace` inside a disposable build copy and can disable
body sync there. Its numbers are not durability admission results. These helpers
are never invoked by a normal product build or benchmark mode.

`restore_baseline.py` restores the two pinned USRBIO files only in the disposable
`/workspace` copy so the frozen baseline and candidate can use the same fixture.
Record source hashes and force Cargo to observe changed inputs before exporting
binaries; verify identities by runtime semantics as well as SHA256. The original
build-identity failure and remedy are included in the evidence report.

Examples and helper scripts are test artifacts; default AFS release packaging
builds product bins only. Do not deploy patched ablation copies.
