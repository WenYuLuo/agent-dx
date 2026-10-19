# Jiuwenbox Linux runtime acceptance

This driver exercises ADX's production Jiuwenbox RuntimeDriver and Linux host
operations against separately installed, unmodified Jiuwenbox and Conch. It
requires a dedicated root Linux KVM host and reserves the existing backends'
fixed paths `/var/run/conch`, `/etc/conch/cni/net.d`, and `/root/.jiuwenbox`.
It refuses preexisting service/state/configuration unless a previous run under
this same artifact root owns it. Use a fresh `--run` name for each attempt.

The prepared artifact root must contain:

- `jiuwenbox/`: develop `9e0569f4fd047e510f4ee4ff28c26c54aaec5066`.
- `conch/`: master `a9a756d86e4e48305c4a19cc3a63bd53ae972659`, with
  `bin/conch`, `bin/conchd`, and `build-artifacts/conch-init-initramfs.cpio.gz` built.
- `venv/`: Python 3.11+ with both packages installed from these sources.
- `bin/cloud-hypervisor`: Conch's v52.0-conch build.
- `tools/bin/virtiofsd`: version 1.13.3; `tools/bin/mkfs.erofs`: version 1.9+.
- `artifacts/vmlinux`: a guest kernel with EROFS, overlayfs, virtiofs/FUSE,
  virtio-pmem, networking, and virtio-vsock built in; x86 kernels also need PVH.
- `artifacts/execd.erofs`: the static Execd plus BusyBox artifact built with
  `build/runtime/rootfs.py`, and its source binary at
  `target/x86_64-unknown-linux-musl/release/adx-execd`.
- `target/debug/examples/jiuwenbox_acceptance`: built with
  `cargo build --locked -p adxlet --example jiuwenbox_acceptance`.

Install bubblewrap, cgroup v2, loop/EROFS support, iproute2, iptables,
bridge netfilter, and bridge/host-local CNI plugins. Existing host routes must
not overlap the fixture's `10.83.0.0/24`. Template creation pulls Ubuntu 24.04;
the resulting immutable template ID is retained in `artifacts/template-ready-<kernel-digest-prefix>`.
Execd is supplied only by the separate EROFS mount.

```sh
sudo /absolute/artifact-root/venv/bin/python -u build/e2e/jiuwenbox/accept.py \
  --root /absolute/artifact-root --run run001
```

Each backend covers create/readiness, independent EROFS host mount, guest binary
checksum and read-only access, delete, driver process SIGKILL/reopen, Jiuwenbox
SIGKILL/restart, and a real create whose successful HTTP response is delayed
beyond the caller deadline. The latter verifies that delete waits for the
late create and Execd is not started afterward. Conch additionally covers
conchd SIGKILL/restart. Fault injection is in a test UDS proxy, with external
backend source unchanged.

Per-case `audit.json` checks durable deletion, native absence, backend processes,
Execd identity, supervisor cgroups/netns/veth, owned firewall chains, and the
Execd loop attachment. After stopping services, `host-audit.json` additionally
checks Conch's pooled namespaces and bridge ports. Logs, failed attempts,
configuration, and `results.json` remain under `runs/<run>/`. Later attempts
reconcile prior owned journals before starting new cases.

This is runtime integration acceptance. It does not exercise the public SDK,
Coordinator scheduling, Gateway routing, complete adxlet process deployment,
multiple nodes, or checkpoint/restore. See the
[configuration and runtime contract](../../../docs/testing/jiuwenbox-runtime.md).

## Complete single-host deployment and public SDK

`full_sdk.py` reuses the real backend fixtures above and starts Redis,
Coordinator, adxlet with embedded Relay, API Server, and standalone Ingress
through `adxctl validate`, `render`, and `run`. Supply a coherent `package-sdk/`
assembled and verified by `build/release/package.py`,
including the pinned Redis 7.2.5 binaries, SDK wheel and independent Execd EROFS.
Install that wheel in `sdk-venv/` before running the fixture.

```sh
sudo /absolute/artifact-root/venv/bin/python -u build/e2e/jiuwenbox/full_sdk.py \
  --root /absolute/artifact-root --run sdk001
```

Use a fresh run name. The fixture reserves loopback ports 26379, 27000–27001,
27090–27092, 28444, 28888, 29444, 30443, and 30880–30881, in addition to the
backend paths above. It generates short-lived test certificates and private
API/Redis keys. Treat deployment YAML, rendered Redis configuration and keys
as private; exclude them from shared evidence bundles.

Business operations run in a separate Python process with `-I`, importing only
the installed `adx-environment` wheel. Both runtime classes cover create,
stdout/stderr/nonzero exit, binary and UTF-8 file round-trips, guest Execd
checksum, read-only bootstrap, and public deletion/absence. The fixture kills
individual adxlet, Coordinator, API Server and Ingress processes; `adxctl`
restarts them, and the same SDK handles must continue working. It also kills
Jiuwenbox and conchd, checking bounded cold restart with new execution IDs.

The test policy binds a fixture-owned host directory at `/workspace` for both
backends. Files in that volume must survive runtime replacement. This is
operator-provided storage configuration: the public SDK's `Mount` currently
supports read-only image/S3 sources, and cannot declare writable Local mounts.
The fixture does not assert that a disposable root filesystem survives.
Backend source code remains unchanged; only isolated policy/service configs
are generated. All business actions, including cleanup, use public SDK calls;
Redis and native backend reads supply independent lifecycle evidence.

`results.json`, per-restart records, `sdk-installation.json`, component logs,
`runtime/audit.json`, and `host-audit.json` remain under the run directory.
The fixture stops the ADX deployment and external backend processes after the
run. This remains single-host acceptance; it does not establish multi-node,
resource-isolation or checkpoint/restore support.


## Redis fault and host reboot acceptance

`redis_recovery.py --root <artifact-root> --run <new-name>` uses the same complete
package and public SDK to test Redis SIGKILL/AOF replay, a 35-second SIGSTOP
outage, and Coordinator restart after Redis recovery. It requires unchanged
Environment/runtime IDs, volume bytes and file hashes, followed by physical
resource cleanup. It does not delete Redis data or test loss of the AOF.

`host_recovery.py` has three explicit phases on a dedicated Linux host or VM:

```sh
<artifact-root>/venv/bin/python build/e2e/jiuwenbox/host_recovery.py prepare \
  --root <artifact-root> --run reboot001
# Reboot only that dedicated host, then reconnect to it.
<artifact-root>/venv/bin/python build/e2e/jiuwenbox/host_recovery.py check \
  --root <artifact-root> --run reboot001
<artifact-root>/venv/bin/python build/e2e/jiuwenbox/host_recovery.py cleanup \
  --root <artifact-root> --run reboot001
```

The prepare phase creates root-owned, uniquely named systemd units for ADX and
both external services, enables boot startup, creates Environments and writes
`host-state.json`. It detaches SDK clients without deletion. This fixture's
ADX unit uses SIGKILL on service interruption to retain desired state; explicit
`adxctl stop` still drains/deletes Environments. The recorded fault uses an
immediate forced OS reboot, so it does not run a graceful deployment drain.
Do not reboot a shared development host: the recorded run uses a dedicated
Linux VM with working nested KVM, hosting the actual Conch guest VM inside it.

The check phase requires a changed kernel boot ID, original Environment IDs and
assignment, new runtime IDs with one additional cold restart attempt, installed
SDK `Environment.from_id()` command/file access, preserved workspace hashes,
EROFS checks and physical deletion. On failure it retains service state and
`host-result.json` for diagnosis. The cleanup phase deletes test Environments
through public SDK calls, stops/disables/removes the recorded units, and audits
resources. No checkpoint/restore is used; this is not power-loss or disk-loss
acceptance.
