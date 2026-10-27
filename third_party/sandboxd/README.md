# sandboxd protocol

- Repository: https://github.com/WenYuLuo/sandboxd
- Selected change: https://github.com/inclusionAI/sandboxd/pull/82
- Pinned head: `f800327459a104c04a0689940062fc9bb9e2bb9e`
- ADX builds this revision without downstream source patches. Rootfs defaults,
  bootstrap mounts and managed-file paths are adapted by the control plane to
  the published sandboxd protocol.
- Source path: `api/runtime/v1/sandbox-api.proto`
- License: Apache-2.0; source and license copied verbatim.

The pinned protocol supplies `StartRequest.start_key` and keyed Delete settlement acknowledgements for unknown Start cleanup, in addition to `StartResponse.sandbox_ip` as field 4 and includes
the Checkpoint RPC. Adxlet generates its sandboxd client from this file.
Build and E2E inputs must use this same revision, rather than an independently
maintained control-plane copy of the backend API.

## CI binary bundle

[`artifacts.json`](artifacts.json) pins the OBS bundle for
`x86_64-unknown-linux-gnu`, using the same sandboxd revision as `source.json`.
The archive contains sandboxd, sbox, runc, runc-shim, sandbox-logger, redis-cli
and their backend manifest. It contains no ADX platform or Execd binaries.

`build/e2e/download_backend.py` checks the archive byte count and SHA256, then
validates the pinned sandboxd revision, declared target, complete file set,
per-file SHA256 and each executable's ELF machine. It rejects unsafe or duplicate
tar entries and installs the directory only after verification. Download or
verification failure stops package assembly.

The bundle was built from the clean pinned sandboxd revision on native Linux
AMD64 and verified against its per-file manifest, protocol hashes, runc checksum
and Redis CLI version. Updating this dependency requires a matching source pin
and verified replacement archive pin. ARM package jobs currently exclude this
x86-only external runtime bundle.
