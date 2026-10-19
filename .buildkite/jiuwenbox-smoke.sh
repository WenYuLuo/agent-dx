#!/usr/bin/env bash
# Supervisor smoke runs in the existing privileged Linux CI image.
set -euo pipefail
: "${BUILDKITE_COMMIT:?Buildkite revision required}"
: "${BUILDKITE_BUILD_ID:?Buildkite build ID required}"
[[ -z $(git status --porcelain) ]] || { echo 'clean checkout required' >&2; exit 1; }
[[ $(git rev-parse HEAD) == "$BUILDKITE_COMMIT" ]] || { echo 'checkout commit mismatch' >&2; exit 1; }
mkdir -p out/buildkite/logs
# Consume this build's unified package, including its tested installed-SDK wheel.
for artifact in adx-release.tar.gz adx-release.tar.gz.sha256; do
  buildkite-agent artifact download "out/buildkite/$artifact" . --step platform-build --build "$BUILDKITE_BUILD_ID"
done
(cd out/buildkite && sha256sum --check adx-release.tar.gz.sha256)
package="$PWD/out/buildkite/jiuwenbox-package"
mkdir "$package"
tar -xzf out/buildkite/adx-release.tar.gz -C "$package"
echo '--- :package: Prepare Jiuwenbox supervisor'
if [[ $(id -u) != 0 ]]; then
  echo 'Jiuwenbox smoke requires the privileged root CI container' >&2
  exit 1
fi
if ! command -v bwrap >/dev/null || ! command -v ip >/dev/null || ! command -v openssl >/dev/null; then
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends bubblewrap iproute2 iptables busybox-static util-linux openssl
fi
# The pinned policy includes the distro-neutral CA directory.
mkdir -p /etc/pki
if [[ ! -w /sys/fs/cgroup/cgroup.subtree_control ]]; then
  mount -o remount,rw /sys/fs/cgroup
fi
backend="$PWD/out/buildkite/jiuwenbox-backend"
python3 build/e2e/jiuwenbox/prepare_supervisor.py \
  --cache "${ADX_JIUWENBOX_CACHE:-/mnt/paas/build-cache/adx/jiuwenbox}" --output "$backend"
exec "$backend/venv/bin/python" -u "$PWD/build/e2e/jiuwenbox/smoke.py" \
  --backend-root "$backend" --package "$package" --commit "$BUILDKITE_COMMIT" \
  --output "$PWD/out/buildkite/jiuwenbox"
