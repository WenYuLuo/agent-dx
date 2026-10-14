#!/usr/bin/env bash
# Invoked by source-gate after the pinned worker image bootstrap.
set -euo pipefail

mkdir -p out/buildkite/logs
echo "--- :books: Public API contract gate"
python3 -m venv --system-site-packages out/buildkite/docs-venv
out/buildkite/docs-venv/bin/python -m pip install -r build/docs/requirements.txt \
  2>&1 | tee out/buildkite/logs/docs-dependencies.log
out/buildkite/docs-venv/bin/python build/docs/openapi_check.py \
  --output out/buildkite/openapi-check.json \
  2>&1 | tee out/buildkite/logs/openapi-check.log
out/buildkite/docs-venv/bin/python -u -m unittest discover -s build/docs/tests -v \
  2>&1 | tee out/buildkite/logs/openapi-tests.log
python3 build/docs/check.py --output out/buildkite/docs-check.json \
  2>&1 | tee out/buildkite/logs/docs-check.log
