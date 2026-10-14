#!/usr/bin/env bash
# Shared repository release gate; Python package versions remain independent.
require_repository_release_tag() {
  local tag=${1:-}
  tag=${tag#refs/tags/}
  local release_pattern='^v[0-9]+\.[0-9]+\.[0-9]+((a|b|rc)[0-9]+|-(alpha|beta|rc)\.[0-9]+)?$'
  [[ "$tag" =~ $release_pattern ]] || {
    echo 'publishing requires a repository release tag (vX.Y.Z or a prerelease)' >&2
    return 1
  }
}
