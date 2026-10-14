"""Repository release tags gate both Python publishers before artifact access."""

import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


class RepositoryReleaseTagTests(unittest.TestCase):
    def test_repository_tags_are_independent_of_python_package_versions(self):
        helper = ROOT / '.buildkite/release-tag.sh'
        for tag in ('v0.1.0', 'v0.1.0rc1', 'refs/tags/v0.2.0rc2', 'v1.2.3-rc.1'):
            with self.subTest(tag=tag):
                result = subprocess.run(
                    ['bash', '-c', 'source "$1"; require_repository_release_tag "$2"', 'release-test', str(helper), tag],
                    env=dict(os.environ, ADX_SDK_VERSION='0.1.0rc1', ADX_ADMIN_VERSION='0.1.0rc2'),
                    capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_invalid_tags_fail_before_either_publisher_downloads_artifacts(self):
        for package, flag in [('sdk', 'ADX_SDK_PYPI_UPLOAD'), ('admin', 'ADX_ADMIN_PYPI_UPLOAD')]:
            for tag in ('', 'refactor', 'sdk-v0.1.0rc1', 'adxadmin-v0.1.0rc2', 'vgarbage'):
                with self.subTest(package=package, tag=tag):
                    environment = dict(os.environ, BUILDKITE_COMMIT='a' * 40,
                                       BUILDKITE_BUILD_ID='release-tag-test', BUILDKITE_TAG=tag)
                    environment[flag] = '1'
                    result = subprocess.run(
                        ['bash', str(ROOT / f'.buildkite/publish-{package}-pypi.sh')],
                        env=environment, capture_output=True, text=True,
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn('repository release tag', result.stderr)
                    self.assertNotIn('buildkite-agent', result.stderr)


if __name__ == '__main__':
    unittest.main()
