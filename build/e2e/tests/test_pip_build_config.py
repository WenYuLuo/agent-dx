"""Configured indexes reach Docker builds without entering the image context."""
import configparser
from pathlib import Path
import stat
import unittest

from e2e.prepare import _pip_build_secret


class PipBuildConfigTests(unittest.TestCase):
    def test_configured_index_is_a_private_temporary_build_secret(self):
        environment = {
            "PIP_INDEX_URL": "https://test-user:fake-pass@packages.invalid/simple",
            "PIP_EXTRA_INDEX_URL": "https://extra.invalid/simple",
            "PIP_DEFAULT_TIMEOUT": "120",
        }
        with _pip_build_secret(environment) as arguments:
            self.assertEqual(arguments[0], "--secret")
            self.assertNotIn("fake-pass", " ".join(arguments))
            self.assertTrue(arguments[1].startswith("id=adx-pip-config,src="))
            path = Path(arguments[1].split("src=", 1)[1])
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            config = configparser.ConfigParser(interpolation=None)
            config.read(path)
            self.assertEqual(config["global"]["index-url"], environment["PIP_INDEX_URL"])
            self.assertEqual(config["global"]["extra-index-url"], environment["PIP_EXTRA_INDEX_URL"])
            self.assertEqual(config["global"]["timeout"], "120")
        self.assertFalse(path.exists())

    def test_no_override_keeps_the_base_image_configuration(self):
        with _pip_build_secret({"UNRELATED": "value"}) as arguments:
            self.assertEqual(arguments, [])

    def test_failure_removes_the_secret_file(self):
        path = None
        with self.assertRaisesRegex(RuntimeError, "failed"):
            with _pip_build_secret({"PIP_INDEX_URL": "https://packages.invalid/simple"}) as arguments:
                path = Path(arguments[1].split("src=", 1)[1])
                raise RuntimeError("failed")
        self.assertFalse(path.exists())

    def test_multiline_values_cannot_inject_pip_options(self):
        for value in ("https://packages.invalid\ntrusted-host = anything", "https://packages.invalid\r"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                with _pip_build_secret({"PIP_INDEX_URL": value}):
                    self.fail("invalid configuration accepted")
