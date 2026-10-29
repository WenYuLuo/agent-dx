"""Pinned source preparation without compiling Go in the tooling tests."""

import importlib.util
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("build_envd", Path(__file__).with_name("build_envd.py"))
builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(builder)
GIT = shutil.which("git")


class EnvdBuildTests(unittest.TestCase):
    def test_default_source_fetches_exact_pin_and_removes_temporary_checkout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            upstream = root / "upstream"
            upstream.mkdir()
            subprocess.run([GIT, "init", "-q", str(upstream)], check=True)
            (upstream / "content").write_text("pinned")
            subprocess.run([GIT, "-C", str(upstream), "add", "."], check=True)
            commit = [GIT, "-C", str(upstream), "-c", "user.name=fixture", "-c", "user.email=fixture@example.com",
                      "commit", "-qm", "fixture"]
            subprocess.run(commit, check=True)
            revision = builder.git_output(upstream, "rev-parse", "HEAD")
            subprocess.run([GIT, "-C", str(upstream), "branch", "pinned", revision], check=True)
            (upstream / "content").write_text("newer")
            subprocess.run([GIT, "-C", str(upstream), "add", "."], check=True)
            subprocess.run(commit, check=True)
            pin = root / "third_party/e2b/source.json"
            pin.parent.mkdir(parents=True)
            pin.write_text(json.dumps({"repository": str(upstream), "revision": revision}))
            observed = []

            def compile_source(source, output, jobs):
                observed.append(source)
                self.assertEqual(builder.git_output(source, "rev-parse", "HEAD"), revision)
                self.assertEqual(builder.git_output(source, "status", "--porcelain"), "")
                self.assertEqual((source / "content").read_text(), "pinned")
                self.assertEqual((output, jobs), (root / "output", 2))

            with patch.object(builder, "ROOT", root), patch.object(builder, "_build", compile_source):
                builder.build(None, root / "output", 2)
            self.assertEqual(len(observed), 1)
            self.assertFalse(observed[0].exists())

    def test_explicit_source_is_not_fetched_or_modified(self):
        source, output = Path("/explicit/source"), Path("/output")
        with patch.object(builder, "_build") as compile_source, patch.object(builder.subprocess, "run") as run:
            builder.build(source, output, 3)
        compile_source.assert_called_once_with(source, output, 3)
        run.assert_not_called()

    def test_wrong_revision_or_dirty_override_is_rejected_before_go(self):
        pin = json.loads((builder.ROOT / "third_party/e2b/source.json").read_text())
        for revision, dirty in (("wrong", ""), (pin["revision"], " M content")):
            with self.subTest(revision=revision, dirty=dirty), \
                    patch.object(builder.platform, "system", return_value="Linux"), \
                    patch.object(builder, "git_output", side_effect=[revision, dirty]), \
                    patch.object(builder.subprocess, "run") as run:
                with self.assertRaisesRegex(ValueError, "clean pinned revision"):
                    builder.build(Path("/source"), Path("/output"))
                run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
