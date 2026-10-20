"""The immutable runtime payload must be traversable by a non-root workload."""
import importlib.util
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("runtime_rootfs", Path(__file__).parents[1] / "rootfs.py")
ROOTFS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ROOTFS)


class RuntimePayloadPermissions(unittest.TestCase):
    def test_non_root_can_traverse_payload_to_execd(self):
        observed = []

        def inspect_image_input(command, **_kwargs):
            if command[0] == "mkfs.erofs":
                root = Path(command[-1])
                # EROFS copies these modes into /__adx; root-owned 0700 blocks
                # every non-root execution even when adx-execd itself is 0755.
                for path in (root, root / "usr", root / "usr/local", root / "usr/local/bin"):
                    observed.append(stat.S_IMODE(path.stat().st_mode) & stat.S_IXOTH)

        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            binary = base / "execd"
            binary.write_bytes(b"runtime")
            busybox = base / "busybox"
            busybox.write_bytes(b"busybox")
            with (
                patch.object(ROOTFS, "static_executable"),
                patch.object(ROOTFS, "_executable", side_effect=lambda name: name),
                patch.object(ROOTFS.subprocess, "check_output", return_value="sh\n"),
                patch.object(ROOTFS.subprocess, "run", side_effect=inspect_image_input),
            ):
                ROOTFS.build(binary, base / "runtime.img", busybox=busybox)
        self.assertTrue(observed)
        self.assertTrue(all(observed), "runtime payload denies traversal to non-root Execd")


if __name__ == "__main__":
    unittest.main()
