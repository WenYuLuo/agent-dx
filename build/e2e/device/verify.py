#!/usr/bin/env python3
"""Verify a real whole-device allocation through the installed public SDK."""

import argparse
import json
import re
import shlex
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--xpu", required=True, help="type:model:count (gpu or npu)")
    parser.add_argument("--node-id", help="pin to a known device-capable node")
    parser.add_argument("--device-path", required=True, help="guest character device to verify")
    parser.add_argument("--probe-command", required=True, help="guest vendor inventory command")
    parser.add_argument("--expect", required=True, help="regex required in inventory output")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    kind = args.xpu.split(":", 1)[0].lower()
    if kind not in {"gpu", "npu"}:
        parser.error("--xpu must request gpu or npu")
    if not args.device_path.startswith("/dev/"):
        parser.error("--device-path must be an absolute path under /dev")
    expected = re.compile(args.expect)
    from adx_environment import Environment, EnvironmentNotFound

    args.output.mkdir(parents=True, exist_ok=False)

    started = time.monotonic()
    report = {"case": f"whole-device-{kind}", "status": "failed", "xpu": args.xpu,
              "runtime": args.runtime, "node_id": args.node_id, "environment_id": None,
              "cleanup_error": None, "error": None}
    environment = None
    try:
        environment = Environment(
            image=args.image, runtime=args.runtime, xpu=args.xpu,
            node_id=args.node_id, cpu=1000, memory=2048,
            create_timeout=150,
        )
        report["environment_id"] = environment.id
        if not environment.is_running():
            raise AssertionError("device environment was not running after create")
        device = environment.commands.run(f"test -c {shlex.quote(args.device_path)}")
        if device.exit_code != 0:
            raise AssertionError(f"guest character device {args.device_path} is absent")
        inventory = environment.commands.run(args.probe_command)
        if inventory.exit_code != 0:
            raise AssertionError(f"device inventory command exited {inventory.exit_code}: "
                                 f"{inventory.stderr[:500]}")
        if expected.search(inventory.stdout) is None:
            raise AssertionError("device inventory did not match --expect")
        report["inventory"] = inventory.stdout[:2000]
        report["status"] = "passed"
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
    finally:
        if environment is not None:
            try:
                environment_id = environment.id
                environment.kill()
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    try:
                        remaining = Environment.from_id(environment_id)
                    except EnvironmentNotFound:
                        break
                    except RuntimeError as error:
                        if "is not running" in str(error):
                            break
                        raise
                    else:
                        remaining.close()
                    time.sleep(1)
                else:
                    raise TimeoutError("deleted device environment remained available")
            except Exception as error:
                report["cleanup_error"] = f"{type(error).__name__}: {error}"
                report["status"] = "failed"
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        (args.output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
