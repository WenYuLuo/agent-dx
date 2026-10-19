#!/usr/bin/env python3
"""Persistent public SDK client; JSON-lines commands are test orchestration only."""

import dataclasses
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import sys
import traceback

config = json.loads(Path(sys.argv[1]).read_text())
os.environ["SSL_CERT_FILE"] = config["ca"]
from adx_environment import ConnectionConfig, Environment, EnvironmentNotFound, RestartPolicy

connection = ConnectionConfig(
    server_address=config["endpoint"],
    token=Path(config["token_file"]).read_text().strip(),
    use_tls=True,
    verify_tls=True,
)
environments = {}
payload = b"adx-sdk-binary\x00\xff\n" * 10000


def check(backend):
    s = environments[backend]
    assert s.get_info().state == "running"
    result = s.commands.run("printf 'adx-sdk'; printf 'stderr-sdk' >&2; exit 7")
    assert (result.stdout, result.stderr, result.exit_code) == ("adx-sdk", "stderr-sdk", 7), repr(result)
    assert s.files.read("/workspace/" + backend + ".bin", format="bytes") == payload
    s.files.write("/workspace/" + backend + ".txt", "SDK 文件读写\n")
    assert s.files.read("/workspace/" + backend + ".txt") == "SDK 文件读写\n"
    probe = s.commands.run(
        "sha256sum /__adx/usr/local/bin/adx-execd; grep ' /__adx ' /proc/mounts; if touch /__adx/SHOULD_NOT_WRITE 2>/dev/null; then exit 9; fi; echo EROFS_READONLY_OK"
    )
    assert (
        probe.exit_code == 0 and config["execd_sha"] in probe.stdout and "EROFS_READONLY_OK" in probe.stdout
    ), repr(probe)
    return {
        "id": s.id,
        "info": dataclasses.asdict(s.get_info()),
        "binary_bytes": len(payload),
        "binary_sha256": hashlib.sha256(payload).hexdigest(),
        "guest_erofs": probe.stdout,
        "command": {"stdout": result.stdout, "stderr": result.stderr, "exit_code": result.exit_code},
    }


print(
    json.dumps(
        {
            "ready": True,
            "sdk_version": importlib.metadata.version("adx-environment"),
            "sdk_location": str(Path(sys.modules["adx_environment"].__file__).resolve()),
        }
    ),
    flush=True,
)
try:
    for line in sys.stdin:
        try:
            request = json.loads(line)
            action, backend = request["action"], request.get("backend")
            if action == "create":
                s = Environment(
                    image="acceptance:v1",
                    runtime=backend,
                    cpu=1000,
                    memory=512,
                    idle_timeout=0,
                    connection=connection,
                    create_timeout=150,
                    inherit_entrypoint=True,
                    restart_policy=RestartPolicy(
                        max_attempts=5, initial_backoff_seconds=1, max_backoff_seconds=2
                    ),
                )
                environments[backend] = s
                s.files.write("/workspace/" + backend + ".bin", payload)
                value = check(backend)
            elif action == "attach":
                for runtime, environment_id in request["ids"].items():
                    environments[runtime] = Environment.from_id(environment_id, connection=connection)
                value = {runtime: check(runtime) for runtime in environments}
            elif action == "delete_ids":
                for environment_id in request["ids"].values():
                    Environment.delete(environment_id, connection=connection)
                value = True
            elif action == "detach":
                value = {runtime: s.id for runtime, s in environments.items()}
                for s in environments.values():
                    s.close()
                environments.clear()
            elif action == "check":
                value = check(backend)
            elif action == "delete":
                s = environments[backend]
                Environment.delete(s.id, connection=connection)
                s.close()
                try:
                    remaining = Environment.from_id(s.id, connection=connection)
                except EnvironmentNotFound:
                    pass
                else:
                    remaining.close()
                    raise AssertionError("deleted Environment still exists")
                s.close()
                del environments[backend]
                value = {"id": s.id, "absent": True}
            elif action == "close":
                for s in environments.values():
                    s.kill()
                    s.close()
                environments.clear()
                value = True
            else:
                raise ValueError(action)
            print(json.dumps({"ok": True, "value": value}, ensure_ascii=False), flush=True)
        except Exception as error:
            traceback.print_exc(file=sys.stderr)
            print(json.dumps({"ok": False, "error": f"{type(error).__name__}: {error}"}), flush=True)
finally:
    for s in environments.values():
        try:
            s.kill()
        except Exception:
            traceback.print_exc(file=sys.stderr)
        s.close()
