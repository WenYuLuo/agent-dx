"""Installed SDK acceptance for a foreground request spanning the idle threshold."""

import json
from time import monotonic

from adx_environment import Environment
from functional_lifecycle import _wait_deleted
from node import catalog


def run(connection, image, output):
    environment = None
    closed = False
    deleted = False
    report = {"status": "failed", "cases": [], "cleanup_errors": []}
    try:
        environment = Environment(
            image=image,
            runtime="runc",
            cpu=500,
            memory=512,
            idle_timeout=6,
            detached=True,
            node_id="node1",
            connection=connection,
            create_timeout=150,
        )
        started = monotonic()
        command = environment.commands.run(
            "sleep 12; printf active-request-complete",
            timeout=20,
        )
        elapsed = monotonic() - started
        if not (command.exit_code == 0 and command.stdout == "active-request-complete"):
            raise AssertionError(command)
        if not (elapsed >= 10):
            raise AssertionError(f"foreground request ended too soon: {elapsed:.3f}s")
        if not (environment.is_running()):
            raise AssertionError("active request did not protect the instance from idle reclaim")
        record = json.loads(catalog()["environment:" + environment.id])
        if not (record["result"]["state"] == "Running"):
            raise AssertionError(record["result"])
        if not (record["result"]["resources_held"]):
            raise AssertionError(record["result"])
        environment.close()
        closed = True
        _wait_deleted(environment.id, connection, timeout=90)
        if not ("environment:" + environment.id not in catalog()):
            raise AssertionError()
        deleted = True
        report["cases"].append(
            {
                "id": "lifecycle.active-request-prevents-idle",
                "status": "passed",
                "seconds": round(monotonic() - started, 3),
            }
        )
        report["status"] = "passed"
    except Exception as error:
        report["error"] = str(error)
        raise
    finally:
        if environment is not None:
            if not closed:
                try:
                    environment.close()
                except Exception as error:
                    report["cleanup_errors"].append(str(error))
            if not deleted:
                try:
                    Environment.delete(environment.id, connection=connection)
                except Exception as error:
                    report["cleanup_errors"].append(str(error))
        if report["cleanup_errors"]:
            report["status"] = "failed"
        output.write_text(json.dumps(report, indent=2) + "\n")
        if report["cleanup_errors"] and "error" not in report:
            raise RuntimeError("idle activity cleanup failed")
