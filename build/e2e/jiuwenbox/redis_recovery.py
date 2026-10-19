#!/usr/bin/env python3
"""Redis SIGKILL and sustained outage against the installed public SDK."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import signal
import time

from full_sdk import FullSDK, command


class RedisRecovery(FullSDK):
    async def scenarios(self, ids):
        for backend in ("supervisor", "conch"):
            value = await self.sdk_rpc("create", backend)
            ids[backend] = value["id"]
            value["record"] = self.record(value["id"])
            (self.run / ("create-" + backend + ".json")).write_text(json.dumps(value, indent=2))
        before = {backend: self.record(id) for backend, id in ids.items()}
        (self.run / "records-before.json").write_text(json.dumps(before, indent=2))
        await self.restart_service("redis")
        await self.until(self.node_ready, "Redis AOF replay and node admission")
        assert self.same_runtimes_ready(ids, before), "Redis crash replaced a healthy runtime"
        await self.check_all("redis-sigkill", ids)

        status = json.loads(
            command(str(self.package / "bin/adxctl"), "--config", str(self.config_path), "status")
        )
        pid = next(item["pid"] for item in status["services"] if item["id"] == "redis")
        assert Path(f"/proc/{pid}/exe").resolve() == self.package / "bin/redis-server"
        started = time.monotonic()
        os.kill(pid, signal.SIGSTOP)
        try:
            await asyncio.sleep(35)
            assert "T" in Path(f"/proc/{pid}/status").read_text().split("State:", 1)[1].splitlines()[0]
            # Existing execution state is independently journaled and must remain owned.
            live = [record for record in self.records(self.runtime_config) if record["phase"] != "Deleted"]
            assert len(live) == 2 and all(record["phase"] == "Ready" for record in live), live
            (self.run / "outage-ownership.json").write_text(json.dumps(live, indent=2))
        finally:
            os.kill(pid, signal.SIGCONT)
        (self.run / "outage.json").write_text(json.dumps({"seconds": time.monotonic() - started, "pid": pid}))
        await self.until(self.node_ready, "Redis outage recovery and node re-registration", 240)
        assert self.same_runtimes_ready(ids, before), "Redis outage replaced a healthy runtime"
        await self.check_all("redis-outage", ids)
        await self.restart_service("coordinator")
        await self.until(self.node_ready, "coordinator recovers durable catalog after Redis outage")
        assert self.same_runtimes_ready(ids, before)
        await self.check_all("redis-outage-coordinator-restart", ids)
        for backend, id in ids.items():
            await self.sdk_rpc("delete", backend)
            await self.until(lambda: self.record(id) is None, "SDK deletion committed")
        await self.until(
            lambda: all(record["phase"] == "Deleted" for record in self.records(self.runtime_config)),
            "physical deletion",
        )
        await self.audit(self.runtime_config)
        self.results.append({"case": "delete-and-native-audit", "status": "passed"})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run", required=True)
    args = parser.parse_args()
    asyncio.run(RedisRecovery(args.root.resolve(), args.run).run_all())
