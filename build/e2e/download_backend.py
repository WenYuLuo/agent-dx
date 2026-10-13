#!/usr/bin/env python3
"""Install a checksum-pinned OBS backend bundle, publishing only verified files."""

import argparse
import json
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

from verify_backend import BACKEND_BINARIES, ROOT, sha, verify

PIN = ROOT / "third_party/sandboxd/artifacts.json"
MACHINES = {"x86_64-unknown-linux-gnu": 62, "aarch64-unknown-linux-gnu": 183}


def artifact_for(target, pin):
    source = json.loads((ROOT / "third_party/sandboxd/source.json").read_text())
    data = json.loads(Path(pin).read_text())
    if (
        data.get("schema_version") != 1
        or data.get("sandboxd_revision") != source["revision"]
    ):
        raise ValueError("backend pin revision or schema mismatch")
    artifact = data.get("artifacts", {}).get(target)
    if target not in MACHINES or not isinstance(artifact, dict):
        raise ValueError("no pinned OBS backend for target: " + target)
    url = artifact.get("url", "")
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    valid_url = (
        parsed.scheme == "https"
        and ".obs." in host
        and host.endswith(".myhuaweicloud.com")
        and not parsed.username
        and not parsed.password
        and not parsed.query
        and not parsed.fragment
    )
    size = artifact.get("bytes")
    digest = artifact.get("sha256", "")
    if (
        not valid_url
        or type(size) is not int
        or size <= 0
        or not re.fullmatch(r"[0-9a-f]{64}", digest)
    ):
        raise ValueError("invalid OBS backend artifact pin")
    return artifact


def fetch_archive(url, output):
    subprocess.run(
        [
            "curl",
            "--fail",
            "--location",
            "--silent",
            "--show-error",
            "--proto",
            "=https",
            "--proto-redir",
            "=https",
            "--retry",
            "3",
            "--connect-timeout",
            "15",
            "--max-time",
            "180",
            "--retry-max-time",
            "360",
            "--output",
            str(output),
            url,
        ],
        check=True,
    )


def unpack(archive, output):
    expected = set(BACKEND_BINARIES) | {"manifest.json"}
    with tarfile.open(archive, "r:gz") as bundle:
        members = {}
        for member in bundle.getmembers():
            if member.isdir() and member.name in (".", "./"):
                continue
            path = PurePosixPath(member.name)
            if (
                not member.isfile()
                or path.is_absolute()
                or ".." in path.parts
                or len(path.parts) != 1
                or path.name not in expected
                or path.name in members
            ):
                raise ValueError(
                    "unsafe or duplicate backend archive entry: " + member.name
                )
            members[path.name] = member
        if set(members) != expected:
            raise ValueError("backend archive file set mismatch")
        for name, member in members.items():
            with (
                bundle.extractfile(member) as source,
                (output / name).open("wb") as destination,
            ):
                shutil.copyfileobj(source, destination)
            (output / name).chmod(0o644 if name == "manifest.json" else 0o755)


def verify_elf(directory, target):
    for name in BACKEND_BINARIES:
        with (directory / name).open("rb") as binary:
            header = binary.read(20)
        if (
            len(header) != 20
            or header[:6] != b"\x7fELF\x02\x01"
            or int.from_bytes(header[18:20], "little") != MACHINES[target]
        ):
            raise ValueError("backend ELF architecture mismatch: " + name)


def install(output, target, pin=PIN):
    output = Path(output)
    if output.exists() or output.is_symlink():
        raise ValueError("backend output already exists")
    artifact = artifact_for(target, pin)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".backend-", dir=output.parent
    ) as temporary:
        staging = Path(temporary)
        archive = staging / "backend.tar.gz"
        print("Download pinned OBS backend:", artifact["url"], flush=True)
        fetch_archive(artifact["url"], archive)
        if (
            archive.stat().st_size != artifact["bytes"]
            or sha(archive) != artifact["sha256"]
        ):
            raise ValueError("backend archive integrity mismatch")
        payload = staging / "payload"
        payload.mkdir()
        unpack(archive, payload)
        verify(payload, target)
        verify_elf(payload, target)
        payload.rename(output)
    print("OBS backend installed:", output, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--pin", type=Path, default=PIN)
    args = parser.parse_args()
    install(args.directory, args.target, args.pin)
