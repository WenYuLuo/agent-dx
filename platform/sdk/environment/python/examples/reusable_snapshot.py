"""Create a reusable Snapshot and restore an independent clone."""

from adx_environment import Environment


def main() -> None:
    source = Environment(name="snapshot-source")
    clone = None
    snapshot = None
    try:
        source.files.write("/tmp/marker", "from reusable snapshot")
        snapshot = source.create_snapshot(name="python-ready")
        clone = Environment.create(snapshot, name="snapshot-clone")
        if not (clone.files.read("/tmp/marker") == "from reusable snapshot"):
            raise AssertionError()
        print(f"Restored {clone.id} from {snapshot.snapshot_id}")
    finally:
        if clone is not None:
            clone.kill()
        source.kill()
        if snapshot is not None:
            Environment.delete_snapshot(snapshot.snapshot_id)


if __name__ == "__main__":
    main()
