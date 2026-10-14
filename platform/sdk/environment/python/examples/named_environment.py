"""Named Environment Example

Demonstrates creating a environment with a custom name and using its real ID with
the lightweight ``adx-environment`` CLI.

Prerequisites:
  - ADX_SERVER_ADDRESS and ADX_TOKEN environment variables must be set.
  - Install the SDK: pip install -e sdk/python/

Usage:
  export ADX_SERVER_ADDRESS=your-server.example.com
  export ADX_TOKEN=your-token
  python named_environment.py
"""

import subprocess
import sys

from adx_environment import Environment


def main():
    name = "my-test-environment"

    with Environment(cpu=2000, memory=4096, name=name) as sb:
        print(f"Environment created with name: {name}")
        print(f"  id: {sb.id}")

        # Verify environment is working
        result = sb.commands.run("echo hello from named environment")
        print(f"  exec result: {result.stdout.strip()}")

        # Run ``adx-environment ls`` in a subprocess to show CLI consistency
        print("\n--- adx-environment ls / output ---")
        try:
            proc = subprocess.run(
                [sys.executable, "-m", "adx_environment.cli", "ls", sb.id, "/"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            print(proc.stdout)
            if proc.returncode == 0:
                print("[OK] adx-environment CLI can access the named environment")
            else:
                print(f"[WARN] adx-environment CLI returned rc={proc.returncode}")
        except Exception as e:
            print(f"  (could not run adx-environment ls: {e})")

    print("\nEnvironment terminated.")


if __name__ == "__main__":
    main()
