"""Copy completed raw hour files from the capture VM to the local data/raw/trust.

Only compressed (completed) hours are copied, and only those not already present, so it is
safe to run repeatedly. The open hour (plain .jsonl) is left on the VM until it completes.

    uv run python scripts/pull_raw.py            # host from CAPTURE_HOST in .env
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
LOCAL = ROOT / "data" / "raw" / "trust"
REMOTE = "railstream/data/raw/trust"  # relative to the VM user's home


def main() -> None:
    env = {**dotenv_values(ROOT / ".env"), **os.environ}
    host = env.get("CAPTURE_HOST")
    if not host:
        sys.exit("Set CAPTURE_HOST=ubuntu@<public-ip> in .env")
    key = Path(env.get("CAPTURE_SSH_KEY", Path.home() / ".ssh" / "railstream_oci")).expanduser()
    ssh = ["ssh", "-i", str(key), "-o", "StrictHostKeyChecking=accept-new", host]

    listing = subprocess.run(
        [*ssh, f"cd {REMOTE} && find . -name 'hour=*.jsonl.gz' | sort"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    missing = [rel.removeprefix("./") for rel in listing if not (LOCAL / rel).exists()]
    print(f"{len(listing)} completed hours on the VM, {len(missing)} new")

    for rel in missing:
        dest = LOCAL / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".part")  # never leave a half-copied .gz behind
        subprocess.run(
            ["scp", "-q", "-i", str(key), f"{host}:{REMOTE}/{rel}", str(tmp)], check=True
        )
        tmp.replace(dest)
        print(f"  {rel}")


if __name__ == "__main__":
    main()
