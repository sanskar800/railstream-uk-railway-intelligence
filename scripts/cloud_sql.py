"""Run SQL on the cloud lakehouse from the laptop: `make sql Q="SELECT ..."`.

The query travels over SSH on stdin (no shell quoting to break), runs in a short-lived Spark
container on the processing VM (`streaming.sql`), and the table prints here. Tables live on the
VM; nothing is copied to the laptop.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
REMOTE = (
    "cd ~/railstream && docker compose --env-file .env -f infra/compose/compose.yaml "
    "--profile stream run --rm --no-deps -T -e SPARK_DRIVER_MEMORY=768m spark-bronze "
    "python -m streaming.sql"
)


def main() -> None:
    query = (" ".join(sys.argv[1:]) or os.environ.get("Q", "")).strip()
    if not query:
        sys.exit('Usage: make sql Q="SELECT count(*) FROM silver.trust_movement"')
    env = {**dotenv_values(ROOT / ".env"), **os.environ}
    host = env.get("CORE_HOST")
    if not host:
        sys.exit("Set CORE_HOST=ubuntu@<processing-vm-public-ip> in .env")
    key = Path(env.get("CAPTURE_SSH_KEY", Path.home() / ".ssh" / "railstream_oci")).expanduser()
    print("running on the cloud VM (Spark starts in ~15 s)...", flush=True)
    result = subprocess.run(
        ["ssh", "-i", str(key), host, REMOTE], input=query, text=True, capture_output=True
    )
    noise = ("WARN", "SLF4J", "log4j", "setLogLevel", "Ivy")
    for line in result.stdout.splitlines():
        if line.strip() and not line.startswith(noise):
            print(line)
    if result.returncode != 0:
        errors = [ln for ln in result.stderr.splitlines() if "Exception" in ln or "Error" in ln]
        sys.exit("\n".join(errors[-5:]) or f"failed (exit {result.returncode})")


if __name__ == "__main__":
    main()
