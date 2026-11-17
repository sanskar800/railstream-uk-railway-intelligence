"""Open the processing VM's web UIs on localhost through an SSH tunnel (Ctrl+C closes it).

    uv run python scripts/tunnel.py      # host from CORE_HOST in .env (make tunnel)

Console http://localhost:8085, Spark UI 4040 (bronze) and 4041 (silver), Airflow 8086,
Lakekeeper http://localhost:8181/ui (tables, schemas, snapshots). The UIs
listen on the VM's 127.0.0.1 only; nothing is exposed to the internet (ADR-012).
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
PORTS = {8085: "Redpanda Console", 4040: "Spark UI (bronze)", 4041: "Spark UI (silver)",
         8086: "Airflow", 8181: "Lakekeeper catalog (open /ui)",
         4042: "Spark UI (maintenance, Spark Connect)"}  # fmt: skip


def main() -> None:
    env = {**dotenv_values(ROOT / ".env"), **os.environ}
    host = env.get("CORE_HOST")
    if not host:
        sys.exit("Set CORE_HOST=ubuntu@<processing-vm-public-ip> in .env")
    key = Path(env.get("CAPTURE_SSH_KEY", Path.home() / ".ssh" / "railstream_oci")).expanduser()
    forwards = [arg for port in PORTS for arg in ("-L", f"{port}:127.0.0.1:{port}")]
    print(f"Tunnel to {host} open; Ctrl+C closes it.")
    for port, name in PORTS.items():
        print(f"  http://localhost:{port}  {name}")
    with contextlib.suppress(KeyboardInterrupt):  # Ctrl+C is the normal way to close it
        subprocess.run(["ssh", "-i", str(key), "-N", "-o", "ExitOnForwardFailure=yes",
                        *forwards, host], check=False)  # fmt: skip


if __name__ == "__main__":
    main()
