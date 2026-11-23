"""Generate the dbt docs site on the processing VM and save it locally: `make dbt-docs`.

One self-contained HTML file (`dbt docs generate --static`): every model, column, test and
description, and the lineage graph. Saved to data/dbt-docs/index.html (git-ignored); open it in
a browser, no server needed.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "dbt-docs" / "index.html"
GENERATE = "cd ~/railstream && make dbt ARGS='docs generate --static'"
FETCH = (
    "docker run --rm -v railstream_airflow-home:/h alpine:3.20 cat /h/dbt/target/static_index.html"
)


def main() -> None:
    env = {**dotenv_values(ROOT / ".env"), **os.environ}
    host = env.get("CORE_HOST")
    if not host:
        sys.exit("Set CORE_HOST=ubuntu@<processing-vm-public-ip> in .env")
    key = Path(env.get("CAPTURE_SSH_KEY", Path.home() / ".ssh" / "railstream_oci")).expanduser()
    ssh = ["ssh", "-i", str(key), host]
    print("generating dbt docs on the cloud VM...", flush=True)
    subprocess.run([*ssh, GENERATE], check=True, capture_output=True)
    html = subprocess.run([*ssh, FETCH], check=True, capture_output=True).stdout
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_bytes(html)
    print(f"saved {len(html) / 1e6:.1f} MB to {OUT}")


if __name__ == "__main__":
    main()
