"""Configure local-only login and launch the pinned Airflow standalone runtime."""

import json
import os
import sys
from pathlib import Path
from urllib.parse import quote


def database_url(password):
    return f"postgresql+psycopg://airflow:{quote(password, safe='')}@postgres:5432/airflow"


def main():
    if sys.argv[1:] == ["--database-url"]:
        # Airflow's _CMD configuration reads this internally; never log this output.
        print(database_url(os.environ["AIRFLOW_DB_PASSWORD"]), end="")
        return
    path = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
    path.write_text(json.dumps({"railstream": os.environ["AIRFLOW_UI_PASSWORD"]}))
    path.chmod(0o600)
    os.execvp("airflow", ["airflow", "standalone"])


if __name__ == "__main__":
    main()
