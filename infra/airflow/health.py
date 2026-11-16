"""Check the API and component heartbeats, not just whether the web port is open."""

import json
from urllib.request import urlopen


def main():
    with urlopen("http://127.0.0.1:8080/api/v2/monitor/health", timeout=5) as response:
        health = json.load(response)
    for name in ("metadatabase", "scheduler", "dag_processor", "triggerer"):
        if health.get(name, {}).get("status") != "healthy":
            raise SystemExit(f"{name} is not healthy")


if __name__ == "__main__":
    main()
