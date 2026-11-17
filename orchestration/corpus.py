"""CORPUS download/validation and append-only, version-idempotent reference loading."""

import base64
import gzip
import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from trust.locations import read_corpus

CORPUS_URL = (
    "https://publicdatafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=CORPUS"
)
MAX_DOWNLOAD_BYTES = 16 * 1024 * 1024


class ReferenceRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        destination = urlsplit(newurl)
        if destination.scheme != "https":
            raise ValueError("Reference redirect requires HTTPS")
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if destination.netloc.lower() != urlsplit(req.full_url).netloc.lower():
            redirected.remove_header("Authorization")
        return redirected


def download_corpus(directory: Path, username: str, password: str, *, opener=None) -> str:
    if not username or not password:
        raise ValueError("Set NR_USERNAME and NR_PASSWORD in the private .env file")
    opener = opener or build_opener(ReferenceRedirect())
    encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
    request = Request(CORPUS_URL, headers={"Authorization": f"Basic {encoded}"})
    # Do not include exception URLs: a redirect may contain a private signed URL.
    try:
        with opener.open(request, timeout=60) as response:
            payload = response.read(MAX_DOWNLOAD_BYTES + 1)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(f"CORPUS download failed ({type(exc).__name__})") from None
    if not payload or len(payload) > MAX_DOWNLOAD_BYTES:
        raise ValueError("CORPUS download is empty or exceeds the download limit")
    # The source may return gzip or plain JSON. The saved format is always gzip.
    if not payload.startswith(b"\x1f\x8b"):
        payload = gzip.compress(payload, mtime=0)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "CORPUSExtract.json.gz"
    temporary = path.with_suffix(".part")
    with temporary.open("wb") as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
    return str(path)


def validate_corpus(path: str) -> dict:
    version, rows = read_corpus(Path(path))
    return {"path": path, "reference_version": version, "locations": len(rows)}


def run_directory(run_id: str) -> Path:
    # A run ID is not a filesystem path, and never contains source credentials.
    key = hashlib.sha256(run_id.encode()).hexdigest()[:24]
    return Path(os.environ.get("REFERENCE_ROOT", "/opt/airflow/reference")) / "corpus" / key


def load_corpus(manifest: dict, *, table_name="ref.locations") -> dict:
    # Check before importing/connecting to the writer at the trust boundary.
    actual = validate_corpus(manifest["path"])
    if actual != manifest:
        raise ValueError("CORPUS changed after validation; rerun validation before loading")

    # One worker thread: with remote signing through fsspec/s3fs, PyIceberg's parallel manifest
    # reads/writes were intermittently refused by SeaweedFS (Forbidden/AccessDenied, 2 of 3 runs);
    # single-threaded, 8 of 8 runs passed (findings F22). A CORPUS load is ~11k rows a day.
    os.environ.setdefault("PYICEBERG_MAX_WORKERS", "1")
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.expressions import EqualTo
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, TimestamptzType

    version, rows = read_corpus(Path(manifest["path"]))
    catalog = load_catalog(
        "railstream",
        type="rest",
        uri=os.environ.get("CATALOG_URI", "http://lakekeeper:8181/catalog"),
        warehouse=os.environ.get("WAREHOUSE", "railstream"),
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "header.X-Iceberg-Access-Delegation": "remote-signing",
        },
    )
    namespace = table_name.rsplit(".", 1)[0]
    catalog.create_namespace_if_not_exists(namespace)
    schema = Schema(
        *[
            NestedField(i, name, TimestamptzType() if name == "loaded_at" else StringType())
            for i, name in enumerate(
                ("stanox", "location_name", "crs", "tiploc", "reference_version", "loaded_at"), 1
            )
        ]
    )
    table = catalog.create_table_if_not_exists(table_name, schema)
    # One reference writer (DAG max_active_runs=1). Atomic whole-version append
    # plus checking the stored version makes retries after commit harmless.
    existing = table.scan(row_filter=EqualTo("reference_version", version)).to_arrow()
    if len(existing):
        stored = sorted(
            [{key: row[key] for key in rows[0]} for row in existing.to_pylist()],
            key=lambda row: row["stanox"],
        )
        if stored != rows:
            raise ValueError("Existing CORPUS version does not match validated locations")
        return {**manifest, "table": table_name, "inserted": 0}
    loaded_at = datetime.now(UTC)
    arrow = pa.Table.from_pylist(
        [dict(row, reference_version=version, loaded_at=loaded_at) for row in rows],
        schema=table.schema().as_arrow(),
    )
    table.append(arrow)
    table.refresh()
    count = table.scan(row_filter=EqualTo("reference_version", version)).count()
    if count != len(rows):
        raise ValueError(f"Stored CORPUS version has {count} rows; expected {len(rows)}")
    return {**manifest, "table": table_name, "inserted": count}


def main() -> None:
    """`python -m orchestration.corpus FILE`: validate and load a CORPUS file (make load-corpus)."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description=main.__doc__)
    # Default = the path make load-corpus mounts. Kept out of the command line because Git Bash
    # rewrites arguments starting with "/" into Windows paths.
    parser.add_argument("file", nargs="?", default="/reference/CORPUSExtract.json.gz")
    parser.add_argument("--table", default="ref.locations")
    args = parser.parse_args()
    result = load_corpus(validate_corpus(args.file), table_name=args.table)
    print("CORPUS_LOAD " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
