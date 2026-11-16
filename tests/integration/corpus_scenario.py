"""DAG import/wiring and immutable reference versions; isolated tables are retained."""

import json
import tempfile
import uuid
from pathlib import Path

from airflow.dag_processing.dagbag import DagBag
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

from orchestration.corpus import load_corpus, validate_corpus


def main():
    bag = DagBag(dag_folder="/opt/airflow/dags")
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["refresh_corpus"]
    assert set(dag.task_ids) == {"download", "validate", "load"}
    assert dag.get_task("download").downstream_task_ids == {"validate"}
    assert dag.get_task("validate").downstream_task_ids == {"load"}
    assert dag.max_active_runs == 1 and not dag.catchup
    assert dag.schedule == "0 4 * * *"
    assert dag.get_task("load").retries == 2

    table_name = f"corpus_it_{uuid.uuid4().hex[:8]}.locations"
    catalog = load_catalog(
        "railstream",
        type="rest",
        uri="http://lakekeeper:8181/catalog",
        warehouse="railstream",
        **{
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "header.X-Iceberg-Access-Delegation": "remote-signing",
        },
    )
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "corpus.json"
        rows = [
            {"STANOX": "00001", "NLCDESC": "Station alias"},
            {"STANOX": "00001", "NLCDESC": "Station", "3ALPHA": "ABC"},
            {"STANOX": "00002", "NLCDESC": "Junction"},
        ]
        path.write_text(json.dumps({"TIPLOCDATA": rows}))
        manifest = validate_corpus(str(path))
        first = load_corpus(manifest, table_name=table_name)
        table = catalog.load_table(table_name)
        snapshot = table.current_snapshot().snapshot_id
        second = load_corpus(manifest, table_name=table_name)
        table.refresh()
        assert first["inserted"] == 2 and second["inserted"] == 0
        assert table.current_snapshot().snapshot_id == snapshot
        original = table.scan().to_arrow().to_pylist()
        assert len(original) == 2 and original[0]["stanox"] == "00001"
        rows[1]["NLCDESC"] = "Station updated"
        path.write_text(json.dumps({"TIPLOCDATA": rows}))
        refreshed = load_corpus(validate_corpus(str(path)), table_name=table_name)
        table.refresh()
        assert refreshed["reference_version"] != first["reference_version"]
        assert refreshed["inserted"] == 2 and table.scan().count() == 4
        stored = table.scan(row_filter=EqualTo("reference_version", first["reference_version"]))
        assert stored.to_arrow().to_pylist() == original
        try:
            load_corpus(manifest, table_name=table_name)
        except ValueError as exc:
            assert "changed after validation" in str(exc)
        else:
            raise AssertionError("changed content was accepted without validation")
        table.refresh()
        assert table.scan().count() == 4
    print(
        "CORPUS_SCENARIO "
        + json.dumps(
            {
                "table": table_name,
                "versions": 2,
                "rows": 4,
                "repeat_inserted": second["inserted"],
                "verified": [
                    "dag_import",
                    "task_wiring",
                    "daily_schedule",
                    "duplicate_key_selection",
                    "repeat_without_new_snapshot",
                    "immutable_old_version",
                    "changed_input_rejected",
                ],
                "cleanup": "isolated reference table retained; no deletes",
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
