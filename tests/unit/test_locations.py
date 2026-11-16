import gzip
import json

import pytest

from trust.locations import read_corpus, select_locations


def test_one_deterministic_row_per_stanox_preserves_leading_zeros():
    rows = [
        {"STANOX": "00001", "NLCDESC": "Junction", "TIPLOC": "JN"},
        {"STANOX": "00001", "NLCDESC": "Station ", "TIPLOC": "ST", "3ALPHA": " ABC "},
        {"STANOX": "", "NLCDESC": "No TRUST mapping"},
    ]
    selected = select_locations(rows)
    assert selected == select_locations(list(reversed(rows)))
    assert selected == [
        {"stanox": "00001", "location_name": "Station", "crs": "ABC", "tiploc": "ST"}
    ]


def test_reference_version_ignores_gzip_header_metadata(tmp_path):
    raw = json.dumps({"TIPLOCDATA": [{"STANOX": "00001", "NLCDESC": "Station"}]}).encode()
    a, b = tmp_path / "a.gz", tmp_path / "b.gz"
    a.write_bytes(gzip.compress(raw, mtime=1))
    b.write_bytes(gzip.compress(raw, mtime=2))
    assert read_corpus(a) == read_corpus(b)


@pytest.mark.parametrize("rows", [[], [{"STANOX": "123"}], [{"STANOX": ""}]])
def test_invalid_or_empty_reference_is_refused(rows, tmp_path):
    path = tmp_path / "locations.json"
    path.write_text(json.dumps({"TIPLOCDATA": rows}))
    with pytest.raises(ValueError):
        read_corpus(path)
