import gzip
import io
import json
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request

import pytest

from orchestration.corpus import (
    ReferenceRedirect,
    download_corpus,
    load_corpus,
    run_directory,
    validate_corpus,
)


class FakeDownload:
    def __init__(self, payload):
        self.payload = payload

    def open(self, request, timeout):
        return io.BytesIO(self.payload)


@pytest.mark.parametrize("compressed", [False, True])
def test_download_and_validation_support_gzip_and_plain_json(tmp_path, compressed):
    raw = json.dumps({"TIPLOCDATA": [{"STANOX": "00001", "NLCDESC": "Station"}]}).encode()
    payload = gzip.compress(raw) if compressed else raw
    path = download_corpus(tmp_path, "test", "unused", opener=FakeDownload(payload))
    result = validate_corpus(path)
    assert result["locations"] == 1
    assert gzip.decompress(Path(path).read_bytes()) == raw


def test_failed_download_preserves_previous_file_without_signed_url_in_error(tmp_path):
    path = tmp_path / "CORPUSExtract.json.gz"
    path.write_bytes(b"previous complete download")

    class Failed:
        def open(self, request, timeout):
            raise URLError("https://example.com/file?private-signature=do-not-log")

    with pytest.raises(RuntimeError, match="^CORPUS download failed \\(URLError\\)$"):
        download_corpus(tmp_path, "test", "unused", opener=Failed())
    assert path.read_bytes() == b"previous complete download"


def test_invalid_payload_never_validates_for_loading(tmp_path):
    path = download_corpus(tmp_path, "test", "unused", opener=FakeDownload(b"<html>login</html>"))
    with pytest.raises(ValueError):
        validate_corpus(path)


def test_changed_download_requires_validation_before_any_iceberg_write(tmp_path):
    path = tmp_path / "source.json"
    path.write_text(json.dumps({"TIPLOCDATA": [{"STANOX": "00001"}]}))
    manifest = validate_corpus(str(path))
    path.write_text(json.dumps({"TIPLOCDATA": [{"STANOX": "00002"}]}))
    with pytest.raises(ValueError, match="changed after validation"):
        load_corpus(manifest)


def test_redirect_strips_credentials_for_signed_storage_url():
    request = Request("https://rail.example/file", headers={"Authorization": "Basic unused"})
    handler = ReferenceRedirect()
    same = handler.redirect_request(request, None, 302, "Found", {}, "https://rail.example/next")
    other = handler.redirect_request(request, None, 302, "Found", {}, "https://s3.example/file")
    assert same.get_header("Authorization") == "Basic unused"
    assert other.get_header("Authorization") is None
    with pytest.raises(ValueError, match="requires HTTPS"):
        handler.redirect_request(request, None, 302, "Found", {}, "http://s3.example/file")


def test_run_directory_cannot_escape_reference_root(monkeypatch, tmp_path):
    monkeypatch.setenv("REFERENCE_ROOT", str(tmp_path))
    path = run_directory("../../outside")
    assert path.parent == tmp_path / "corpus"
    assert path == run_directory("../../outside")
