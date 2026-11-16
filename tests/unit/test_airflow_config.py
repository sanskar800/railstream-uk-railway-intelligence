"""Airflow reads its database URI command verbatim, including trailing whitespace."""

from urllib.parse import unquote, urlsplit

from infra.airflow.start import main


def test_airflow_database_command_encodes_password_without_newline(monkeypatch, capsys):
    password = "local:p@ss/word?#%"
    monkeypatch.setenv("AIRFLOW_DB_PASSWORD", password)
    monkeypatch.setattr("sys.argv", ["start.py", "--database-url"])
    main()
    output = capsys.readouterr().out
    assert output == output.strip()
    parsed = urlsplit(output)
    assert unquote(parsed.password) == password
    assert parsed.hostname == "postgres" and parsed.path == "/airflow"
