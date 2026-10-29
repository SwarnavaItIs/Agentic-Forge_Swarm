import pytest

from forge_agent.server import runtime_token


def test_mounted_credential_is_bounded_and_unambiguous(tmp_path, monkeypatch):
    path = tmp_path / "credential"
    path.write_text("x" * 48 + "\n")
    monkeypatch.setenv("AGENT_RUNTIME_TOKEN_FILE", str(path))
    monkeypatch.delenv("AGENT_RUNTIME_TOKEN", raising=False)
    assert runtime_token() == "x" * 48
    monkeypatch.setenv("AGENT_RUNTIME_TOKEN", "y" * 48)
    with pytest.raises(ValueError):
        runtime_token()
    monkeypatch.delenv("AGENT_RUNTIME_TOKEN")
    # A maximum-length token plus newline cannot hide an ignored file tail.
    path.write_text("x" * 256 + "\nignored-tail")
    with pytest.raises(ValueError):
        runtime_token()
    path.write_text("x" * 256 + "\n")
    assert runtime_token() == "x" * 256
    path.write_text("x" * 257)
    with pytest.raises(ValueError):
        runtime_token()
