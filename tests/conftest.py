import pathlib
import tempfile

import pytest


@pytest.fixture(autouse=True)
def xdg_home(monkeypatch):
    with tempfile.TemporaryDirectory() as temp_xdg:
        monkeypatch.setenv("XDG_DATA_HOME", temp_xdg)
        yield pathlib.Path(temp_xdg)
