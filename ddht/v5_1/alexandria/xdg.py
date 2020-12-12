import os
from pathlib import Path

from ddht.xdg import get_xdg_ddht_root


def get_xdg_alexandria_root() -> Path:
    """
    Returns the base directory under which alexandria will store data.
    """
    try:
        return Path(os.environ["XDG_ALEXANDRIA_ROOT"])
    except KeyError:
        return get_xdg_ddht_root() / "alexandria"
