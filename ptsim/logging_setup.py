from __future__ import annotations

import logging
import sys


def setup(level: str = "INFO") -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-7s %(name)-18s %(message)s", "%H:%M:%S")
    )
    root.addHandler(h)
    root.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
