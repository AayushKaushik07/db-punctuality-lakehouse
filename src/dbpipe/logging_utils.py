"""Small logging helper so every module logs in the same format."""

import logging
import sys

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    root = logging.getLogger("dbpipe")
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT, datefmt="%H:%M:%S"))
        root.addHandler(handler)
        root.setLevel(logging.INFO)
        # Spark's py4j logger is very chatty at INFO level
        logging.getLogger("py4j").setLevel(logging.WARNING)
    return logging.getLogger(f"dbpipe.{name}")
