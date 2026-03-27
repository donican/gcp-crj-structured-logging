import json
import logging
import sys
from logger.context import get_current_context

def setup_logging() -> None:
    logger = logging.getLogger()

    if logger.handlers:
        return

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(message)s'))

    logger.addHandler(handler)

def log_info(logger: logging.Logger, message: str, **fields) -> None:
    context = get_current_context()
    payload = {
        "message": message,
        "severity": "INFO",
        **context,
        **fields
    }
    logger.info(json.dumps(payload, ensure_ascii=False))

def log_error(logger: logging.Logger, message: str, **fields) -> None:
    payload = {
        "message": message,
        "severity": "ERROR", 
        **fields
    }
    logger.error(json.dumps(payload, ensure_ascii=False))