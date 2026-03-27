import json
import logging
import sys
from logger.context import get_current_context

def setup_logging() -> logging.Logger:
    logger = logging.getLogger()

    if logger.handlers:
        return

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(message)s'))

    logger.addHandler(handler)

    return logger

def _log(logger: logging.Logger, severity:str, message: str, **fields) -> None:
    context = get_current_context()
    payload = {
        "logger": logger.name,
        "message": message,
        "severity": severity,
        **context,
        **fields
    }

    if severity == "ERROR":
        logger.error(json.dumps(payload, ensure_ascii=False))
    else:
        logger.info(json.dumps(payload, ensure_ascii=False))

def log_info(logger: logging.Logger, message: str, **fields) -> None:
    _log(logger, "INFO", message, **fields)

def log_error(logger: logging.Logger, message: str, **fields) -> None:
    _log(logger, "ERROR", message, **fields)