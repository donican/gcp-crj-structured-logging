import uuid
from contextvars import ContextVar
from typing import Any, Dict

_log_context: ContextVar[Dict[str, Any]] = ContextVar("log_context", default={})

class LogContext:
    def __init__(self, **kwargs):
        self.attributes = kwargs
        if "trace_id" not in self.attributes:
            self.attributes["trace_id"] = str(uuid.uuid4())
        self.token = None

    def __enter__(self):
        current = _log_context.get().copy()
        current.update(self.attributes)
        self.token = _log_context.set(current)
        return current

    def __exit__(self, exc_type, exc_val, exc_tb):
        _log_context.reset(self.token)

def get_current_context() -> Dict[str, Any]:
    return _log_context.get()