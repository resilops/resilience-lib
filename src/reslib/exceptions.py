from typing import Any, Dict, Optional


class ExceptionWithContext(Exception):
    def __init__(self, message: str, *, context: Optional[Dict[Any, Any]] = None):
        super().__init__(message)
        self.context = context


class FunctionNotFound(ExceptionWithContext):
    pass


class InvalidAsyncHandler(ExceptionWithContext):
    pass


class NotSupportedError(ExceptionWithContext):
    pass
