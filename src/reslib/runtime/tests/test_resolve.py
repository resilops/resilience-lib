from types import ModuleType

import pytest

from reslib.exceptions import FunctionNotFound, InvalidAsyncHandler
from reslib.runtime.phases import ExecutionPhase
from reslib.runtime.resolve import _resolve_async_function, resolve


async def _async_handler():
    return None


def _sync_handler():
    return None


def test_resolve_async_function_returns_exported_async_handler():
    module = ModuleType("fake_module")
    module.__all__ = ["_async_handler"]
    module._async_handler = _async_handler

    handler = _resolve_async_function(module, "_async_handler")

    assert handler is _async_handler


def test_resolve_async_function_rejects_missing_export():
    module = ModuleType("fake_module")
    module.__all__ = []

    with pytest.raises(FunctionNotFound, match="missing"):
        _resolve_async_function(module, "missing")


def test_resolve_async_function_rejects_sync_handler():
    module = ModuleType("fake_module")
    module.__all__ = ["_sync_handler"]
    module._sync_handler = _sync_handler

    with pytest.raises(InvalidAsyncHandler, match="_sync_handler"):
        _resolve_async_function(module, "_sync_handler")


def test_resolve_returns_known_observer_handler():
    handler = resolve(ExecutionPhase.OBSERVER, "measure_endpoint_latency")

    assert handler.__name__ == "measure_endpoint_latency"
