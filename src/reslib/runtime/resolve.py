import inspect
from types import ModuleType

from reslib import actions, guardrails, observers, rollbacks
from reslib.constants import AsyncFunc
from reslib.exceptions import FunctionNotFound, InvalidAsyncHandler
from reslib.runtime.phases import ExecutionPhase

__all__ = ["resolve"]


_PHASE_MODULES = {
    ExecutionPhase.ACTION: actions,
    ExecutionPhase.OBSERVER: observers,
    ExecutionPhase.GUARDRAIL: guardrails,
    ExecutionPhase.ROLLBACK: rollbacks,
}


def _resolve_async_function(module: ModuleType, name: str) -> AsyncFunc:
    """
    Resolve an exported async function from a module by name.

    This function enforces strict safety guarantees:
    - Only functions explicitly exported via ``__all__`` may be resolved
    - The resolved object must be an async function

    This prevents accidental execution of internal helpers
    and keeps the runtime execution surface explicit and controlled.

    Args:
        module:
            Python module containing exported async functions.
        name:
            Name of the function to resolve.

    Returns:
        AsyncFunc:
            The resolved async callable.

    Raises:
        FunctionDoesntExists:
            If the function is not exported or does not exist.
        TypeError:
            If the resolved object is not an async function.
    """
    exported = getattr(module, "__all__", [])
    if name not in exported:
        raise FunctionNotFound("Function not available", context={"function": name})

    func: AsyncFunc = getattr(module, name)

    if not inspect.iscoroutinefunction(func):
        raise InvalidAsyncHandler(
            "Invalid async handler", context={"name": name, "module": module.__name__}
        )

    return func


def resolve(phase: ExecutionPhase, name: str) -> AsyncFunc:
    """
    Resolve an async executable for a given execution phase.

    This function acts as the single entry point for resolving
    *all* user-defined runtime logic (actions, observers,
    guardrails, rollbacks).

    Resolution rules:
    - The phase determines the module namespace
    - The function name must be explicitly exported via ``__all__``
    - The resolved object must be an async callable

    Args:
        phase:
            The execution phase in which the function will run
            (e.g. ACTION, OBSERVER, ROLLBACK).
        name:
            Name of the function to resolve within the phase module.

    Returns:
        AsyncFunc:
            An async callable ready for execution.

    Raises:
        ValueError:
            If the execution phase is not supported.
        FunctionDoesntExists:
            If the function does not exist or is not exported.
        TypeError:
            If the resolved object is not an async function.
    """
    module = _PHASE_MODULES.get(phase)
    if not module:
        raise ValueError(f"Unsupported execution phase: {phase}")

    return _resolve_async_function(module, name)
