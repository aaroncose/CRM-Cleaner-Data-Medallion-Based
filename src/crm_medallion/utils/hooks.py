"""Hook system for extensibility."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


class HookPhase(str, Enum):
    """When the hook is executed."""

    PRE = "pre"
    POST = "post"


class HookResult(str, Enum):
    """Result of hook execution."""

    CONTINUE = "continue"
    SKIP = "skip"
    ABORT = "abort"


T = TypeVar("T")


@dataclass
class HookContext(Generic[T]):
    """Context passed to hooks."""

    data: T
    layer: str
    phase: HookPhase
    metadata: dict[str, Any]

    def with_data(self, new_data: T) -> "HookContext[T]":
        """Create new context with updated data."""
        return HookContext(
            data=new_data,
            layer=self.layer,
            phase=self.phase,
            metadata=self.metadata,
        )


@dataclass
class HookResponse(Generic[T]):
    """Response from a hook execution."""

    result: HookResult
    data: T | None = None
    message: str | None = None


class Hook(ABC, Generic[T]):
    """Abstract base class for hooks."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Hook identifier for logging."""
        pass

    @abstractmethod
    def execute(self, context: HookContext[T]) -> HookResponse[T]:
        """
        Execute the hook.

        Args:
            context: Hook context with data and metadata

        Returns:
            HookResponse with result and optional modified data
        """
        pass


class FunctionHook(Hook[T]):
    """Hook wrapper for simple callable functions."""

    def __init__(
        self,
        func: Callable[[HookContext[T]], HookResponse[T] | T | None],
        name: str | None = None,
    ):
        self._func = func
        self._name = name or func.__name__

    @property
    def name(self) -> str:
        return self._name

    def execute(self, context: HookContext[T]) -> HookResponse[T]:
        result = self._func(context)

        if isinstance(result, HookResponse):
            return result

        if result is None:
            return HookResponse(result=HookResult.CONTINUE)

        return HookResponse(result=HookResult.CONTINUE, data=result)


class HookRegistry:
    """Registry for managing hooks across layers."""

    def __init__(self):
        self._hooks: dict[str, list[Hook]] = {}

    def _key(self, layer: str, phase: HookPhase) -> str:
        return f"{phase.value}_{layer}"

    def register(
        self,
        layer: str,
        phase: HookPhase,
        hook: Hook | Callable,
    ) -> None:
        """
        Register a hook for a layer and phase.

        Args:
            layer: Layer name (bronze, silver, gold)
            phase: When to execute (pre, post)
            hook: Hook instance or callable
        """
        key = self._key(layer, phase)

        if key not in self._hooks:
            self._hooks[key] = []

        if callable(hook) and not isinstance(hook, Hook):
            hook = FunctionHook(hook)

        self._hooks[key].append(hook)
        logger.debug(f"Registered hook '{hook.name}' for {phase.value}_{layer}")

    def unregister(self, layer: str, phase: HookPhase, hook_name: str) -> bool:
        """
        Unregister a hook by name.

        Returns:
            True if hook was found and removed
        """
        key = self._key(layer, phase)

        if key not in self._hooks:
            return False

        original_count = len(self._hooks[key])
        self._hooks[key] = [h for h in self._hooks[key] if h.name != hook_name]

        removed = len(self._hooks[key]) < original_count
        if removed:
            logger.debug(f"Unregistered hook '{hook_name}' from {phase.value}_{layer}")

        return removed

    def get_hooks(self, layer: str, phase: HookPhase) -> list[Hook]:
        """Get all hooks for a layer and phase."""
        key = self._key(layer, phase)
        return self._hooks.get(key, [])

    def clear(self, layer: str | None = None, phase: HookPhase | None = None) -> None:
        """Clear hooks, optionally filtered by layer and/or phase."""
        if layer is None and phase is None:
            self._hooks.clear()
        elif layer is not None and phase is not None:
            key = self._key(layer, phase)
            self._hooks.pop(key, None)
        elif layer is not None:
            for p in HookPhase:
                key = self._key(layer, p)
                self._hooks.pop(key, None)
        elif phase is not None:
            keys_to_remove = [k for k in self._hooks if k.startswith(phase.value)]
            for key in keys_to_remove:
                self._hooks.pop(key, None)


class HookExecutor:
    """Executes hooks and handles results."""

    def __init__(self, registry: HookRegistry):
        self.registry = registry

    def execute_hooks(
        self,
        layer: str,
        phase: HookPhase,
        data: T,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[HookResult, T]:
        """
        Execute all hooks for a layer and phase.

        Args:
            layer: Layer name
            phase: Hook phase
            data: Data to pass to hooks
            metadata: Optional metadata

        Returns:
            Tuple of (final result, possibly modified data)
        """
        hooks = self.registry.get_hooks(layer, phase)

        if not hooks:
            return HookResult.CONTINUE, data

        context = HookContext(
            data=data,
            layer=layer,
            phase=phase,
            metadata=metadata or {},
        )

        current_data = data

        for hook in hooks:
            logger.debug(f"Executing hook '{hook.name}' for {phase.value}_{layer}")

            try:
                response = hook.execute(context.with_data(current_data))

                if response.data is not None:
                    current_data = response.data

                if response.result == HookResult.SKIP:
                    logger.info(
                        f"Hook '{hook.name}' requested skip: {response.message}"
                    )
                    return HookResult.SKIP, current_data

                if response.result == HookResult.ABORT:
                    logger.warning(
                        f"Hook '{hook.name}' requested abort: {response.message}"
                    )
                    return HookResult.ABORT, current_data

            except Exception as e:
                logger.error(f"Hook '{hook.name}' raised exception: {e}")
                raise

        return HookResult.CONTINUE, current_data


_global_registry = HookRegistry()


def get_global_registry() -> HookRegistry:
    """Get the global hook registry."""
    return _global_registry


def register_hook(
    layer: str,
    phase: HookPhase,
    hook: Hook | Callable,
) -> None:
    """Register a hook in the global registry."""
    _global_registry.register(layer, phase, hook)


def create_hook(name: str | None = None):
    """Decorator to create a hook from a function."""

    def decorator(func: Callable) -> FunctionHook:
        return FunctionHook(func, name=name)

    return decorator
