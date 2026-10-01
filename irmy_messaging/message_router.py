import inspect
import typing
from logging import Logger
from typing import Any, Callable
from pydantic import BaseModel

from .model import MessageHeaders
from .rabbitmq_client import routing_key_str


class RabbitMessageRouter:
    def __init__(self, logger: Logger) -> None:
        # rk -> (ModelClass, handler)
        self._routes: dict[str, tuple[type[BaseModel], Callable]] = {}
        self._logger = logger

    def register(self, routing_key: str, handler: Callable) -> None:
        routing_key = routing_key_str(routing_key)
        model_cls = self._extract_model(handler)
        self._routes[routing_key] = (model_cls, handler)
        self._logger.debug("Registered handler '%s' for routing key '%s'", handler.__qualname__, routing_key)

    def get_routing_keys(self) -> list[str]:
        return list(self._routes.keys())

    async def __call__(self, routing_key: str, body: dict, headers: MessageHeaders) -> None:
        if routing_key not in self._routes:
            self._logger.warning("No handler for routing key '%s', dropping message", routing_key)
            return

        model_cls, handler = self._routes[routing_key]

        try:
            event = model_cls.model_validate(body)
        except Exception:
            # A payload that does not match its model never will: drop it (the
            # consumer acks) instead of retrying it into the dead-letter queue.
            self._logger.exception(
                "Failed to deserialize body for routing key '%s' (tracing id %s), dropping message",
                routing_key, headers.tracing_id,
            )
            return

        await handler(event, headers)

    @staticmethod
    def _extract_model(handler: Callable) -> type[BaseModel]:
        hints = _get_hints_without_self(handler)

        models = [
            v for v in hints.values()
            if isinstance(v, type) and issubclass(v, BaseModel) and v is not MessageHeaders
        ]

        if not models:
            raise ValueError(f"Handler '{handler.__qualname__}' has no Pydantic model parameter")
        if len(models) > 1:
            raise ValueError(f"Handler '{handler.__qualname__}' has multiple Pydantic model parameters")

        return models[0]


def _get_hints_without_self(handler: Callable) -> dict[str, Any]:
    sig = inspect.signature(handler)
    type_hints = typing.get_type_hints(handler)

    return {
        name: type_hints[name]
        for name in sig.parameters
        if name not in ("self", "return") and name in type_hints
    }
