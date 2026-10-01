import logging
import time
import uuid

import pytest
from pydantic import BaseModel

from irmy_messaging import QueueType, RabbitConsumer, RabbitMessageRouter
from irmy_messaging.model import MessageHeaders
from irmy_messaging.rabbitmq_client import DLQ_MESSAGE_TTL_MS

_LOGGER = logging.getLogger("irmy-messaging-test")


def _headers() -> MessageHeaders:
    return MessageHeaders(
        event_version=1, event_timestamp=int(time.time()), tracing_id=uuid.uuid4(),
        producer_name="test", producer_id=uuid.uuid4(), x_signature="a" * 64,
    )


# --------------------------------------------------------------------------- router


class _Order(BaseModel):
    orderId: int


async def test_router_drops_invalid_payload():
    called = []

    async def handler(body: _Order, headers: MessageHeaders):
        called.append(body)

    router = RabbitMessageRouter(logger=_LOGGER)
    router.register("order.create", handler)
    await router("order.create", {"orderId": "not-a-number"}, _headers())  # must not raise
    assert called == []


async def test_router_dispatches_valid_payload():
    called = []

    async def handler(body: _Order, headers: MessageHeaders):
        called.append(body.orderId)

    router = RabbitMessageRouter(logger=_LOGGER)
    router.register("order.create", handler)
    await router("order.create", {"orderId": 7}, _headers())
    assert called == [7]


async def test_router_propagates_handler_errors():
    async def handler(body: _Order, headers: MessageHeaders):
        raise RuntimeError("boom")

    router = RabbitMessageRouter(logger=_LOGGER)
    router.register("order.create", handler)
    with pytest.raises(RuntimeError):
        await router("order.create", {"orderId": 7}, _headers())


# --------------------------------------------------------------------------- topology


class _FakeQueue:
    def __init__(self, name, kwargs):
        self.name, self.kwargs, self.bindings = name, kwargs, []

    async def bind(self, exchange, routing_key):
        self.bindings.append((exchange.name, routing_key))

    async def consume(self, callback):
        return f"tag-{self.name}"


class _FakeExchange:
    def __init__(self, name):
        self.name = name


class _FakeChannel:
    def __init__(self):
        self.exchanges, self.queues = {}, {}

    async def declare_exchange(self, name, **kwargs):
        return self.exchanges.setdefault(name, _FakeExchange(name))

    async def declare_queue(self, name, **kwargs):
        return self.queues.setdefault(name, _FakeQueue(name, kwargs))


def _consumer(queue_type=QueueType.DURABLE) -> RabbitConsumer:
    return RabbitConsumer(
        connection_url="amqp://localhost/", logger=_LOGGER, exchange_name="irmy",
        hmac_secret="s" * 16, bindings={}, queue_type=queue_type,
    )


async def test_one_shared_dlq_for_all_service_queues():
    channel = _FakeChannel()
    await _consumer()._declare_infrastructure(
        channel, "irmy", {"application_management": ["alert.#"], "incident_management": ["incident.#"]}
    )

    assert set(channel.queues) == {"irmy_dlq", "application_management", "incident_management"}
    dlq = channel.queues["irmy_dlq"]
    assert dlq.bindings == [("irmy_dlx", "#")]
    assert dlq.kwargs["arguments"] == {"x-queue-type": "quorum", "x-message-ttl": DLQ_MESSAGE_TTL_MS}
    assert DLQ_MESSAGE_TTL_MS == 14 * 24 * 3600 * 1000
    for name in ("application_management", "incident_management"):
        assert channel.queues[name].kwargs["arguments"]["x-dead-letter-exchange"] == "irmy_dlx"


async def test_temporary_queues_have_no_dead_lettering():
    channel = _FakeChannel()
    await _consumer(QueueType.TEMPORARY)._declare_infrastructure(channel, "irmy", {"replica-1": ["control.#"]})
    assert set(channel.queues) == {"replica-1"}
    assert "irmy_dlx" not in channel.exchanges


async def test_delete_queues_keeps_the_shared_dlq():
    deleted = []

    class _Channel:
        async def queue_delete(self, name):
            deleted.append(name)

    consumer = RabbitConsumer(
        connection_url="amqp://localhost/", logger=_LOGGER, exchange_name="irmy",
        hmac_secret="s" * 16, bindings={"plugin-dispatch.slack": ["slack"]},
    )

    async def _get_channel():
        return _Channel()

    consumer._get_channel = _get_channel
    await consumer.delete_queues()
    assert deleted == ["plugin-dispatch.slack"]
