import logging
import time
import uuid
from enum import Enum

from prometheus_client import REGISTRY
from pydantic import BaseModel

from irmy_messaging import RabbitMessageRouter, RabbitProducer
from irmy_messaging.model import MessageHeaders
from irmy_messaging.rabbitmq_client import routing_key_str

from test_dead_letters import _FakeChannel, _consumer

_LOGGER = logging.getLogger("irmy-messaging-test")


class _Keys(str, Enum):
    ORDER_CREATE = "order.create"


class _Order(BaseModel):
    orderId: int


def test_routing_key_str():
    assert routing_key_str(_Keys.ORDER_CREATE) == "order.create"
    assert type(routing_key_str(_Keys.ORDER_CREATE)) is str
    assert routing_key_str("order.create") == "order.create"


async def test_publish_uses_enum_value_for_amqp_and_metrics():
    published = []

    class _Exchange:
        async def publish(self, msg, routing_key, timeout):
            published.append(routing_key)

    producer = RabbitProducer(connection_url="amqp://localhost/", logger=_LOGGER, exchange_name="irmy",
                              hmac_secret="s" * 16, producer_name="test", producer_id=str(uuid.uuid4()))
    producer._exchange = _Exchange()
    await producer.publish({"orderId": 1}, routing_key=_Keys.ORDER_CREATE)

    assert published == ["order.create"] and type(published[0]) is str
    assert REGISTRY.get_sample_value("messages_published_total", {"routing_key": "order.create"}) >= 1
    assert REGISTRY.get_sample_value("messages_published_total", {"routing_key": "_Keys.ORDER_CREATE"}) is None


async def test_consumer_binds_enum_value():
    channel = _FakeChannel()
    await _consumer()._declare_infrastructure(channel, "irmy", {"orders": [_Keys.ORDER_CREATE]})
    assert channel.queues["orders"].bindings == [("irmy", "order.create")]
    assert type(channel.queues["orders"].bindings[0][1]) is str


async def test_router_registered_with_enum_matches_incoming_string_key():
    called = []

    async def handler(body: _Order, headers: MessageHeaders):
        called.append(body.orderId)

    router = RabbitMessageRouter(logger=_LOGGER)
    router.register(_Keys.ORDER_CREATE, handler)
    assert router.get_routing_keys() == ["order.create"]
    headers = MessageHeaders(event_version=1, event_timestamp=int(time.time()), tracing_id=uuid.uuid4(),
                             producer_name="test", producer_id=uuid.uuid4(), x_signature="a" * 64)
    await router("order.create", {"orderId": 3}, headers)
    assert called == [3]
