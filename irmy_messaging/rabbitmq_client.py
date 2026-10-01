import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from enum import Enum
from typing import Callable, Dict, List, Optional, Any, Union
from uuid import UUID

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustQueue, DeliveryMode
from aio_pika.exceptions import AMQPError

from pydantic import ValidationError

from .model import MessageHeaders
from .exceptions import RabbitException
from . import metrics


# Dead letters of every service end up in one queue per exchange: "<exchange>_dlq",
# bound to "<exchange>_dlx" with "#". The broker's x-death header on each message
# records which queue (= service) rejected it and why. Kept for 14 days.
DLQ_MESSAGE_TTL_MS = 14 * 24 * 60 * 60 * 1000


def routing_key_str(routing_key: Union[str, Enum]) -> str:
    """Plain string of a routing key. Callers pass members of ``(str, Enum)`` classes
    such as ``irmy_events.RoutingKeys``; since Python 3.11 ``str()``/f-strings of those
    give ``"RoutingKeys.AUDIT_SERVICE_CREATE"``, not the value, which then leaks into
    metric labels and logs."""
    return routing_key.value if isinstance(routing_key, Enum) else routing_key


class QueueType(str, Enum):
    """Queue durability profile for a ``RabbitConsumer``.

    ``DURABLE`` (default) — durable quorum queue with an ``x-delivery-limit`` that
    dead-letters to the shared ``<exchange>_dlx`` → ``<exchange>_dlq``; survives
    broker and consumer restarts.
    ``TEMPORARY`` — a non-durable, exclusive, auto-delete classic queue bound to
    the consumer's connection (the broker deletes it when that connection drops),
    with no DLX/DLQ. Use ``TEMPORARY`` for ephemeral fan-out — e.g. a per-replica
    control channel — where orphaned durable queues would otherwise pile up.
    """

    DURABLE = "durable"
    TEMPORARY = "temporary"


class _BaseRabbit:
    def __init__(self, connection_url: str, logger: logging.Logger):
        self._connection_url = connection_url
        self._logger = logger
        self._connection: Optional[aio_pika.RobustConnection] = None
        self._channel: Optional[aio_pika.RobustChannel] = None
        self._connection_lock = asyncio.Lock()
        self._channel_lock = asyncio.Lock()

    async def _get_connection(self) -> aio_pika.RobustConnection:
        async with self._connection_lock:
            if self._connection is None or self._connection.is_closed:
                try:
                    self._connection = await aio_pika.connect_robust(
                        self._connection_url,
                        timeout=10
                    )
                    self._logger.info("RabbitMQ connected.")
                except Exception as e:
                    self._logger.critical(f"RabbitMQ connection failed: {e}")
                    raise RabbitException(f"Connection failed: {e}")
            return self._connection

    async def _get_channel(self) -> aio_pika.RobustChannel:
        async with self._channel_lock:
            if self._channel is None or self._channel.is_closed:
                conn = await self._get_connection()
                self._channel = await conn.channel(publisher_confirms=True)
            return self._channel

    async def disconnect(self) -> None:
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            self._logger.info("RabbitMQ disconnected.")

    @staticmethod
    def _calculate_hmac(secret: str, timestamp: int, body_bytes: bytes) -> str:
        """
        Payload = <timestamp>.<body_bytes>
        """
        prefix = f"{timestamp}.".encode('ascii')
        payload_to_sign = prefix + body_bytes

        return hmac.new(
            key=secret.encode('utf-8'),
            msg=payload_to_sign,
            digestmod=hashlib.sha256
        ).hexdigest()


class RabbitProducer(_BaseRabbit):
    MIN_SECRET_LENGTH = 8

    def __init__(
            self,
            connection_url: str,
            logger: logging.Logger,
            exchange_name: str,
            hmac_secret: str,
            producer_name: str,
            producer_id: str,
            retries: int = 3
    ):
        super().__init__(connection_url, logger)

        if not exchange_name or not exchange_name.strip():
            raise ValueError("exchange_name cannot be empty")
        if not hmac_secret or len(hmac_secret) < self.MIN_SECRET_LENGTH:
            raise ValueError(f"secret must be at least {self.MIN_SECRET_LENGTH} characters")
        if not producer_name or not producer_name.strip():
            raise ValueError("producer_name cannot be empty")

        self._exchange_name = exchange_name
        self._secret = hmac_secret
        self._producer_name = producer_name
        if not isinstance(producer_id, UUID):
            try:
                self._producer_id = UUID(producer_id)
            except ValueError:
                raise ValueError(f"Invalid UUID for producer_id: {producer_id}")
        else:
            self._producer_id = producer_id

        self._retries = retries
        self._exchange = None
        self._publish_timeout = 5

    async def connect(self) -> None:
        try:
            channel = await self._get_channel()
            self._exchange = await channel.declare_exchange(
                self._exchange_name,
                type=aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            self._logger.info(f"Producer initialized. Exchange: {self._exchange_name}")
        except Exception as e:
            raise RabbitException(f"Producer init error: {e}")

    async def publish(self, message: dict, routing_key: str, tracing_id: Optional[Union[str, UUID]] = None) -> None:
        routing_key = routing_key_str(routing_key)
        if not self._exchange:
            await self.connect()

        body_bytes = json.dumps(message, sort_keys=True, separators=(',', ':')).encode('utf-8')

        # Logic: Convert tracing_id to UUID object
        if tracing_id:
            try:
                actual_tracing_id = tracing_id if isinstance(tracing_id, UUID) else UUID(tracing_id)
            except ValueError:
                raise RabbitException(f"Invalid tracing_id format: {tracing_id}")
        else:
            actual_tracing_id = uuid.uuid4()

        timestamp = int(time.time())
        signature = self._calculate_hmac(self._secret, timestamp, body_bytes)

        try:
            headers_model = MessageHeaders(
                event_version=1,
                event_timestamp=timestamp,
                tracing_id=actual_tracing_id,
                producer_name=self._producer_name,
                producer_id=self._producer_id,
                x_signature=signature
            )
        except ValueError as e:
            raise RabbitException(f"Failed to build valid headers: {e}")

        msg = aio_pika.Message(
            body=body_bytes,
            headers=headers_model.model_dump(by_alias=True, mode="json"),
            content_type='application/json',
            delivery_mode=DeliveryMode.PERSISTENT
        )

        start_time = time.perf_counter()
        for attempt in range(1, self._retries + 2):
            try:
                await asyncio.wait_for(
                    self._exchange.publish(msg, routing_key=routing_key, timeout=self._publish_timeout),
                    timeout=self._publish_timeout + 1
                )
                duration = time.perf_counter() - start_time
                metrics.messages_published_total.labels(routing_key=routing_key).inc()
                metrics.messages_publish_duration_seconds.labels(status="SUCCESS", routing_key=routing_key).observe(duration)
                return

            except (AMQPError, asyncio.TimeoutError, ConnectionError) as e:
                self._logger.warning(f"Publish attempt {attempt} failed: {e}")
                if attempt > self._retries:
                    duration = time.perf_counter() - start_time
                    metrics.messages_publish_errors_total.labels(routing_key=routing_key).inc()
                    metrics.messages_publish_duration_seconds.labels(status="ERROR", routing_key=routing_key).observe(duration)
                    self._logger.error("Max retries reached. Message dropped locally.")
                    raise RabbitException(f"Publish failed after retries: {e}")

                await asyncio.sleep(0.5 * attempt)
                self._exchange = None
                await self.connect()

    async def get_health(self) -> bool:
        return (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
            and self._exchange is not None
        )


class RabbitConsumer(_BaseRabbit):
    def __init__(
            self,
            connection_url: str,
            logger: logging.Logger,
            exchange_name: str,
            hmac_secret: str,
            bindings: Dict[str, List[str]],
            prefetch_count: int = 10,
            requeue_limit: int = 3,
            queue_type: Union[QueueType, str] = QueueType.DURABLE
    ):
        super().__init__(connection_url, logger)
        self._exchange_name = exchange_name
        self._secret = hmac_secret
        self._bindings = bindings
        self._prefetch_count = prefetch_count
        self._requeue_limit = requeue_limit
        self._queue_type = QueueType(queue_type)
        self._user_handler: Optional[Callable[[str, dict, MessageHeaders], Any]] = None
        self._consumer_tags: Dict[str, str] = {}
        self._queues: Dict[str, AbstractRobustQueue] = {}
        self._tag_to_queue: Dict[str, str] = {}

    async def _declare_infrastructure(self, channel: aio_pika.RobustChannel, exchange_name: str,
                                      queue_map: Dict[str, List[str]]) -> None:
        # 1. Main Exchange
        exchange = await channel.declare_exchange(
            exchange_name, type=aio_pika.ExchangeType.TOPIC, durable=True
        )

        # 2. DLX — only for durable queues; temporary queues are best-effort and
        # carry no dead-letter machinery.
        dlx_name: Optional[str] = None
        if self._queue_type is QueueType.DURABLE:
            dlx_name = f"{exchange_name}_dlx"
            dlx = await channel.declare_exchange(dlx_name, type=aio_pika.ExchangeType.TOPIC, durable=True)

            # 3. One shared DLQ per exchange (Quorum). Every service declares it with
            # the same arguments, so the declaration is idempotent.
            dlq = await channel.declare_queue(
                f"{exchange_name}_dlq",
                durable=True,
                arguments={"x-queue-type": "quorum", "x-message-ttl": DLQ_MESSAGE_TTL_MS},
            )
            await dlq.bind(dlx, routing_key="#")

        for q_name, routing_keys in queue_map.items():
            routing_keys = [routing_key_str(rk) for rk in routing_keys]
            if self._queue_type is QueueType.DURABLE:
                # 4. Main Queue (Quorum)
                queue = await channel.declare_queue(
                    q_name,
                    durable=True,
                    arguments={
                        "x-queue-type": "quorum",
                        "x-dead-letter-exchange": dlx_name,
                        "x-delivery-limit": self._requeue_limit
                    }
                )
            else:
                # Temporary: non-durable, exclusive, auto-delete. The broker drops
                # it when this connection closes — no orphans across replica churn.
                queue = await channel.declare_queue(
                    q_name,
                    durable=False,
                    exclusive=True,
                    auto_delete=True,
                )

            for rk in routing_keys:
                await queue.bind(exchange, routing_key=rk)

            self._queues[q_name] = queue
            consumer_tag = await queue.consume(self._on_message)
            self._consumer_tags[q_name] = consumer_tag
            self._tag_to_queue[consumer_tag] = q_name
            self._logger.info(
                f"Subscribed to queue '{q_name}' ({self._queue_type.value}) on "
                f"exchange '{exchange_name}' with keys {routing_keys}"
            )

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        start_time = time.perf_counter()
        queue_name = self._tag_to_queue.get(message.consumer_tag, "unknown")

        async with message.process(ignore_processed=True):
            try:
                # 1. Security check
                received_sig = message.headers.get("x-signature")
                timestamp_raw = message.headers.get("eventTimestamp")

                if not received_sig or timestamp_raw is None:
                    self._logger.error("DROPPED: Missing Security Headers.")
                    metrics.messages_rejected_total.labels(queue=queue_name).inc()
                    await message.ack()
                    return

                try:
                    timestamp_int = int(timestamp_raw)
                    calculated_sig = self._calculate_hmac(self._secret, timestamp_int, message.body)
                except ValueError:
                    self._logger.error("DROPPED: Invalid Timestamp format.")
                    metrics.messages_rejected_total.labels(queue=queue_name).inc()
                    await message.ack()
                    return

                if not hmac.compare_digest(received_sig, calculated_sig):
                    self._logger.critical(f"DROPPED: Invalid Signature!")
                    metrics.messages_rejected_total.labels(queue=queue_name).inc()
                    await message.ack()
                    return

                # 2. Schema check
                try:
                    headers_model = MessageHeaders(**message.headers)
                except ValidationError as e:
                    self._logger.error(f"DROPPED: Invalid Headers Data: {e}")
                    metrics.messages_rejected_total.labels(queue=queue_name).inc()
                    await message.ack()
                    return

                # 3. Parse message body
                try:
                    body_str = message.body.decode('utf-8')
                    payload = json.loads(body_str)
                except (UnicodeDecodeError, json.JSONDecodeError) as e:
                    self._logger.error(f"DROPPED: Invalid message body: {e}")
                    metrics.messages_rejected_total.labels(queue=queue_name).inc()
                    await message.ack()
                    return

                # 4. Business Logic
                if self._user_handler:
                    await self._user_handler(message.routing_key, payload, headers_model)

                duration = time.perf_counter() - start_time
                metrics.messages_consumed_total.labels(status="SUCCESS", queue=queue_name).inc()
                metrics.messages_processing_duration_seconds.labels(status="SUCCESS", queue=queue_name).observe(duration)
                await message.ack()

            except Exception as e:
                duration = time.perf_counter() - start_time
                self._logger.error(f"Processing error in endpoint: {e}")
                metrics.messages_consumed_total.labels(status="ERROR", queue=queue_name).inc()
                metrics.messages_processing_duration_seconds.labels(status="ERROR", queue=queue_name).observe(duration)
                # Durable queues requeue (bounded by x-delivery-limit -> DLQ).
                # Temporary queues have no DLQ, so a requeue would loop forever —
                # drop the message instead.
                requeue = self._queue_type is QueueType.DURABLE
                if requeue:
                    metrics.messages_requeued_total.labels(queue=queue_name).inc()
                await message.nack(requeue=requeue)

    async def consume(self, endpoint: Callable[[str, dict, MessageHeaders], Any]) -> None:
        self._user_handler = endpoint
        try:
            channel = await self._get_channel()
            await channel.set_qos(prefetch_count=self._prefetch_count)

            await self._declare_infrastructure(channel, self._exchange_name, self._bindings)

            self._logger.info("Consumer setup complete. Listening...")
        except Exception as e:
            self._logger.error(f"Consumer setup error: {e}")
            raise RabbitException(f"Consumer setup failed: {e}")

    async def delete_queues(self) -> None:
        """Deletes every queue this consumer was configured with (``bindings``
        at construction time), removing them from the broker entirely. The
        shared ``<exchange>_dlq`` is never deleted — other services use it; dead
        letters that came from these queues stay there until their TTL expires.

        Unlike ``stop()``, which only cancels this consumer's subscription,
        this is a permanent, broker-wide teardown: call it when a queue's
        owner (e.g. an uninstalled plugin) is gone for good, not on ordinary
        disable/restart. Safe to call whether or not the queue was ever
        actually declared (e.g. a consumer that never successfully started
        consuming) or already deleted by another caller — a missing queue is
        treated as already-gone, not an error, and does not stop the rest of
        the deletions (a stale channel from an AMQP-level "not found" is
        transparently reopened before continuing).
        """
        names: List[str] = list(self._bindings)

        for name in names:
            try:
                channel = await self._get_channel()
                await channel.queue_delete(name)
                self._logger.info(f"Deleted queue '{name}'")
            except Exception as e:
                self._logger.warning(f"Could not delete queue '{name}' (may not exist): {e}")
                # A "not found" delete can close the channel server-side;
                # drop our reference so the next _get_channel() reopens one,
                # rather than letting every later name in this loop fail too.
                self._channel = None

    async def stop(self) -> None:
        for q_name, tag in self._consumer_tags.items():
            queue = self._queues.get(q_name)
            if queue:
                try:
                    await queue.cancel(tag)
                    self._logger.info(f"Cancelled consumer on queue '{q_name}'")
                except Exception as e:
                    self._logger.warning(f"Failed to cancel consumer on queue '{q_name}': {e}")
        self._consumer_tags.clear()
        self._tag_to_queue.clear()
        self._queues.clear()
        self._user_handler = None

    async def disconnect(self) -> None:
        await self.stop()
        await super().disconnect()

    async def get_health(self) -> bool:
        return (
            self._connection is not None
            and not self._connection.is_closed
            and self._channel is not None
            and not self._channel.is_closed
            and len(self._consumer_tags) > 0
        )
