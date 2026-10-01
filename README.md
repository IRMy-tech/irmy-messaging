# irmy-messaging

Asynchronous RabbitMQ client used by IRMy services (RabbitMQ 4.x / Quorum Queues).

An asynchronous wrapper around `aio-pika` for working with RabbitMQ (version 4+).
This module implements the Publisher/Subscriber pattern with a strong focus on reliability (Quorum Queues) and security (HMAC message signing).

## Features

- **Quorum Queues**: Utilizes modern Quorum queues by default for data safety.
- **Reliability**: Automatic reconnection, Publisher Confirms, and Native Retry strategies (using `x-delivery-limit`).
- **Dead letters**: One shared dead-letter queue per exchange instead of one per service.
- **Security**: Data integrity validation via HMAC-SHA256 (signs both Timestamp and Body).
- **Validation**: Strict header typing and validation using Pydantic.
- **Non-blocking**: The Consumer operates in the background without blocking the main Event Loop.

## Installation

```bash
pip install irmy-messaging
```

Requires Python 3.12+. The import package is `irmy_messaging`.

## Usage
### Message Router

```python
import logging
from pydantic import BaseModel
from irmy_messaging import RabbitMessageRouter
from irmy_messaging.model import MessageHeaders

logger = logging.getLogger()

class OrderProcessingModel(BaseModel):
    ...

class PaymentProcessingModel(BaseModel):
    ...


async def _process_order(body: OrderProcessingModel, headers: MessageHeaders):
    pass

async def _process_payment(body: PaymentProcessingModel, headers: MessageHeaders):
    pass



message_router = RabbitMessageRouter(logger=logger)
message_router.register("event.order.create", _process_order)
message_router.register("event.payment.receive", _process_payment)
```

### Producer
```python
import asyncio
import logging
from irmy_messaging import RabbitProducer
from irmy_messaging.exceptions import RabbitException

async def main():
    logger = logging.getLogger("Producer")
    
    producer = RabbitProducer(
        connection_url="amqp://guest:guest@localhost:5672/",
        logger=logger,
        exchange_name="my_exchange",
        hmac_secret="my_super_secret",
        producer_name="payment_service",
        producer_id="550e8400-e29b-41d4-a716-446655440000",
        retries=3
    )

    try:
        await producer.connect()
        
        # tracing_id is optional (a new UUID will be generated if not provided)
        await producer.publish(
            message={"order_id": 123, "status": "paid"},
            routing_key="order.paid"
        )
    except RabbitException as e:
        logger.error(f"Failed to send: {e}")
    finally:
        await producer.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
```

### Consumer
```python
import asyncio
import logging
from irmy_messaging import RabbitConsumer, RabbitMessageRouter
from irmy_messaging.model import MessageHeaders

logger = logging.getLogger("Consumer")

message_router = RabbitMessageRouter(logger=logger)
message_router.register(...)
message_router.register(...)

async def main():
    # Topology configuration
    bindings = {
        "my_queue": message_router.get_routing_keys()
    }

    consumer = RabbitConsumer(
        connection_url="amqp://guest:guest@localhost:5672/",
        logger=logger,
        hmac_secret="my_super_secret",
        bindings=bindings,
        exchange_name="irmy",
        prefetch_count=10,
        requeue_limit=3
    )

    # Non-blocking call
    await consumer.consume(message_router)
    
    # Prevent script termination
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
```

## Dead-letter queue
Durable consumer queues (`QueueType.DURABLE`, the default) are quorum queues with
`x-delivery-limit` (`requeue_limit`, default 3). A message whose handler keeps failing is
dead-lettered to `<exchange>_dlx`, which routes everything (`#`) to **one shared queue,
`<exchange>_dlq`**, for all services — there is no per-service DLQ. Messages are kept
there for 14 days (`x-message-ttl`).

Which service rejected a message, and why, is in the standard RabbitMQ `x-death` header
the broker adds when dead-lettering: `queue` (the consumer queue, i.e. the service),
`reason`, `count`, `time`, `exchange` and `routing-keys`.

To replay a dead letter, publish it to the queue named in `x-death[0].queue` (default
exchange, routing key = queue name). Re-publishing it to the main exchange with its
original routing key would deliver it again to every service bound to that key.

Messages that can never succeed are not retried:
* a body that fails validation against the handler's model (`RabbitMessageRouter`) is
  logged and dropped;
* a message with missing or invalid security headers or signature is logged and dropped.

`QueueType.TEMPORARY` queues have no dead-lettering: a failed message is dropped.

## Message headers
Every published message carries these headers (names are case-sensitive):

* `eventVersion`: event message version (integer, currently `1`).
* `eventTimestamp`: Unix epoch timestamp (UTC) of event creation.
* `tracingId`: UUID used to trace the event across consumers.
* `producerName`: name of the producing microservice.
* `producerId`: UUID of the producing microservice instance.
* `x-signature`: HMAC signature, described below.

## Security (HMAC Signature)
Every message is automatically signed to ensure integrity and authenticity.

### Signature Format:
`HMAC_SHA256(secret, payload)`

### Payload Format:
The payload to be signed is constructed as follows:

`<eventTimestamp>.<SortedJSONBodyBytes>`

* `<eventTimestamp>`: Unix epoch timestamp (integer).
* `.`: Dot separator.
* `<SortedJSONBodyBytes>`: JSON body with keys sorted alphabetically.


### Consumer Validation Logic:
The Consumer automatically performs the following checks:

1. Existence: Checks for the presence of the signature header.
2. Integrity: Validates the signature (protects against tampering with the body or the timestamp).
3. Freshness: Checks eventTimestamp. Messages older than 5 minutes are dropped.
4. Schema: Validates the structure and types of all required headers.

 
If any of these checks fail, the message is dropped (ACKed) immediately without being passed to the handler, and a security alert is logged.
