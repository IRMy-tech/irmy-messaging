# Changelog
## 1.0.0
First release of `irmy-messaging` (import package `irmy_messaging`)

* Dead letters of all services go to one shared quorum queue per exchange,
  `<exchange>_dlq` (bound to `<exchange>_dlx` with `#`, 14-day `x-message-ttl`); no
  per-service DLQs. The rejecting queue/service is in the broker's `x-death` header.
* `RabbitMessageRouter` drops (acks) a message whose body fails validation against the
  handler's model instead of requeueing it into the dead-letter queue.
* `RabbitConsumer.delete_queues()` deletes only the consumer's own queues; the shared
  `<exchange>_dlq` is left in place.
* Routing keys given as `(str, Enum)` members (e.g. `irmy_events.RoutingKeys`) are
  converted to their value in `RabbitProducer.publish`, consumer bindings and
  `RabbitMessageRouter.register`, so metric labels and logs show
  `audit.service.create` instead of `RoutingKeys.AUDIT_SERVICE_CREATE`.
