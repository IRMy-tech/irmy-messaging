from prometheus_client import Counter, Histogram

# Producer metrics
messages_published_total = Counter(
    "messages_published_total",
    "Number of published messages",
    ["routing_key"]
)

messages_publish_duration_seconds = Histogram(
    "messages_publish_duration_seconds",
    "Histogram for counting how much time took to produce event",
    ["status", "routing_key"]
)

messages_publish_errors_total = Counter(
    "messages_publish_errors_total",
    "Number of messages failed to publish",
    ["routing_key"]
)

# Consumer metrics
messages_consumed_total = Counter(
    "messages_consumed_total",
    "Number of consumed messages",
    ["status", "queue"]
)

messages_processing_duration_seconds = Histogram(
    "messages_processing_duration_seconds",
    "Message processing time",
    ["status", "queue"]
)

messages_requeued_total = Counter(
    "messages_requeued_total",
    "Messages that were requeued",
    ["queue"]
)

messages_rejected_total = Counter(
    "messages_rejected_total",
    "Number of messages rejected to process",
    ["queue"]
)
