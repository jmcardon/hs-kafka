# kafka-native TODO

## Completed
- [x] Produce path performance (3-4x faster than librdkafka)
- [x] Wire parser (4-7x faster than bytesmith)
- [x] Key/value/header encoding in record batch
- [x] Murmur2 partitioner (Java Kafka compatible)
- [x] Timestamps in record batches
- [x] ProducerRecord + DeliveryReport + pollEvents API
- [x] Callbacks in poller thread (not broker thread)
- [x] Queue forwarding
- [x] Dynamic broker discovery from metadata
- [x] Periodic metadata refresh timer
- [x] Log/error/stats callback config types
- [x] message.timeout.ms config

## High Priority

### Consumer rewrite using KafkaClient
The biggest remaining gap. Current Consumer uses direct socket access.
Needs: KafkaClient broker threads, Wire parser, fetch/commit/rebalance.

### Callback invocation points
Log/error/stats callbacks are configured but not invoked anywhere yet.
Wire up: broker connect/disconnect → error callback, metadata refresh → log,
queue stats → stats callback.

### message.timeout.ms enforcement
Config exists but messages don't actually expire in the batch queue.
Need: check message age in senderLoop, fail expired messages.

## Medium Priority

### SASL/SSL
No authentication. Need TLS via Haskell `tls` package, SASL-PLAIN/SCRAM.

### Fetch v12 (flexible encoding)
Current Fetch at v10. Upgrade for compact encoding + tagged fields.

### Priority ops
Control messages (shutdown, flush) should jump ahead of produce ops.
TBQueue is FIFO — need priority queue or separate control channel.

### Sticky partitioner
librdkafka's default since 2.4. Sticky to one partition per batch
for better batching efficiency. Currently round-robin.

### High-performance queue investigation
Research faster alternatives to TBQueue for the delivery report path.
Need to support 100k+ msg/sec without backpressure. Consider:
- Unagi-chan
- Lock-free ring buffers
- Batched queue (push [DeliveryEntry] instead of one-at-a-time)

## Low Priority

### Transactional producer
InitProducerId API exists. Need full txn state machine.

### Admin APIs
CreateTopics, DeleteTopics, etc.

### Topic-level config
Per-topic overrides for compression, acks, etc.

### Remove dead code
Combinator.hs, Zigzag.hs, ShowDebug.hs — all replaceable/dead.
bytesmith/byteslice — remove from library deps.
