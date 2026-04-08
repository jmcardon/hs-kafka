# kafka-native TODO

## Completed
- [x] Wire parser (4-7x faster than bytesmith)
- [x] Single-alloc RecordBatch (3-4x faster than librdkafka)
- [x] Key/value/header encoding, murmur2 partitioner, timestamps
- [x] ProducerRecord + DeliveryReport + pollEvents
- [x] Callbacks in poller thread
- [x] Queue forwarding
- [x] Dynamic broker discovery + periodic metadata refresh
- [x] Log/error/stats callback config + invocation on connect/disconnect
- [x] message.timeout.ms config + enforcement in flushBatch
- [x] Priority ops (separate control channel for flush/shutdown)
- [x] Sticky partitioner (keyless → same partition per batch)
- [x] Fetch v12 request/response (flexible encoding)
- [x] New consumer scaffold (Consumer.New with KafkaClient)

## High Priority

### Request assembly optimization
buildProduceRequest assembles via BSL concatenation + BSL.toStrict (copy).
Should pre-compute total size and write into single pinned buffer.
The record batch (99% of bytes) is already optimal — this is the ~40 byte
header that's assembled suboptimally.

### Consumer integration tests
Consumer.New compiles but needs integration tests: subscribe, fetch,
commit, rebalance, multi-partition, auto-commit.

### Consumer auto-commit timer
ccAutoCommitMs is configured but no timer thread yet.

## Medium Priority

### SASL/SSL authentication
### Transactional producer (state machine)
### Admin APIs (CreateTopics, etc.)
### Topic-level config overrides

## Low Priority

### High-performance queue investigation
TBQueue vs unagi-chan vs ring buffer for 100k+ msg/sec.
### Remove dead code (Combinator.hs, old Consumer.hs, Zigzag.hs)
### Remove bytesmith/byteslice from library deps
