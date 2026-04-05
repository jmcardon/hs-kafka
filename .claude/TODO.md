# kafka-native TODO

## Completed: Produce Path Performance ✓

All 10 issues from `.claude/improvement.md` addressed:
- [x] Phase A: Single-allocation RecordBatch via pinned ForeignPtr + Ptr pokes
- [x] Phase B: Correlation ID baked into buildProduceRequest (no patching)
- [x] Phase C: Strict ByteString output, NBS.sendAll (single syscall)
- [x] Phase D: ByteString end-to-end, no ByteArray in produce path

Result: 3-4x faster than hw-kafka-client (librdkafka FFI).

## Immediate Priority: Producer API + Delivery Reports

### ProducerRecord type
Need a proper message type matching librdkafka's `rd_kafka_message_t`:
- Topic + partition selection (explicit or auto)
- Key (for partitioning by key hash)
- Value (the payload)
- Headers (KIP-82)
- Timestamp (optional, broker assigns if absent)

Currently only have `produce :: ByteString -> IO (Either KafkaException ())`.
Need: `produce :: ProducerRecord -> IO (Either KafkaException DeliveryReport)`.

### Delivery Reports / Callbacks
librdkafka's `dr_msg_cb` pattern: per-message callback with offset on success
or error on failure. Currently we have `TMVar (Either KafkaException ())` which
gives success/failure but not the offset. Need:
- Offset in success case
- Original ProducerRecord in both success and failure cases
- Polling API: `pollDeliveryReports :: KafkaProducer -> IO [DeliveryReport]`

### Key-based Partitioning
Currently round-robin only. Need murmur2 hash (Kafka's default partitioner)
on the key to route to the correct partition. librdkafka reference:
`rdkafka_msg.c:rd_kafka_msg_partitioner_murmur2`.

### Record Headers (KIP-82)
Record format v2 supports headers: list of (key, value) pairs.
Currently hardcoded `headerCount = 0` in RecordBatch.hs.
Need to encode headers in writeRecord.

## Next Priority

### Consumer rewrite using KafkaClient
The current Consumer uses direct socket access with MVar mutex.
Rewrite to use broker thread infrastructure.

### Remove dead code
- `Combinator.hs` — fully replaced by Wire, unused
- `Zigzag.hs` — only used by zigzag unit tests, RecordBatch has inline zigzag
- `ShowDebug.hs` — old debug printing, unused by new code
- `bytesmith`/`byteslice` — remove from library deps (keep in bench for comparison)

### Fetch v12 (flexible encoding)
Current Fetch is at v10. Upgrade to v12 for flexible encoding.

### Topic UUID support
Produce v13+, Fetch v13+, Metadata v10+ use UUIDs.

### Dynamic broker discovery
After metadata refresh, add new brokers from metadata response.

### Periodic metadata refresh
Timer in KafkaClient for periodic metadata updates.

## Reference
- Performance analysis: `.claude/improvement.md`
- librdkafka source: `../librdkafka/src/`
- flatparse source: `../flatparse/`
- bytesmith source: `../bytesmith/`
