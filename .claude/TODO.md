# kafka-native TODO

## Immediate Priority: Produce Path Performance

kafka-native is 7-24x slower than hw-kafka-client on the criterion benchmark.
Root cause: ~15,000 heap allocations per 1000-message batch.
Fix plan is in `.claude/improvement.md` — 10 issues, 4 phases.

### Phase A — Single-allocation record batch builder (Issues 1-4, 7)
Create `src/Kafka/Internal/RecordBatch.hs` with a `MutableByteArray`-based
builder. Eliminates ~14,000 of ~15,000 allocations per batch.
- [ ] `writeZigzag` / `writeUvarint` — direct mutable buffer writes
- [ ] `zigzagSize` / `uvarintSize` — pure size calculators
- [ ] `writeRecordMetadata` — write record header directly into buffer
- [ ] `writeRecordBatchHeader` — write preCrc+postCrc (49 bytes) directly
- [ ] Remove `gatherChunks` (foldr (<>) is O(n^2))
- [ ] Remove `buildToBA` (BuildR → BSL → BS → [Word8] → ByteArray triple copy)
- [ ] Make `zero` a top-level CAF (or eliminate)

### Phase B — Correlation ID threading (Issue 5)
- [ ] Pass corrId into `buildProducePayload` so it's baked in from the start
- [ ] Remove `patchCorrelationIdLBS` (ByteString surgery ~5 allocs per request)

### Phase C — Full request in one buffer (Issue 6)
- [ ] Write entire produce request into single MutableByteArray
- [ ] Single ByteString conversion at send boundary
- [ ] Pre-calculate size from tracked offsets (no BSL.length traversal)

### Phase D — Cleanup (Issues 8-10)
- [ ] Fix `bsToBA` everywhere to use `Data.Bytes.toByteArrayClone`
- [ ] Consider `HashMap` for batch accumulation (O(1) vs O(log n) per message)
- [ ] Use `Network.Socket.ByteString.sendMany` for scatter-gather I/O

## Next Priority

### Consumer rewrite using KafkaClient
The current `src/Kafka/Consumer.hs` uses direct socket access (old `Kafka` type)
with an `MVar ()` mutex. It does NOT use the broker thread infrastructure.
Rewrite to:
- Use `KafkaClient` for all broker communication via `enqueueRequest`
- Route group ops to coordinator broker
- Route fetch requests to partition leader brokers
- Handle FindCoordinator response properly
- Support multiple topics

### Fetch v12 (flexible encoding)
Current Fetch is at v10 (pre-flexible). Upgrade to v12:
- Request header v2 (legacy clientId + tagged fields)
- Compact arrays for topics/partitions
- Add `RackId` field (v11+)

### Topic UUID support
Produce v13+, Fetch v13+, Metadata v10+ use UUIDs.
Need UUID type, TopicId cache in MetadataCache, fallback to name.

### Dynamic broker discovery
After `refreshTopicMetadata`, if metadata lists new brokers not in `kcBrokers`,
create `BrokerEnv` + start broker thread for them.

### Periodic metadata refresh
`forkIO` + `threadDelay` timer in KafkaClient that periodically refreshes
metadata (like librdkafka's 1-second scan).

## Reference

- Produce path optimization plan: `.claude/improvement.md`
- Efficiency rules: `.claude/EFFICIENCY.md`
- Progress tracking: `.claude/PROGRESS.md`
- librdkafka mock cluster API: `test/MockCluster.hs`
- librdkafka source: `../librdkafka/src/`
- hw-kafka-client: `/Users/josecardona/personal/hw-kafka-client/`
- Kafka 4.2 protocol spec: `https://kafka.apache.org/42/design/protocol`
- Kafka protocol JSON schemas: `https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message/`
