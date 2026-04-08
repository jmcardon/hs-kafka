# kafka-native Progress

## Performance
- **3-4x faster than hw-kafka-client (librdkafka FFI)** on all payload sizes
- Wire parser: 4-7x faster than bytesmith
- Core/ASM verified: no register spills, no boxing in hot loops

## Completed

### Wire Parser (Kafka.Internal.Wire)
- [x] flatparse-inspired parser on Addr# from ByteString ForeignPtr
- [x] Native-endian load + byteSwap (single BSWAP instruction per read)
- [x] Zero-copy ByteString slices via ForeignPtr sharing
- [x] All 13 response parsers migrated from bytesmith to Wire
- [x] 71 Wire parser unit tests

### RecordBatch Builder (Kafka.Internal.RecordBatch)
- [x] Single pinned ForeignPtr allocation for entire batch
- [x] Ptr pokes for header fields (REV + STR on ARM64)
- [x] Direct FFI CRC32C on Ptr (hardware accelerated, no ByteString wrapper)
- [x] writeRecord fully inlined, all args unboxed in Core
- [x] pokeUvarint: 7-instruction tight loop, no spills

### Produce Path (ByteString end-to-end)
- [x] buildProduceRequest: corrId baked in, returns strict ByteString
- [x] PendingMessage.pmPayload is ByteString (was ByteArray)
- [x] produce/produceAsync accept ByteString payloads
- [x] compressBatch operates on ByteString directly (no BA↔BS round-trip)
- [x] NBS.sendAll for strict send (single syscall)
- [x] No patchCorrelationIdLBS on produce path
- [x] No UnliftedArray, no messagesToPayloadArray
- [x] computeRecordsSizeAndCount: single pass via unboxed tuple

### Infrastructure
- [x] Multi-broker client with metadata cache + node ID re-keying
- [x] Batching producer (linger.ms + batch.size + batch.num.messages)
- [x] race_ for sender/receiver (structured concurrency)
- [x] ProduceResponse error parsing + retry + in-flight limit
- [x] Idempotent producer (PID/epoch/sequences)
- [x] All 4 compression codecs (gzip, snappy, lz4, zstd)
- [x] Response header v1 tagged fields in all flexible parsers
- [x] Records field as compact bytes in Produce v9

### Dependencies
- [x] Removed castagnoli (replaced by digest CRC32C)
- [x] bytesmith replaced by Wire (kept in cabal for bench comparison only)
- [x] No ByteArray in produce hot path

### Tests
- **126 unit tests** (including 71 Wire parser tests)
- **28 integration tests** (mock cluster)
- Wire parser benchmark (vs bytesmith)
- Throughput benchmark (vs hw-kafka-client)

## Not Started
- ProducerRecord type (key, headers, partition selection)
- Delivery reports with offset
- Key-based partitioning (murmur2)
- Record headers (KIP-82)
- Consumer rewrite
- Remove dead code (Combinator.hs, Zigzag.hs, ShowDebug.hs)
- Fetch v12, Topic UUIDs, dynamic broker discovery
