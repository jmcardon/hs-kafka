# kafka-native Progress

## Completed

### Foundation Migration (GHC 8.6 → 9.10)
- [x] Replaced `base-noprelude` → `base`, dropped custom `src/Prelude.hs`
- [x] Replaced `sockets` → `network` for TCP
- [x] Replaced `String.Ascii` → `ByteString` throughout
- [x] Replaced `builder` (Andrew Martin's) → `proto3-wire` `BuildR` reverse builder
- [x] Dropped `primitive-convenience`, `primitive-slice`, `cpu`, `ip`, `chronos`, `torsor`
- [x] Cabal set to `GHC2024`

### Zero-Allocation Wire Parser
- [x] `Wire.hs` — flatparse-inspired parser on raw `Addr#` from `ByteString`
- [x] Fixed-size big-endian reads via `indexWordXXOffAddr#` + `byteSwap` (single BSWAP insn)
- [x] Zero-copy `ByteString` slices via `ForeignPtr` sharing
- [x] Failure propagation via `unsafeCoerce#`
- [x] 4-7x faster than bytesmith (benchmarked)
- [x] **All 13 response parsers migrated from bytesmith to Wire**
- [x] Response.hs returns `ByteString` (no more `ByteArray` in the response path)
- [x] `Common.hs` types migrated: `GroupMember`, `MemberAssignment` use `ByteString`

### Broker Infrastructure
- [x] `Broker.hs` — per-broker green thread, sender/receiver via `race_` (async)
- [x] `Client.hs` — multi-broker, metadata cache, broker selection, node ID re-keying
- [x] `Producer.hs` — batching producer with sync/async produce + flushProducer
- [x] `Config.hs` — typed config with defaults
- [x] `Reconnect.hs` — exponential backoff + jitter

### STM Consolidation
- [x] Merged redundant `atomically` blocks throughout (stopBroker, closeClient, etc.)
- [x] `anyBroker`/`leaderBrokerFor` use single consistent STM snapshot
- [x] `flushBatch` uses `Map.foldlWithKey'` (no intermediate list)
- [x] Dead `beThread` field removed from BrokerEnv

### ProduceResponse Error Parsing + Retry + In-flight Limit
- [x] Parse ProduceResponse error codes (v9 flexible format) via Wire
- [x] Retry on retriable errors with configurable retries
- [x] In-flight request limit via STM `check`

### Idempotent Producer
- [x] `IdempotentRef` with PID/epoch/per-partition sequences
- [x] `initIdempotentState` calls InitProducerId v5 API
- [x] Per-partition sequence number allocation

### Compression
- [x] Gzip (zlib), Snappy (FFI), LZ4 (frame), Zstd — all 4 codecs
- [x] Falls back to uncompressed on failure (matches librdkafka)

### Protocol Version Upgrades (Kafka 4.2)
- [x] Produce v9 (flexible, compact encoding, UVARINT records length)
- [x] Metadata v12 (flexible, UUID field, header v1 tagged fields)
- [x] InitProducerId v5 (flexible, header v1 tagged fields)
- [x] ApiVersions v0/v3 response parsers
- [x] Response header v1 tagged fields in all flexible parsers

### Mock Cluster + Integration Tests
- [x] `MockCluster.hs` — FFI bindings: create/destroy, topic/broker management
- [x] `mockPushRequestErrors`, `mockClearRequestErrors`, `mockPartitionSetLeader`, `mockBrokerSetRtt`
- [x] 28 integration tests: multi-broker, leader failover, retry, backpressure, RTT, all compression codecs

### Test Count
- **126 unit tests** passing (including 71 Wire parser tests)
- **28 integration tests** passing

### Benchmarks
- Wire parser: 4-7x faster than bytesmith
- Throughput: 1.6x slower than hw-kafka-client at 100B (encode bottleneck, not parse)

## In Progress

### Produce path encode performance
Still 1.6-10x slower than hw-kafka-client on the encode path.
Root cause: ~15,000 heap allocations per 1000-message batch from intermediate
ByteArray allocations in zigzag/makeRecordMetadata/gatherChunks.
Fix: single-allocation MutableByteArray builder (see `.claude/improvement.md`).

## Not Started
- Consumer rewrite using KafkaClient
- Fetch v12 (flexible encoding upgrade)
- Topic UUID support
- Dynamic broker discovery (add new brokers from metadata)
- Periodic metadata refresh
- Remove bytesmith dependency entirely (still in cabal, no longer used by src/)
