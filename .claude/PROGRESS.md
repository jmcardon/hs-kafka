# kafka-native Progress

## Completed

### Foundation Migration (GHC 8.6 → 9.10)
- [x] Replaced `base-noprelude` → `base`, dropped custom `src/Prelude.hs`
- [x] Replaced `sockets` → `network` for TCP
- [x] Replaced `String.Ascii` → `ByteString` throughout
- [x] Replaced `builder` (Andrew Martin's) → `proto3-wire` `BuildR` reverse builder
- [x] Kept `bytesmith`, `byteslice`, `castagnoli` (all compile on GHC 9.10)
- [x] Dropped `primitive-convenience`, `primitive-slice` (git deps), `cpu`, `ip`, `chronos`, `torsor`
- [x] Updated all 11 Request modules to use `BuildR`, return `BSL.ByteString`
- [x] Updated all 10 Response modules with explicit imports
- [x] Updated Common.hs: `Kafka` wraps `Socket`, simplified `KafkaException` ADT
- [x] Updated Writer.hs, Combinator.hs for new deps
- [x] Cabal set to `GHC2024`, removed git `source-repository-package` entries
- [x] All golden test files regenerated

### New Infrastructure
- [x] `Broker.hs` — per-broker green thread, sender/receiver, batching, correlation, reconnection
- [x] `Client.hs` — multi-broker, metadata cache, broker selection
- [x] `Producer.hs` — batching producer with sync/async produce + flushProducer
- [x] `Config.hs` — typed config with defaults
- [x] `Reconnect.hs` — exponential backoff + jitter

### ProduceResponse Error Parsing + Retry + In-flight Limit
- [x] Parse ProduceResponse error codes in Broker.hs dispatcher (v9 flexible format)
- [x] Match per-partition error codes to callbacks
- [x] `isRetriable` in Common.hs with Kafka's retriable error classification
- [x] Retry on retriable errors: re-enqueue with decremented retry counter
- [x] Fail with `KafkaProtocolException` when retries exhausted
- [x] In-flight request limit via `beInflightCount` TVar + STM `check`
- [x] `ccRetries`, `ccMaxInFlight` from Config.hs fully wired

### Idempotent Producer
- [x] `IdempotentRef` in BrokerEnv with PID/epoch/per-partition sequences
- [x] `setIdempotentState` to wire PID from Producer to Broker envs
- [x] `initIdempotentState` calls InitProducerId v5 API on startup
- [x] `produceRequestIdempotent` encodes PID/epoch/baseSequence in record batch
- [x] Per-partition sequence number allocation in `sendPartitionBatch`
- [x] `newProducer` returns `Either KafkaException KafkaProducer`

### Compression
- [x] Gzip compression via `zlib`
- [x] Snappy compression via `snappy` (FFI)
- [x] LZ4 compression via `lz4` (frame format per KIP-57)
- [x] Zstd compression via `zstd`
- [x] Falls back to uncompressed when compression doesn't reduce size
- [x] `produceRequestCompressed` — unified produce path with compression+idempotence
- [x] Compression wired into Broker.hs via `ccCompression` config

### Protocol Version Upgrades (Kafka 4.2)
- [x] ApiVersions v0 request (handshake) / v0+v3 response parsers
- [x] Produce v9 (flexible, compact topic/partition arrays, tagged fields)
- [x] Metadata v12 (flexible, compact encoding, UUID field, partition replicas as arrays)
- [x] InitProducerId v5 (flexible, ProducerId/Epoch fields from v3+)
- [x] Request header v2: legacy INT16 clientId + tagged fields (NOT compact clientId)
- [x] Both legacy and flexible parsers available for all upgraded APIs
- [x] Golden test files regenerated for v9 produce encoding

### Compact Encoding (KIP-482)
- [x] Writer.hs: `unsignedVarInt`, `compactString`, `compactArray`, `taggedFields`, etc.
- [x] Combinator.hs: matching decoders + `skipTaggedFields`
- [x] Round-trip unit tests

### Mock Cluster + Integration Tests
- [x] `test/MockCluster.hs` — FFI bindings to librdkafka mock cluster
- [x] `test/IntegrationTests.hs` — 16 integration tests
- [x] OS-conditional lib paths (`if os(darwin)` / `if os(linux)`)

### Benchmark
- [x] `bench/Benchmark.hs` — criterion benchmark using mock cluster
- [x] Compares kafka-native vs hw-kafka-client (librdkafka FFI)
- [x] Tests 1K/10K messages at 100B/1KB/10KB payload sizes

### Test Count
- **68 unit tests** passing
- **16 integration tests** (mock cluster required)

## In Progress

### Produce path performance optimization
kafka-native is 7-24x slower than hw-kafka-client. Root cause: ~15,000 heap
allocations per 1000-message batch from intermediate ByteArray allocations.
Fix plan: single-allocation MutableByteArray builder. See `.claude/improvement.md`.

## Not Started
- Consumer rewrite using KafkaClient
- Fetch v12 (flexible encoding upgrade)
- Topic UUID support (Produce v13+, Fetch v13+)
- Dynamic broker discovery from metadata
- Periodic metadata refresh
- ByteString-native CRC32C
