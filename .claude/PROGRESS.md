# kafka-native Progress

## Completed

### Foundation Migration (GHC 8.6 → 9.12)
- [x] Replaced `base-noprelude` → `base`, dropped custom `src/Prelude.hs`
- [x] Replaced `sockets` → `network` for TCP
- [x] Replaced `String.Ascii` → `ByteString` throughout
- [x] Replaced `builder` (Andrew Martin's) → `proto3-wire` `BuildR` reverse builder
- [x] Kept `bytesmith`, `byteslice`, `castagnoli` (all compile on GHC 9.12)
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
- [x] `Compression.hs` — stub (NoCompression only)

### Bug Fixes
- [x] Wired `ccAcks`/`ccClientId` from config into produce path
- [x] Fixed N-copies produce bug (single gather)
- [x] Fixed `bsToByteArray` (no intermediate `[Word8]` list)
- [x] Changed `clientId` from `"ruko"` to `"kafka-native"`
- [x] Implemented `flushProducer` with `BrokerFlush` op
- [x] Fixed `patchCorrelationIdLBS` two-chunk layout bug (was silently not patching)

### Compact Encoding (KIP-482)
- [x] Writer.hs: `unsignedVarInt`, `compactString`, `compactArray`, `taggedFields`, etc.
- [x] Combinator.hs: matching decoders + `skipTaggedFields`
- [x] 12 round-trip unit tests

### New APIs
- [x] ApiVersions (key 18, v0) — request + response + handshake in Broker.hs
- [x] InitProducerId (key 22, v0) — request + response (NOT YET WIRED IN)

### Mock Cluster + Integration Tests
- [x] `test/MockCluster.hs` — FFI bindings to librdkafka mock cluster
- [x] `test/IntegrationTests.hs` — 10 passing integration tests
- [x] OS-conditional lib paths (`if os(darwin)` / `if os(linux)`)

### Test Count
- **40 unit tests** passing
- **10 integration tests** passing

## In Progress (stopped mid-work)
- Config.hs has `ccMaxInFlight`, `ccIdempotent`, `ccRetries` fields added but NOT wired into Broker.hs
- ProduceResponse error parsing NOT implemented (dispatcher assumes success)
- Retry, idempotent delivery, in-flight limit NOT implemented

## Not Started
- Consumer rewrite using KafkaClient
- Protocol version upgrades (Kafka 2.3 → 4.2)
- Compression codecs
- Dynamic broker discovery from metadata
- Periodic metadata refresh
- Performance benchmarks
- ByteString-native CRC32C
