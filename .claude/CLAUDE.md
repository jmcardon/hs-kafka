# kafka-native — High-Performance Pure Haskell Kafka Client

Forked from hs-kafka (Ethan Jones, 2019). Goal: a production-grade, pure-Haskell
Kafka client that is significantly faster than hw-kafka-client (FFI to librdkafka)
by eliminating FFI overhead, unnecessary copies, and bad abstractions, while
replicating librdkafka's battle-tested threading, batching, and reliability
algorithms directly in Haskell.

## Build

```bash
cabal build                    # library
cabal test unit                # 40 unit tests
cabal test integration         # 10 integration tests (requires librdkafka: brew install librdkafka)
```

GHC 9.12+, `default-language: GHC2024`. Flags: `-Wall -O2`.

## Reference Repositories

When implementing features, refer to these codebases for algorithms:

- **librdkafka** (C reference): `../librdkafka/src/`
- **hw-kafka-client** (what we're replacing): `~/arista/hw-kafka-client/`
- **milena** (Haskell reference, obsolete): `../milena/`

## Current State (March 2026)

### What Works
- 13 Kafka API pairs (original 10 + ApiVersions v0 + InitProducerId v0 + compact encoding primitives)
- Message format v2 (record batches, CRC32c via castagnoli)
- Encoding via `proto3-wire` `BuildR` reverse builder, decoding via `bytesmith` (zero-copy)
- Multi-broker client (`Client.hs`) with metadata cache and broker selection
- Batching producer (`Producer.hs`) with linger.ms + batch.size + batch.num.messages
- Per-broker green threads (`Broker.hs`) with sender/receiver, correlation ID tracking
- Reconnection with exponential backoff + jitter (`Reconnect.hs`)
- Backpressure via bounded TBQueue
- ApiVersions handshake on every connection
- `flushProducer` to drain pending batches
- Configurable acks and clientId (wired into produce path)
- Compact encoding primitives ready (unsignedVarInt, compactString, compactArray, taggedFields)
- Mock cluster FFI bindings for integration testing (links to librdkafka)
- **40 unit tests + 10 integration tests passing**

### What's Fixed (was broken in original hs-kafka)
- ~~Single broker~~ → multi-broker client with metadata
- ~~No batching~~ → linger + size + count batching in broker thread
- ~~Hardcoded correlationId~~ → monotonic counter + inflight dispatch
- ~~Hardcoded clientId~~ → configurable via `ccClientId`
- ~~Hardcoded acks~~ → configurable via `ccAcks`
- ~~No reconnection~~ → exponential backoff + jitter
- ~~No backpressure~~ → bounded TBQueue

### What's Still Missing
- **ProduceResponse error parsing** — dispatcher assumes success, ignores error codes
- **No retry** — retriable errors not retried
- **No idempotent producer** — InitProducerId API exists but PID/epoch/sequences not tracked
- **No in-flight limit** — unlimited pipelining
- **No compression** — codec stubs only
- **Consumer not rewritten** — old single-connection code, not using KafkaClient
- **No dynamic broker discovery** — metadata updates leaders but doesn't add new brokers
- **No periodic metadata refresh**
- **FindCoordinator response still ignored** in old Consumer
- **Protocol versions at Kafka 2.3** — compact encoding ready but no APIs upgraded yet

### Migration from original hs-kafka
- `sockets` → `network` (TCP)
- `String.Ascii` → `ByteString` (topic names, client IDs, group names)
- `builder` (Andrew Martin's) → `proto3-wire` `BuildR` (encoding)
- Custom `Prelude.hs` → dropped, using base Prelude with explicit imports
- `base-noprelude` → `base`
- Dropped: `primitive-convenience`, `primitive-slice` (git deps), `cpu`, `ip`, `chronos`, `torsor`
- Kept: `bytesmith`, `byteslice`, `castagnoli`, `primitive`, `primitive-unlifted`, `contiguous`

## Architecture

### Source Layout
```
src/
  Kafka/
    Common.hs                         -- Core types: Kafka (wraps Socket), TopicName (wraps ByteString),
                                      --   KafkaException (ADT), withKafka, connectBroker
    Client.hs                         -- NEW: Multi-broker client, metadata cache, broker selection
    Producer.hs                       -- NEW: Batching producer (sync/async/flush)
    Consumer.hs                       -- OLD: Single-connection consumer (needs rewrite)
    Internal/
      Broker.hs                       -- NEW: Per-broker green thread (sender/receiver/batching/reconnect)
      Config.hs                       -- NEW: Typed config (BrokerAddress, Acknowledgments, etc.)
      Reconnect.hs                    -- NEW: Exponential backoff + jitter
      Compression.hs                  -- NEW: Compression stubs
      Writer.hs                       -- Encoding primitives (proto3-wire BuildR). Legacy + compact.
      Combinator.hs                   -- Decoding primitives (bytesmith). Legacy + compact.
      Zigzag.hs                       -- Varint zigzag encoding for record batches
      Request.hs                      -- Central request dispatch (sends via network)
      Response.hs                     -- Central response reader (recv + bytesmith parse)
      Request/Types.hs                -- Request data types for all API ops
      ShowDebug.hs                    -- Debug printing typeclass
      Topic.hs                        -- Topic metadata lookup
      ApiVersions/                    -- NEW: API key 18 (connection handshake)
      InitProducerId/                 -- NEW: API key 22 (idempotent producer, not yet wired)
      {Produce,Fetch,Metadata,...}/   -- Per-API-key Request.hs + Response.hs pairs
test/
  UnitTests.hs                        -- 40 unit tests
  IntegrationTests.hs                 -- 10 integration tests against librdkafka mock cluster
  MockCluster.hs                      -- FFI bindings to rd_kafka_mock_cluster_*
```

### Key Types
- `Kafka` — newtype over `Socket` from `network`
- `TopicName` — newtype over `ByteString`
- `GroupName` — newtype over `ByteString`
- `KafkaClient` — multi-broker client (`TVar (IntMap BrokerEnv)` + `TVar MetadataCache`)
- `KafkaProducer` — wraps `KafkaClient` + per-topic round-robin counters
- `BrokerEnv` — per-broker state (TBQueue, inflight map, reconnect, ApiVersions)
- `Consumer` — `ReaderT (TVar ConsumerState) (ExceptT KafkaException IO)` (OLD, needs rewrite)

### Serialization Pattern
Encoding (requests): `BuildR` from `proto3-wire` → `buildRequest` → `BSL.ByteString`.
Non-produce requests return `BSL.ByteString`. Produce request uses `UnliftedArray ByteArray`
internally for CRC computation, then gathers to `BSL.ByteString` at the end.
Writer.hs exports both legacy primitives (`string`, `array`, `int32`) and compact/flexible
primitives (`compactString`, `compactArray`, `unsignedVarInt`, `taggedFields`).

Decoding (responses): `bytesmith` `Parser` operating on `ByteArray`. Zero-copy.
Response.hs reads from socket via `network`, converts `ByteString` → `ByteArray` for parsing.
Combinator.hs exports both legacy and compact decoders.

## Target Architecture (librdkafka-inspired)

```
┌──────────────────────────────────────────────────────────┐
│  Application Layer                                        │
│  Producer.produce()  Consumer.poll()  Admin.createTopic() │
├──────────────────────────────────────────────────────────┤
│  Client Core                                              │
│  ┌────────────┐ ┌──────────────┐ ┌────────────────────┐  │
│  │ Metadata   │ │ Correlation  │ │ Config             │  │
│  │ Cache      │ │ Tracking     │ │ (typed, validated) │  │
│  │ (TVar)     │ │ (TVar IntMap)│ │                    │  │
│  └────────────┘ └──────────────┘ └────────────────────┘  │
├──────────────────────────────────────────────────────────┤
│  Broker Manager (one forkIO green thread per broker)      │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ BrokerThread                                         │  │
│  │  ├─ TBQueue BrokerOp         (incoming operations)   │  │
│  │  ├─ Socket (GHC IO manager)  (non-blocking I/O)      │  │
│  │  ├─ BatchAccumulator         (linger + size + count)  │  │
│  │  ├─ ReconnectState           (exp backoff + jitter)   │  │
│  │  └─ ResponseDispatch         (correlationId → TMVar)  │  │
│  └─────────────────────────────────────────────────────┘  │
├──────────────────────────────────────────────────────────┤
│  Two-Level Queue (Producer)                               │
│  ┌───────────────────┐     ┌──────────────────────────┐  │
│  │ Partition MsgQueue │ ──→ │ Broker XmitQueue         │  │
│  │ (TBQueue, shared)  │     │ (local, no contention)   │  │
│  └───────────────────┘     └──────────────────────────┘  │
├──────────────────────────────────────────────────────────┤
│  Protocol Layer                                           │
│  Encode: proto3-wire reverse builder (or builder)         │
│  Decode: bytesmith zero-copy parser                       │
│  CRC32c: castagnoli (hw-accelerated)                      │
│  Compression: gzip / snappy / lz4 / zstd                  │
├──────────────────────────────────────────────────────────┤
│  Network: GHC IO Manager (epoll/kqueue) + Network.Socket  │
└──────────────────────────────────────────────────────────┘
```

## Implementation Plan — librdkafka Algorithm Ports

Each section below maps a librdkafka algorithm to the Haskell implementation.
File references are relative to `../librdkafka/src/`.

**Status key**: DONE = implemented + tested, PARTIAL = exists but incomplete, TODO = not started.

---

### Phase 1: Broker Manager + Multi-Broker — DONE

**Goal**: Replace single `Kafka` connection with a broker manager that discovers
brokers from metadata and runs one green thread per broker.

**librdkafka reference**:
- `rdkafka_broker.c:rd_kafka_broker_add()` (line ~5049) — creates broker + thread
- `rdkafka_broker.c:rd_kafka_broker_thread_main()` (line ~4505) — broker thread loop
- `rdkafka.c:rd_kafka_thread_main()` (line ~2241) — main coordinator thread
- `rdkafka_broker.c` states: INIT, DOWN, TRY_CONNECT, CONNECT, UP

**Haskell design**:
```haskell
data BrokerState = BrokerInit | BrokerDown | BrokerConnecting | BrokerUp

data Broker = Broker
  { brokerId        :: {-# UNPACK #-} !Int32
  , brokerHost      :: !ByteArray
  , brokerPort      :: {-# UNPACK #-} !Int32
  , brokerOps       :: !(TBQueue BrokerOp)    -- like rkb->rkb_ops
  , brokerState     :: !(TVar BrokerState)
  , brokerThread    :: !(Async ())
  }

data BrokerOp
  = SendRequest !Request !(TMVar Response)    -- request + where to put response
  | Shutdown                                   -- clean shutdown

data KafkaClient = KafkaClient
  { clientBrokers       :: !(TVar (IntMap Broker))   -- nodeId → Broker
  , clientMetadata      :: !(TVar MetadataCache)
  , clientNextCorrId    :: !(IORef Int32)             -- monotonic counter
  , clientConfig        :: !ClientConfig
  }
```

Each broker thread:
1. Connects to broker (with backoff on failure)
2. Loops: read from `brokerOps` TBQueue, send request, read response, dispatch via correlation ID
3. On socket error: transition to BrokerDown, reconnect with backoff

**Metadata refresh**: periodic timer (like librdkafka's 1-second scan in main thread).
Use `forkIO` + `threadDelay` or `registerDelay` + STM.

---

### Phase 2: Request Correlation — DONE

**Goal**: Replace hardcoded `correlationId = 0xbeef` with monotonic counter +
response dispatch map.

**librdkafka reference**:
- `rdkafka_int.h:rk_conf.max_msg_size` — tracks in-flight requests
- `rdkafka_broker.c:rd_kafka_broker_buf_enq1()` — assigns correlation ID
- Response reading matches correlation ID to pending request

**Haskell design**:
```haskell
-- In KafkaClient:
clientNextCorrId :: IORef Int32  -- atomicModifyIORef' to get next ID

-- In each BrokerThread:
brokerInflight :: TVar (IntMap (TMVar Response))  -- corrId → response slot
```

Flow:
1. `sendRequest` atomically gets next correlation ID
2. Creates `TMVar` for the response
3. Inserts into `brokerInflight` map
4. Sends request bytes with that correlation ID
5. Response reader thread reads correlation ID from response, looks up TMVar, fills it
6. Caller blocks on `readTMVar`

---

### Phase 3: Two-Level Producer Queue — PARTIAL (queue directly to broker, no per-partition queues)

**Goal**: Implement librdkafka's partition queue → broker transmit queue pattern
to minimize lock contention.

**librdkafka reference**:
- `rdkafka_partition.c:rd_kafka_toppar_enq_msg()` (line ~695) — enqueue to partition
- `rdkafka_broker.c:rd_kafka_broker_produce_toppars()` (line ~4226) — drain partitions
- `rdkafka_msg.h:rd_kafka_msgq_t` — partition message queue structure
- `rdkafka_broker.c:rd_kafka_toppar_producer_serve()` (line ~3872) — move msgs to xmit queue

**Haskell design**:
```haskell
-- Level 1: Per-partition queue (shared between app thread + broker thread)
data PartitionQueue = PartitionQueue
  { pqMessages  :: !(TBQueue ProduceMessage)   -- bounded for backpressure
  , pqLeader    :: !(TVar Int32)               -- current leader broker ID
  }

-- Level 2: Broker-local transmit list (only touched by broker thread, no contention)
-- This is just a local [ProduceMessage] or Seq in the broker thread's loop
```

App thread does: partition message → enqueue to `pqMessages` (STM, brief)
Broker thread does: drain all partition queues it leads → accumulate locally → batch

---

### Phase 4: Batching (linger.ms + batch.size + batch.num.messages) — DONE

**Goal**: Accumulate messages and send in batches, matching librdkafka's strategy.

**librdkafka reference**:
- `rdkafka_msg.c:rd_kafka_msgq_allow_wakeup_at()` (line ~1779) — batch readiness check
- `rdkafka_msg.h:rd_kafka_msgq_may_wakeup()` (line ~432) — wakeup condition
- Config: `queue.buffering.max.ms` (linger), `batch.size`, `batch.num.messages`

**Batch is ready when ANY of**:
1. Message count >= `batchNumMessages` (default 10000)
2. Total bytes >= `batchSize` (default 1000000)
3. Time since oldest msg enqueue >= `lingerMs` (default 5ms)
4. Flush requested (linger overridden to 0)

**Haskell design**:
```haskell
data BatchConfig = BatchConfig
  { lingerUs         :: !Int          -- microseconds (linger.ms * 1000)
  , batchSizeBytes   :: !Int          -- max bytes per batch
  , batchNumMessages :: !Int          -- max messages per batch
  }

-- In broker thread loop:
brokerProducerServe :: Broker -> BatchConfig -> IO ()
brokerProducerServe broker cfg = do
  -- Drain partition queues into local accumulator
  msgs <- drainPartitionQueues broker
  -- Check batch readiness using STM + registerDelay
  -- If linger timer expires OR size/count threshold hit → encode + send batch
```

Timer implementation: `registerDelay lingerUs` creates a `TVar Bool`.
Combine with `STM` `orElse`:
- Wait for either: new messages arrive in TBQueue, OR linger timer fires
- On either event: check batch thresholds, send if ready

This replaces librdkafka's condvar + pipe-fd wakeup mechanism naturally.

---

### Phase 5: Wakeup Management — DONE (free from STM)

**Goal**: Avoid redundant broker thread wakeups (matches librdkafka's `signalled` flag).

**librdkafka reference**:
- `rdkafka_msg.h:rd_kafka_msgq_may_wakeup()` — only wake on first msg to empty queue,
  or when thresholds exceeded. Uses `signalled` flag to prevent duplicate wakeups.
- `rdkafka_partition.c:rd_kafka_toppar_enq_msg()` — calls `rd_kafka_q_yield()` conditionally

**Haskell design**: STM handles this naturally! `TBQueue` operations only wake
blocked threads when there's actually something to read. No explicit wakeup
management needed — this is a freebie from STM.

The broker thread's `atomically` block combining TBQueue reads with timer
TVars handles the wakeup logic implicitly. No pipe fds, no condvars, no
signalled flags.

---

### Phase 6: Reconnection with Exponential Backoff + Jitter — DONE

**Goal**: When a broker connection drops, reconnect with backoff matching
librdkafka's algorithm.

**librdkafka reference**:
- `rdkafka_broker.c` (line ~2221-2269) — backoff calculation
- Algorithm:
  1. Initial backoff: `reconnect.backoff.ms` (default 100ms)
  2. After each failure: `backoff = backoff * 2`, capped at `reconnect.backoff.max.ms` (default 10000ms)
  3. Jitter: random value between 80% and 120% of current backoff (NOTE: librdkafka actually uses 75%-150% range via `rd_jitter()`)
  4. If no connection attempt for > max_backoff, reset to initial

**Haskell design**:
```haskell
data ReconnectState = ReconnectState
  { reconnectBackoffMs    :: !Int    -- current backoff (doubles each failure)
  , reconnectInitialMs    :: !Int    -- initial backoff (default 100)
  , reconnectMaxMs        :: !Int    -- cap (default 10000)
  , reconnectLastAttempt  :: !Int64  -- timestamp of last attempt
  }

reconnectWithBackoff :: ReconnectState -> IO (Connection, ReconnectState)
reconnectWithBackoff st = do
  jitter <- randomRIO (75, 150)  -- 75-150% like librdkafka's rd_jitter
  let delayMs = (reconnectBackoffMs st * jitter) `div` 100
  threadDelay (delayMs * 1000)
  tryConnect >>= \case
    Right conn -> pure (conn, st { reconnectBackoffMs = reconnectInitialMs st })
    Left _err  -> reconnectWithBackoff st
      { reconnectBackoffMs = min (reconnectBackoffMs st * 2) (reconnectMaxMs st) }
```

---

### Phase 7: Backpressure — DONE (bounded TBQueue)

**Goal**: Limit how many messages can be buffered, matching librdkafka's
`queue.buffering.max.messages` and `queue.buffering.max.kbytes`.

**librdkafka reference**:
- `rdkafka_int.h:rd_kafka_curr_msgs_add()` (line ~792) — blocks or returns QUEUE_FULL
- `rdkafka_broker.c:rd_kafka_broker_outbufs_space()` (line ~3831) — limits outbufs

**Haskell design**: Use `TBQueue` (bounded) for partition queues. When full,
`atomically . writeTBQueue` will block (backpressure). Alternatively, use
`tryWriteTBQueue` to return immediately with a QUEUE_FULL error.

Global byte counter via `TVar Int64` — increment on enqueue, decrement on
delivery report. Check before accepting new messages.

---

### Phase 8: Compression — TODO (stubs only)

**Goal**: Support gzip, snappy, lz4, zstd compression on produce batches.

**librdkafka reference**:
- `rdkafka_msgset_writer.c:rd_kafka_msgset_writer_finalize()` (line ~1362) —
  compresses after all records written to batch
- `rdkafka_msgset_writer.c` lines 1039-1157 — per-codec compression functions
- If compressed > uncompressed, fall back to uncompressed

**Haskell design**: Apply compression to the record batch payload section
(everything after the CRC). Set bits 0-2 of `recordBatchAttributes`:
0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd.

Packages: `zlib` (gzip), `snappy` (via FFI or pure), `lz4-hs`, `zstd`.

---

### Phase 9: Idempotent Producer — TODO (InitProducerId API exists, not wired in)

**Goal**: Support exactly-once semantics via PID + epoch + per-partition sequences.

**librdkafka reference**:
- `rdkafka_idempotence.c` — full state machine
- States: INIT → REQ_PID → WAIT_TRANSPORT → WAIT_PID → ASSIGNED
- `rdkafka_int.h:rd_kafka_idemp_state_t` (line ~144)
- Per-partition: `rktp_eos.epoch_base_msgid`, `rktp_eos.pid`
- Uses InitProducerId API (API key 22)

**Haskell design**:
```haskell
data IdempotentState
  = IdempInit
  | IdempWaitingForPid
  | IdempAssigned !ProducerId !ProducerEpoch

data ProducerPartitionState = ProducerPartitionState
  { ppsNextSequence :: !Int32        -- per-partition sequence number
  , ppsInflight     :: !(IntMap ProduceBatch)  -- sequence → batch for retry
  }
```

Requires implementing InitProducerId request/response (API key 22, not currently
in hs-kafka).

---

### Phase 10: Configuration System — DONE (Config.hs)

**Goal**: Replace hardcoded values with a typed configuration.

```haskell
data ClientConfig = ClientConfig
  { configBootstrapServers  :: !(NonEmpty (ByteArray, Int32))  -- host:port pairs
  , configClientId          :: !ByteArray
  , configAcks              :: !Acknowledgments
  , configLingerMs          :: !Int           -- default 5
  , configBatchSize         :: !Int           -- default 1000000
  , configBatchNumMessages  :: !Int           -- default 10000
  , configMaxInFlight       :: !Int           -- default 5
  , configReconnectBackoff  :: !Int           -- default 100ms
  , configReconnectMax      :: !Int           -- default 10000ms
  , configRequestTimeoutMs  :: !Int           -- default 30000
  , configMetadataMaxAgeMs  :: !Int           -- default 300000
  , configCompression       :: !Compression   -- None | Gzip | Snappy | Lz4 | Zstd
  , configIdempotent        :: !Bool          -- default False
  }
```

## Coding Conventions

- **Strict fields**: Always use `{-# UNPACK #-}` on numeric fields, `!` on all fields
- **No custom Prelude**: Using base's standard Prelude. Each module has explicit imports.
- **ByteString for user-facing data**: Topic names, client IDs, group names are `ByteString`.
- **ByteArray for internal protocol data**: `bytesmith` parses `ByteArray`, `castagnoli` CRCs `ByteArray`.
  `UnliftedArray ByteArray` used in Produce/Request.hs for CRC scatter-gather.
- **BuildR for encoding**: `proto3-wire`'s reverse builder. Request modules return `BSL.ByteString`.
- **Big-endian**: Kafka protocol is big-endian. Writer.hs uses `R.int32BE` etc.
- **Error handling**: Use `KafkaException` ADT. Prefer `Either KafkaException a`.
  Never use `error` or `undefined`.
- **GHC2024**: `default-language: GHC2024` — `ScopedTypeVariables`, `LambdaCase`,
  `OverloadedStrings` etc. are enabled by default. Avoid TemplateHaskell.
- **See `.claude/EFFICIENCY.md`** for produce path efficiency rules (read before committing).

## Key librdkafka Files Quick Reference

| Feature | librdkafka file | Key function |
|---------|----------------|--------------|
| Broker thread main loop | `rdkafka_broker.c:4505` | `rd_kafka_broker_thread_main` |
| Producer serve | `rdkafka_broker.c:4226` | `rd_kafka_broker_producer_serve` |
| Batch readiness | `rdkafka_msg.c:1779` | `rd_kafka_msgq_allow_wakeup_at` |
| Wakeup management | `rdkafka_msg.h:432` | `rd_kafka_msgq_may_wakeup` |
| Message enqueue | `rdkafka_partition.c:695` | `rd_kafka_toppar_enq_msg` |
| Reconnection backoff | `rdkafka_broker.c:2221` | (inline in state machine) |
| ProduceRequest encoding | `rdkafka_msgset_writer.c:1452` | `rd_kafka_msgset_create_ProduceRequest` |
| Compression | `rdkafka_msgset_writer.c:1157` | `rd_kafka_msgset_writer_compress_*` |
| Idempotent producer | `rdkafka_idempotence.c` | `rd_kafka_idemp_init` |
| Transaction manager | `rdkafka_txnmgr.c` | `rd_kafka_txn_*` |
| Op queue | `rdkafka_queue.h:53` | `rd_kafka_q_t` struct |
| Transport I/O | `rdkafka_transport.c:974` | `rd_kafka_transport_io_serve` |
| Main thread | `rdkafka.c:2241` | `rd_kafka_thread_main` |
| Broker add | `rdkafka_broker.c:5049` | `rd_kafka_broker_add` |
| Outbuf backpressure | `rdkafka_broker.c:3831` | `rd_kafka_broker_outbufs_space` |
| Toppar producer serve | `rdkafka_broker.c:3872` | `rd_kafka_toppar_producer_serve` |

## librdkafka → Haskell Translation Table

| librdkafka concept | Haskell equivalent |
|-------------------|-------------------|
| OS thread per broker | `forkIO` green thread (~0.5KB) |
| `mutex_t + cnd_t` | `TVar` / `TMVar` via STM |
| `rd_kafka_q_t` (op queue) | `TBQueue BrokerOp` |
| pipe fd wakeup | Not needed — STM wakes blocked threads automatically |
| `poll()` event loop | GHC IO manager (transparent epoll/kqueue) |
| `rd_atomic32_t` | `IORef` + `atomicModifyIORef'` |
| `TAILQ` linked list | `Seq` or `[]` (for broker-local) |
| `rd_buf_t` scatter-gather | `UnliftedArray ByteArray` + `sendMany` |
| `rd_kafka_msgq_t` | `TBQueue ProduceMessage` (bounded = backpressure) |
| `registerDelay` + condvar timeout | `registerDelay :: Int -> IO (TVar Bool)` (exact same!) |
| `sendmsg()` with iovec | GHC `writev` via `sendMany` / `Network.Socket.ByteString.sendMany` |
| `rd_jitter(lo,hi)` | `randomRIO (lo, hi)` |
| Linger timer | `registerDelay lingerUs` + STM `orElse` with TBQueue read |
