# kafka-native TODO

## Immediate (was in progress when session ended)

Config.hs already has `ccMaxInFlight`, `ccIdempotent`, `ccRetries` fields added
to `ClientConfig` and `defaultConfig`. They are NOT yet wired into Broker.hs.

### 1. Parse ProduceResponse error codes in Broker.hs dispatcher

File: `src/Kafka/Internal/Broker.hs`, function `dispatchResponse`, the
`InflightBatch` case (around line 467).

Currently:
```haskell
Just (InflightBatch _topic callbacks) ->
  -- assumes success
  mapM_ (\(_part, tmvars) -> ...) callbacks
```

Need to:
- Import `parseProduceResponse` from `Kafka.Internal.Produce.Response`
- Parse the response bytes with `Smith.parseByteArray parseProduceResponse`
- For each `ProducePartitionResponse`, check `prResponseErrorCode`
- If error code is 0: fill TMVar with `Right ()`
- If retriable error: re-enqueue for retry (see #2)
- If fatal error: fill TMVar with `Left (KafkaProtocolException ...)`
- Match partition responses to callbacks by `prResponsePartition`

### 2. Retry on retriable errors

Retriable Kafka errors (from Common.hs `KafkaProtocolError`):
- `LeaderNotAvailable` (5)
- `NotLeaderForPartition` (6)
- `RequestTimedOut` (7)
- `NotEnoughReplicas` (19)
- `NotEnoughReplicasAfterAppend` (20)
- `NetworkException` (13)

When a retriable error is received:
- Decrement retry counter
- If retries remaining: re-enqueue the `BrokerProduce` ops back to `beOps`
- If retries exhausted: fill TMVars with the error
- Use `ccRetries` from config (default 3)

Add retry tracking to `PendingMessage`:
```haskell
data PendingMessage = PendingMessage
  { pmPayload    :: !ByteArray
  , pmResult     :: !(TMVar (Either KafkaException ()))
  , pmRetriesLeft :: !Int
  }
```

### 3. In-flight request limit

Add to `BrokerEnv`:
```haskell
beInflightCount :: !(TVar Int)  -- current number of inflight produce requests
```

In `sendPartitionBatch`:
- Before sending, check `inflightCount < ccMaxInFlight`
- If at limit, block (STM retry) until a response is dispatched
- Increment on send, decrement on response dispatch

The sender loop should use STM to combine:
```haskell
atomically $ do
  count <- readTVar (beInflightCount env)
  check (count < ccMaxInFlight cfg)
  -- ... proceed with send
```

### 4. Idempotent producer (PID + epoch + sequences)

When `ccIdempotent` is True in config:

a) On producer startup, send `InitProducerId` request through any broker:
   - Use `initProducerIdRequest Nothing 30000` (non-transactional, 30s timeout)
   - Parse response to get `ipProducerId` and `ipProducerEpoch`
   - Store in producer state

b) Add to `BrokerEnv` or a shared producer state:
   ```haskell
   data IdempotentState = IdempotentState
     { isProducerId    :: !Int64
     , isProducerEpoch :: !Int16
     , isSequences     :: !(TVar (IntMap Int32))  -- partition → next sequence
     }
   ```

c) In `produceRequest`, pass PID/epoch/baseSequence instead of the defaults (-1):
   - `produceRequest` already has `defaultProducerId = -1`, `defaultProducerEpoch = -1`,
     `defaultBaseSequence = -1`. These need to become parameters.
   - Each batch for a partition gets the next sequence number
   - Sequence increments by the number of records in the batch

d) On `OutOfOrderSequenceNumber` or `DuplicateSequenceNumber` errors,
   the producer should bump the epoch (re-init PID).

Files to modify:
- `src/Kafka/Internal/Produce/Request.hs` — add PID/epoch/sequence params
- `src/Kafka/Internal/Broker.hs` — pass idempotent state to produceRequest
- `src/Kafka/Producer.hs` — init PID on startup, manage sequence state

## Next Priority

### 5. Consumer rewrite using KafkaClient

The current `src/Kafka/Consumer.hs` uses direct socket access (old `Kafka` type)
with an `MVar ()` mutex. It does NOT use the broker thread infrastructure.

Rewrite to:
- Use `KafkaClient` for all broker communication via `enqueueRequest`
- Route group ops (JoinGroup, SyncGroup, Heartbeat, etc.) to coordinator broker
- Route fetch requests to partition leader brokers
- Handle FindCoordinator response properly (create connection to coordinator)
- Support multiple topics
- Move old consumer to `Kafka.Consumer.Legacy`

### 6. Upgrade protocol versions

Compact encoding primitives are ready in Writer.hs and Combinator.hs.
To upgrade an API past its flexible threshold:
1. Change the apiVersion constant in the Request module
2. Switch from legacy `string`/`array` to `compactString`/`compactArray`
3. Add `taggedFields` at the end of each struct
4. Switch request header from v1 (INT16 string) to v2 (COMPACT_NULLABLE_STRING + taggedFields)
5. Update the Response parser similarly

Priority order for upgrades:
- ApiVersions v0 → v3 (flexible, needed for proper negotiation)
- Produce v7 → v9+ (flexible, adds error_message field)
- Fetch v10 → v12+ (flexible)

### 7. Compression codecs

`src/Kafka/Internal/Compression.hs` has stubs. Implement:
- Gzip: `zlib` package
- Snappy: `snappy` or FFI to libsnappy
- LZ4: `lz4-hs` package
- Zstd: `zstd` package

Wire into `Produce/Request.hs`: after building record batch payload,
compress it, set bits 0-2 of `recordBatchAttributes`.

### 8. Dynamic broker discovery

After `refreshTopicMetadata`, if the metadata response lists brokers
not in `kcBrokers`, create new `BrokerEnv` + start broker thread for them.
Requires parsing the metadata broker host:port and resolving to a `BrokerAddress`.

### 9. Performance

- Parameterize all request encoders with correlationId + clientId
  (eliminate `patchCorrelationIdLBS` copy of first chunk)
- Write ByteString-native CRC32C to eliminate `UnliftedArray ByteArray`
  from the produce path entirely
- Add benchmarks (`bench/Benchmark.hs`) comparing with hw-kafka-client
- Profile with `+RTS -hc` for allocation hotspots

## Reference

- Plan file: `/Users/jcardona/.claude/plans/prancy-petting-finch.md`
- Efficiency rules: `.claude/EFFICIENCY.md`
- librdkafka mock cluster API: `test/MockCluster.hs`
- librdkafka source: `../librdkafka/src/`
- hw-kafka-client (reference for FFI patterns): `~/arista/hw-kafka-client/`
