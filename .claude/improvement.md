# Produce Path Performance Improvements

Baseline: ~10x slower than hw-kafka-client (librdkafka FFI) on 1000 x 100B messages.
Root cause: ~15,000+ heap allocations per 1000-message batch.

librdkafka achieves zero per-message allocations by writing directly into a
pre-sized mutable buffer. We rebuild the encoding path to do the same.

---

## Issue 1: `makeRecordMetadata` — ~10 allocations PER MESSAGE [CRITICAL]

**File:** `src/Kafka/Internal/Produce/Request.hs:73-86`

**Problem:** For each record, builds 5 zigzag ByteArrays, a 1-byte ByteArray
for attributes, folds them with `<>` (4 intermediate ByteArrays), then
concatenates the length prefix (1 more). ~10 allocations per message.
1000 messages = 10,000 allocations.

```haskell
makeRecordMetadata index content =
  let
    recordLength = zigzag (sizeofByteArray metadataContent + sizeofByteArray content + 1)
    metadataContent = fold
      [ byteArrayFromList [defaultRecordAttributes]
      , zigzag defaultTimestampDelta
      , zigzag index
      , zigzag (-1)
      , zigzag (sizeofByteArray content)
      ]
  in recordLength <> metadataContent
```

**librdkafka equivalent:** `rdkafka_msgset_writer.c:673-768`
Pre-encodes all varints into `char[10]` stack buffers, computes Length from
their known sizes, then writes everything sequentially into the output buffer.
Zero allocations.

**Fix:** Replace with a function that writes directly into a `MutableByteArray`
at a given offset, returning the new offset. No intermediate ByteArrays.

```haskell
writeRecordMetadata :: MutableByteArray s -> Int -> Int -> ByteArray -> ST s Int
writeRecordMetadata buf off index payload = do
  -- Pre-compute varint sizes (just arithmetic, no allocs)
  let tsSize     = zigzagSize 0           -- timestampDelta=0 -> 1 byte
      offSize    = zigzagSize index        -- offsetDelta
      keySize    = zigzagSize (-1)         -- no key -> 1 byte
      valSize    = zigzagSize (sizeofByteArray payload)
      bodySize   = 1 {-attrs-} + tsSize + offSize + keySize + valSize
                   + sizeofByteArray payload + 1 {-headerCount=0-} + 1 {-trailing zero-}
      lenSize    = zigzagSize bodySize
  -- Write Length varint
  off1 <- writeZigzag buf off bodySize
  -- Write Attributes (0)
  writeByteArray buf off1 (0 :: Word8)
  -- Write TimestampDelta (zigzag 0 = 0x00)
  off2 <- writeZigzag buf (off1 + 1) 0
  -- Write OffsetDelta
  off3 <- writeZigzag buf off2 index
  -- Write KeyLen (-1)
  off4 <- writeZigzag buf off3 (-1)
  -- Write ValueLen
  off5 <- writeZigzag buf off4 (sizeofByteArray payload)
  -- Write Value (copyByteArray, no intermediate)
  copyByteArray buf off5 payload 0 (sizeofByteArray payload)
  let off6 = off5 + sizeofByteArray payload
  -- Write HeaderCount (zigzag 0)
  writeZigzag buf off6 0
```

**Status:** TODO

---

## Issue 2: `zigzag` / `varint` — Absurd Allocation Chain [CRITICAL]

**File:** `src/Kafka/Internal/Zigzag.hs:12-29`

**Problem:** Each call builds: `NonEmpty Word8` via `unfoldr` -> second
`NonEmpty` via `setMsb` -> `[Word8]` via `toList` -> `ByteArray` via
`byteArrayFromList`. Four intermediate data structures to produce 1-2 bytes.
Called 5x per message = 5000 times for 1000 messages.

```haskell
varint n =
  let chunks = setMsb $ chunk n
      setMsb (x :| xs) = let rest = setMsb <$> nonEmpty xs
                          in case rest of { Just r -> (128 .|. x) <| r; Nothing -> pure x }
  in byteArrayFromList (toList chunks)
```

**librdkafka equivalent:** `rdvarint.h:48-72`
Tight loop writing bytes directly into a `char[]` buffer on the stack.

**Fix:** Two new functions, no allocations:

```haskell
-- Write zigzag-encoded varint directly into mutable buffer. Returns new offset.
writeZigzag :: MutableByteArray s -> Int -> Int -> ST s Int
writeZigzag buf off n = writeUvarint buf off (zigzagEncode n)
  where
    zigzagEncode x | x >= 0    = x * 2
                   | otherwise = (-x) * 2 - 1

writeUvarint :: MutableByteArray s -> Int -> Int -> ST s Int
writeUvarint buf off n
  | n < 0x80  = writeByteArray buf off (fromIntegral n :: Word8) >> pure (off + 1)
  | otherwise = do
      writeByteArray buf off (fromIntegral (n .&. 0x7F .|. 0x80) :: Word8)
      writeUvarint buf (off + 1) (n `shiftR` 7)

-- Pure size calculator (for pre-sizing buffers). No allocations.
zigzagSize :: Int -> Int
zigzagSize n = uvarintSize (zigzagEncode n)

uvarintSize :: Int -> Int
uvarintSize n
  | n < 0x80       = 1
  | n < 0x4000     = 2
  | n < 0x200000   = 3
  | n < 0x10000000 = 4
  | otherwise       = 5
```

**Status:** TODO

---

## Issue 3: `gatherChunks` — O(n^2) ByteArray Concatenation [CRITICAL]

**File:** `src/Kafka/Internal/Produce/Request.hs:162-164`

**Problem:** `foldrUnliftedArray (<>) mempty` over 3000 ByteArrays (1000
records x 3 chunks each). Each `<>` allocates a new ByteArray and copies
both operands. Right fold means the accumulator grows from right to left,
copying increasingly large prefixes. Total bytes copied ~ O(n * totalSize).

```haskell
gatherChunks :: UnliftedArray ByteArray -> ByteArray
gatherChunks = foldrUnliftedArray (<>) mempty
```

**librdkafka equivalent:** No equivalent needed — records are written
directly into the output buffer.

**Fix:** Eliminated entirely by Issues 1-2 fix (write records directly into
the mutable buffer). If needed as a standalone fix:

```haskell
gatherChunks :: UnliftedArray ByteArray -> ByteArray
gatherChunks arr = runST $ do
  let totalSize = foldrUnliftedArray (\e acc -> acc + sizeofByteArray e) 0 arr
  buf <- newByteArray totalSize
  let go !off !i
        | i >= sizeofUnliftedArray arr = pure ()
        | otherwise = do
            let chunk = indexUnliftedArray arr i
                len = sizeofByteArray chunk
            copyByteArray buf off chunk 0 len
            go (off + len) (i + 1)
  go 0 0
  unsafeFreezeByteArray buf
```

Single allocation, single pass, no intermediates.

**Status:** TODO

---

## Issue 4: `buildToBA` — Triple Copy via `BS.unpack` [CRITICAL]

**File:** `src/Kafka/Internal/Produce/Request.hs:70-71`

**Problem:** Converts BuildR to ByteArray via:
`BuildR -> BSL.ByteString -> BS.ByteString -> [Word8] -> ByteArray`

```haskell
buildToBA b = bsToBA (BSL.toStrict (toLazyByteString b))
bsToBA bstr = byteArrayFromList (BS.unpack bstr)
```

The `BS.unpack` is especially bad: creates a linked list of boxed Word8
(~24 bytes per element). For 40-byte postCrc: 960 bytes of garbage.

Called twice per batch (preCrc=9 bytes, postCrc=40 bytes).

**Fix:** Write the record batch header directly into the mutable buffer too.
The preCrc and postCrc fields are fixed-size (9 + 40 = 49 bytes), so we
can write them with direct `writeByteArray` calls — no BuildR needed.

```haskell
writeRecordBatchHeader :: MutableByteArray s -> Int
  -> Int64 -> Int16 -> Int32 -> Int16 -> Int -> Int32 -> ByteArray
  -> ST s ()
writeRecordBatchHeader buf off producerId epoch baseSeq batchAttrs
    recordCount crc records = do
  -- preCrc (9 bytes): baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) + crc(4)
  writeBE64 buf off 0              -- baseOffset
  writeBE32 buf (off+8) batchLen   -- batchLength
  writeBE32 buf (off+12) 0         -- partitionLeaderEpoch
  writeByteArray buf (off+16) (2 :: Word8)  -- magic
  writeBE32 buf (off+17) crc       -- CRC (placeholder, updated later)
  -- postCrc (40 bytes): attributes(2) + lastOffsetDelta(4) + ...
  writeBE16 buf (off+21) batchAttrs
  writeBE32 buf (off+23) (recordCount - 1)  -- lastOffsetDelta
  writeBE64 buf (off+27) 0         -- firstTimestamp
  writeBE64 buf (off+35) 0         -- maxTimestamp
  writeBE64 buf (off+43) producerId
  writeBE16 buf (off+51) epoch
  writeBE32 buf (off+53) baseSeq
  writeBE32 buf (off+57) recordCount
```

**Status:** TODO

---

## Issue 5: `patchCorrelationIdLBS` — Post-hoc ByteString Surgery [HIGH]

**File:** `src/Kafka/Internal/Broker.hs:632-655`

**Problem:** After building the entire request as a `BSL.ByteString`, we
destructure it into chunks, slice at byte offsets, concatenate pieces, and
rebuild. ~5 allocations per request.

```haskell
patchCorrelationIdLBS corrId lbs =
  ...BSL.toChunks...BS.take...BS.drop...BS.concat...BSL.fromChunks...
```

**librdkafka equivalent:** `rd_kafka_buf_update_i32(rkbuf, offset, value)`
Pokes 4 bytes directly at a known offset. Zero allocations.

**Fix:** Two options:
  (a) Accept the correlation ID as a parameter to `buildProducePayload` so
      it's baked into the BuildR from the start. No patching needed.
  (b) If we move to a MutableByteArray-based builder for the whole request,
      write the corrId directly at offset 8 (after 4-byte size prefix +
      2-byte apiKey + 2-byte apiVersion).

Option (a) is simpler and backward-compatible.

**Status:** TODO

---

## Issue 6: Request Assembly — Multiple Materialization Passes [HIGH]

**File:** `src/Kafka/Internal/Produce/Request.hs:227-235`

**Problem:** Three separate materializations, a BA->BS copy, lazy BSL length
traversal, then another materialization for the size prefix.

```haskell
prefixBytes = toLazyByteString prefixBuilder
recordBatchBS = BSL.fromStrict (baToBS recordBatchSection)  -- copy!
suffixBytes = toLazyByteString suffixBuilder
fullBody = prefixBytes <> recordBatchBS <> suffixBytes
bodySize = fromIntegral (BSL.length fullBody) :: Int32      -- traverse!
in toLazyByteString (int32 bodySize) <> fullBody
```

**librdkafka equivalent:** Pre-calculates size from tracked offsets, writes
size field as placeholder and updates in-place.

**Fix:** With the MutableByteArray approach, the entire request (prefix +
record batch + suffix) is in one buffer. We know the exact size from the
write offset. Write the size prefix at offset 0 and we're done.

For the prefix/suffix (which use BuildR for protocol header fields), we
can either:
  - Also move them into the mutable buffer (best perf)
  - Or keep BuildR for the ~40 byte prefix and send it as a separate iovec
    via `sendMany` (scatter-gather I/O, good enough)

**Status:** TODO

---

## Issue 7: `zero` ByteArray — Per-Batch runST [LOW]

**File:** `src/Kafka/Internal/Produce/Request.hs:192-195`

**Problem:** 1-byte ByteArray rebuilt every batch instead of being a CAF.

```haskell
zero = runST $ do
  ba <- newByteArray 1
  writeByteArray ba 0 (0 :: Word8)
  unsafeFreezeByteArray ba
```

**Fix:** Top-level constant:

```haskell
{-# NOINLINE zeroByte #-}
zeroByte :: ByteArray
zeroByte = runST $ do
  ba <- newByteArray 1
  writeByteArray ba 0 (0 :: Word8)
  unsafeFreezeByteArray ba
```

Eliminated entirely if we adopt the mutable buffer approach (the trailing
zero byte is just `writeByteArray buf off (0 :: Word8)`).

**Status:** TODO

---

## Issue 8: `bsToBA` using `BS.unpack` everywhere [MODERATE]

**Files:** Multiple — `Produce/Request.hs:67`, `Compression.hs:42-44`,
`Response.hs:45-46`, `Consumer.hs:83`

**Problem:** `byteArrayFromList (BS.unpack bstr)` creates a linked list of
boxed Word8 values. Each cons cell is ~24 bytes. A 100-byte ByteString
produces ~2400 bytes of garbage just for the intermediate list.

**Fix:** Use `Data.Bytes.fromByteString` + `Data.Bytes.toByteArrayClone`
(already used in Compression.hs:42-44) or better, use `copyByteArray` with
`byteStringToByteArray#` from GHC primitives.

```haskell
bsToBA :: ByteString -> ByteArray
bsToBA bs =
  let bytes = Data.Bytes.fromByteString bs
  in Data.Bytes.toByteArrayClone bytes
```

**Status:** TODO

---

## Issue 9: `Map.alter` Per Message in Batch Accumulation [MODERATE]

**File:** `src/Kafka/Internal/Broker.hs:121-129`

**Problem:** Every message does `Map.alter` on a `Map (TopicName, Int32)`.
For 1000 messages to the same partition, this is 1000 map lookups that all
find the same key. The messages are stored in reverse order, then reversed
again at line 406.

**Fix:** Use `Map.insertWith (flip (++))` or better, switch to a
`Map (TopicName, Int32) (Seq PendingMessage)` to avoid the reversal. Or
for the common single-partition case, special-case the accumulator.

Actually the best fix: use `Map.insertWith` with a DList or just accept
the cons-then-reverse pattern (it's O(n) total, just with a constant factor).
The Map.alter overhead is the real issue — consider `HashMap` for O(1)
amortized lookup.

**Status:** TODO

---

## Issue 10: `NBSL.sendAll` vs scatter-gather `sendMany` [LOW]

**File:** `src/Kafka/Internal/Broker.hs:443`

**Problem:** `NBSL.sendAll` sends a lazy ByteString which may internally
iterate chunk-by-chunk making multiple `send()` syscalls.

**librdkafka equivalent:** `rdkafka_transport.c:127-152` converts all buffer
segments to `struct iovec` and calls `sendmsg()` once.

**Fix:** Use `Network.Socket.ByteString.sendMany` which uses `writev()`
(scatter-gather) to send multiple chunks in a single syscall. Or convert
the final request to a strict ByteString (single `send()` call).

**Status:** TODO

---

## Implementation Strategy

**Phase A — Single-allocation record batch builder (Issues 1-4, 7):**
Create `src/Kafka/Internal/RecordBatch.hs` with a `MutableByteArray`-based
builder. This one change eliminates ~14,000 of the ~15,000 allocations.

**Phase B — Correlation ID threading (Issue 5):**
Pass corrId into `buildProducePayload`. Small API change.

**Phase C — Full request in one buffer (Issue 6):**
Write the entire produce request (header + record batch + suffix) into a
single `MutableByteArray`. Convert to `ByteString` once for sending.

**Phase D — Cleanup (Issues 8-10):**
Fix `bsToBA`, batch accumulation, and scatter-gather I/O.

Expected result: ~3000x fewer allocations in the produce hot path.
Target: match or beat hw-kafka-client throughput.
