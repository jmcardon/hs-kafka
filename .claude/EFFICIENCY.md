# Pre-Commit Efficiency Review — READ BEFORE EVERY COMMIT

## Cardinal Rule

Do NOT make any change that makes code efficiency worse without an explicit
justification comment in the code explaining WHY the regression is acceptable.

## The Produce Path is Sacred

The produce path is the hot path. Every message goes through it. The original
hs-kafka design was carefully optimized:

- `UnliftedArray ByteArray` for scatter-gather encoding — payloads are NEVER copied
  between creation and kernel send. Do not replace this with `[ByteString]` or
  `BSL.ByteString` unless you can prove zero-copy is preserved.
- `patchCorrelationId` copies only the ~50-byte header chunk, NEVER the payload
  chunks. Any correlation ID patching that forces the entire request is a bug.
- CRC32-C (`castagnoli`) operates on `ByteArray` natively. Do not convert payloads
  to `ByteString` just to CRC them.

## Where Copies Are Allowed

1. **Network send boundary** — ONE gather-copy from `UnliftedArray ByteArray` into
   a contiguous `ByteString` for `NBS.sendAll`. This is unavoidable because
   `network` speaks `ByteString` (pinned) and `ByteArray` is unpinned.
2. **Network receive boundary** — ONE copy from `ByteString` (recv) to `ByteArray`
   (bytesmith parser). Unavoidable.
3. **Small control requests** (metadata, fetch, offset, group ops) — these are <1KB.
   `BuildR → toLazyByteString → toStrict` is fine. Don't over-optimize these.
4. **Topic names, client IDs** — small strings, copy freely.

## Where Copies Are NOT Allowed

1. Do NOT `BSL.toStrict` a produce request. That's copying potentially MB of payload
   data for no reason.
2. Do NOT convert each payload `ByteArray → ByteString` individually and then
   concatenate. Use a single gather operation at the send boundary.
3. Do NOT introduce `BSL.ByteString` in any data type or public API. It's an
   implementation detail of `proto3-wire`'s `BuildR`, confined to `Writer.hs`.

## Type Discipline

| Context | Type | Why |
|---------|------|-----|
| Produce payloads (internal CRC) | `UnliftedArray ByteArray` | castagnoli needs ByteArray |
| Produce request (final output) | `BSL.ByteString` | Sent via NBSL.sendAll |
| Control requests (encoded) | `BSL.ByteString` | Small, BuildR output via buildRequest |
| Encoding primitives | `BuildR` (proto3-wire) | Reverse builder, efficient length-prefix |
| Network send | `BSL.ByteString` | `NBSL.sendAll` uses writev on chunks |
| Network recv | `ByteString` (strict) | `network` library API |
| Parser input | `ByteArray` | `bytesmith` API |
| Topic names, client IDs | `ByteString` | User-facing, standard type |
| CRC computation | `ByteArray` / `Bytes` | `castagnoli` API |

## Gather-Send Pattern

For sending `UnliftedArray ByteArray` over `network`:

```haskell
-- ONE allocation + ONE memcpy + ONE syscall
sendGathered :: Socket -> UnliftedArray ByteArray -> IO ()
sendGathered sock chunks = do
  let totalLen = sumSizes chunks
  bs <- BSI.create totalLen $ \ptr ->
    copyChunksToPtr chunks ptr 0
  NBS.sendAll sock bs
```

This is the ONLY acceptable way to bridge `UnliftedArray ByteArray` to `network`.
Do not use `BSL.fromChunks [baToBS chunk | chunk <- ...]` — that's N copies + N
allocations vs 1.

## Future: ByteString-native CRC32C

Once we rewrite castagnoli (or write our own CRC32C over ByteString), the
produce path can drop `UnliftedArray ByteArray` entirely. Everything becomes
ByteString end-to-end: BuildR → BSL.ByteString → CRC → send. Zero copies
from builder to kernel.
