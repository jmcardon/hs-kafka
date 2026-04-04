{-# language BangPatterns #-}

-- | Compression support for Kafka record batches.
--
-- Compression is applied per-batch after all records are written,
-- matching librdkafka's approach (rdkafka_msgset_writer.c:1362).
-- If compressed size >= uncompressed size, or compression fails,
-- falls back to uncompressed (attribute 0) — matching librdkafka
-- (rdkafka_msgset_writer.c:1395).
--
-- Supported codecs:
--   - NoCompression (0): passthrough
--   - Gzip (1): via zlib
--   - Snappy (2): via snappy (FFI to C library)
--   - Lz4 (3): via lz4 (frame format, per KIP-57)
--   - Zstd (4): via zstd
module Kafka.Internal.Compression
  ( compressBatch
  , decompressBatch
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16)
import Data.Primitive.ByteArray (ByteArray, sizeofByteArray)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Codec.Compression.GZip as GZip
import qualified Codec.Compression.LZ4 as LZ4
import qualified Codec.Compression.Snappy as Snappy
import qualified Codec.Compression.Zstd as Zstd

import Data.Bytes.Types (Bytes(Bytes))
import qualified Data.Bytes

import Kafka.Internal.Config (Compression(..))

-- | Convert a ByteArray to a strict ByteString (one memcpy).
baToBS :: ByteArray -> ByteString
baToBS ba = Data.Bytes.toByteString (Bytes ba 0 (sizeofByteArray ba))

-- | Convert a strict ByteString to a ByteArray.
bsToBA :: ByteString -> ByteArray
bsToBA bs =
  let bytes = Data.Bytes.fromByteString bs
  in Data.Bytes.toByteArrayClone bytes

-- | Compress a batch payload. Returns the (possibly compressed) bytes
-- and the compression attribute to set in the record batch.
-- On failure or if compressed >= original, falls back to uncompressed.
compressBatch :: Compression -> ByteArray -> (ByteArray, Int16)
compressBatch NoCompression !payload = (payload, 0)
compressBatch Gzip !payload =
  let original = baToBS payload
      compressed = BSL.toStrict (GZip.compress (BSL.fromStrict original))
  in if BS.length compressed < BS.length original
     then (bsToBA compressed, 1)
     else (payload, 0)
compressBatch Snappy !payload =
  let original = baToBS payload
      compressed = Snappy.compress original
  in if BS.length compressed < BS.length original
     then (bsToBA compressed, 2)
     else (payload, 0)
compressBatch Lz4 !payload =
  let original = baToBS payload
  in case LZ4.compress original of
    Just compressed
      | BS.length compressed < BS.length original -> (bsToBA compressed, 3)
    _ -> (payload, 0)
compressBatch Zstd !payload =
  let original = baToBS payload
      compressed = Zstd.compress 3 original
  in if BS.length compressed < BS.length original
     then (bsToBA compressed, 4)
     else (payload, 0)

-- | Decompress a batch payload given the compression codec attribute.
decompressBatch :: Int16 -> ByteString -> Either String ByteString
decompressBatch 0 bs = Right bs
decompressBatch 1 bs = Right $ BSL.toStrict (GZip.decompress (BSL.fromStrict bs))
decompressBatch 2 bs = Right $ Snappy.decompress bs
decompressBatch 3 bs = case LZ4.decompress bs of
  Nothing -> Left "LZ4 decompression failed"
  Just r  -> Right r
decompressBatch 4 bs = case Zstd.decompress bs of
  Zstd.Decompress r -> Right r
  Zstd.Error msg    -> Left ("Zstd decompression failed: " ++ msg)
  Zstd.Skip         -> Right BS.empty
decompressBatch n _  = Left ("unknown compression codec: " ++ show n)
