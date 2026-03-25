{-# language BangPatterns #-}

-- | Compression support for Kafka record batches.
--
-- Compression is applied per-batch after all records are written,
-- matching librdkafka's approach (rdkafka_msgset_writer.c:1362).
-- If compressed size > uncompressed size, falls back to uncompressed.
--
-- Currently only NoCompression is implemented. Codec implementations
-- (gzip, snappy, lz4, zstd) will be added as needed.
module Kafka.Internal.Compression
  ( compressBatch
  ) where

import Data.Int (Int16)
import Data.Primitive.ByteArray (ByteArray)

import Kafka.Internal.Config (Compression(..))

-- | Compress a batch payload. Returns the (possibly compressed) bytes
-- and the compression attribute to set in the record batch.
-- Falls back to uncompressed if compression would increase size.
compressBatch :: Compression -> ByteArray -> (ByteArray, Int16)
compressBatch NoCompression !payload = (payload, 0)
compressBatch Gzip   !payload = (payload, 0) -- TODO: implement with zlib
compressBatch Snappy !payload = (payload, 0) -- TODO: implement with snappy
compressBatch Lz4    !payload = (payload, 0) -- TODO: implement with lz4
compressBatch Zstd   !payload = (payload, 0) -- TODO: implement with zstd
