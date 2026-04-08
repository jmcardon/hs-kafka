{-# language BangPatterns #-}

-- | Compression for Kafka record batches. All operations on ByteString.
--
-- Falls back to uncompressed if compressed >= original (matches librdkafka).
module Kafka.Internal.Compression
  ( compressBatch
  , decompressBatch
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Codec.Compression.GZip as GZip
import qualified Codec.Compression.LZ4 as LZ4
import qualified Codec.Compression.Snappy as Snappy
import qualified Codec.Compression.Zstd as Zstd

import Kafka.Internal.Config (Compression(..))

-- | Compress a batch payload. Returns (possibly compressed bytes, attribute).
-- Falls back to uncompressed (attribute 0) if compression doesn't help.
compressBatch :: Compression -> ByteString -> (ByteString, Int16)
compressBatch NoCompression !payload = (payload, 0)
compressBatch Gzip !payload =
  let !compressed = BSL.toStrict (GZip.compress (BSL.fromStrict payload))
  in if BS.length compressed < BS.length payload
     then (compressed, 1) else (payload, 0)
compressBatch Snappy !payload =
  let !compressed = Snappy.compress payload
  in if BS.length compressed < BS.length payload
     then (compressed, 2) else (payload, 0)
compressBatch Lz4 !payload =
  case LZ4.compress payload of
    Just compressed
      | BS.length compressed < BS.length payload -> (compressed, 3)
    _ -> (payload, 0)
compressBatch Zstd !payload =
  let !compressed = Zstd.compress 3 payload
  in if BS.length compressed < BS.length payload
     then (compressed, 4) else (payload, 0)

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
