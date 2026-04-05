{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Produce v9 request encoding.
--
-- The entire request is built as a single strict ByteString:
--   1. Record batch built via RecordBatch (single pinned allocation)
--   2. Protocol header written via Writer (BuildR, ~40 bytes)
--   3. Assembled: size prefix + header + batch + tagged fields
--
-- Correlation ID is baked in from the start — no post-hoc patching.
module Kafka.Internal.Produce.Request
  ( buildProduceRequest
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int16, Int32, Int64)

import Kafka.Common (TopicName(..))
import Kafka.Internal.Compression (compressBatch)
import Kafka.Internal.Config (Compression(..))
import Kafka.Internal.RecordBatch (buildRecordBatch, buildRecords, wrapRecordBatch)
import Kafka.Internal.Writer

produceApiVersion :: Int16
produceApiVersion = 9

produceApiKey :: Int16
produceApiKey = 0

-- | Build a complete Produce v9 request as a strict ByteString.
--
-- Correlation ID is baked in — no patching needed. The caller provides
-- the corrId to use. Returns a strict ByteString ready for sendAll.
buildProduceRequest ::
     Int32           -- ^ correlation ID (baked in, not patched)
  -> Int16           -- ^ acks
  -> ByteString      -- ^ client ID
  -> Int             -- ^ timeout (ms)
  -> TopicName
  -> Int32           -- ^ partition
  -> Int64           -- ^ producerId (-1 for non-idempotent)
  -> Int16           -- ^ producerEpoch (-1 for non-idempotent)
  -> Int32           -- ^ baseSequence (-1 for non-idempotent)
  -> Compression
  -> [ByteString]    -- ^ message payloads
  -> ByteString
buildProduceRequest !corrId !acksVal !cid !timeout !topic !partition
    !producerId !producerEpoch !baseSeq !compression payloads =
  let
    !n = length payloads

    -- Build the record batch
    !batchBS = case compression of
      NoCompression ->
        buildRecordBatch producerId producerEpoch baseSeq 0 payloads
      _ ->
        let !rawRecords = buildRecords payloads
            (!compRecords, !attr) = compressBatch compression rawRecords
        in if attr == 0
          then buildRecordBatch producerId producerEpoch baseSeq 0 payloads
          else wrapRecordBatch producerId producerEpoch baseSeq attr n compRecords

    !batchLen = BS.length batchBS

    -- Protocol prefix with corrId baked in
    !prefixBuilder =
      int16 produceApiKey
      <> int16 produceApiVersion
      <> int32 corrId
      <> string cid
      <> taggedFields
      <> compactNullableString Nothing
      <> int16 acksVal
      <> int32 (fromIntegral timeout)
      <> unsignedVarInt 2
      <> compactString (let TopicName tn = topic in tn)
      <> unsignedVarInt 2
      <> int32 partition
      <> unsignedVarInt (batchLen + 1)

    !suffixBuilder = taggedFields <> taggedFields <> taggedFields

    !prefixBytes = toLazyByteString prefixBuilder
    !suffixBytes = toLazyByteString suffixBuilder
    !fullBody = prefixBytes <> BSL.fromStrict batchBS <> suffixBytes
    !bodySize = fromIntegral (BSL.length fullBody) :: Int32

    -- Final: size prefix + body, materialized as strict ByteString
  in BSL.toStrict (toLazyByteString (int32 bodySize) <> fullBody)
