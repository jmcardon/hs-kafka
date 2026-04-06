{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

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
import Kafka.Internal.RecordBatch (RecordInput(..), buildRecordBatch, buildRecords, wrapRecordBatch)
import Kafka.Internal.Writer

produceApiVersion :: Int16
produceApiVersion = 9

produceApiKey :: Int16
produceApiKey = 0

-- | Build a Produce v9 request as a strict ByteString.
buildProduceRequest ::
     Int32           -- ^ correlation ID
  -> Int16           -- ^ acks
  -> ByteString      -- ^ client ID
  -> Int             -- ^ timeout (ms)
  -> TopicName
  -> Int32           -- ^ partition
  -> Int64           -- ^ producerId
  -> Int16           -- ^ producerEpoch
  -> Int32           -- ^ baseSequence
  -> Compression
  -> Int64           -- ^ firstTimestamp (epoch ms, 0 for broker-assigned)
  -> [RecordInput]   -- ^ records (key, value, headers, timestamp delta)
  -> ByteString
buildProduceRequest !corrId !acksVal !cid !timeout !topic !partition
    !producerId !producerEpoch !baseSeq !compression !firstTs records =
  let
    !n = length records

    !batchBS = case compression of
      NoCompression ->
        buildRecordBatch producerId producerEpoch baseSeq 0 firstTs records
      _ ->
        let !rawRecords = buildRecords records
            (!compRecords, !attr) = compressBatch compression rawRecords
        in if attr == 0
          then buildRecordBatch producerId producerEpoch baseSeq 0 firstTs records
          else wrapRecordBatch producerId producerEpoch baseSeq attr n firstTs compRecords

    !batchLen = BS.length batchBS

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

    -- Compute body size arithmetically (no materialization needed)
    TopicName !tn = topic
    !topicLen = BS.length tn
    !prefixSize = 24 + BS.length cid
                + uvarSize (topicLen + 1) + topicLen
                + uvarSize (batchLen + 1)
    !suffixSize = 3  -- three taggedFields (0x00 each)
    !bodySize = fromIntegral (prefixSize + batchLen + suffixSize) :: Int32

    -- Build entire request as a single BuildR, materialize once
    !fullRequest = int32 bodySize
                <> prefixBuilder
                <> bs batchBS
                <> suffixBuilder

  in BSL.toStrict (toLazyByteString fullRequest)

-- | Unsigned varint size (for computing prefix size).
uvarSize :: Int -> Int
uvarSize n
  | n < 0x80       = 1
  | n < 0x4000     = 2
  | n < 0x200000   = 3
  | n < 0x10000000 = 4
  | otherwise       = 5
{-# INLINE uvarSize #-}
