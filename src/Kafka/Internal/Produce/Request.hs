{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Produce.Request
  ( produceRequest
  , produceRequestIdempotent
  , produceRequestCompressed
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Bytes.Types (Bytes(Bytes))
import qualified Data.Bytes
import Data.Int (Int16, Int32, Int64)
import Data.Primitive.ByteArray (ByteArray, sizeofByteArray)
import Data.Primitive.Unlifted.Array (UnliftedArray, sizeofUnliftedArray)

import Kafka.Common
import Kafka.Internal.Compression (compressBatch)
import Kafka.Internal.Config (Compression(..))
import Kafka.Internal.RecordBatch (buildRecordBatch, buildRecords, wrapRecordBatch)
import Kafka.Internal.Writer

produceApiVersion :: Int16
produceApiVersion = 9

produceApiKey :: Int16
produceApiKey = 0

-- | Build a Produce v9 request.
--
-- The record batch is built via RecordBatch (single pinned allocation).
-- For compression, records are built separately, compressed, then wrapped.
buildProducePayload ::
     Int16 -> ByteString -> Int -> TopicName -> Int32
  -> Int64 -> Int16 -> Int32 -> Compression
  -> UnliftedArray ByteArray -> BSL.ByteString
buildProducePayload !acksVal !cid !timeout !topic !partition
    !producerId !producerEpoch !baseSeq !compression payloads =
  let
    !n = sizeofUnliftedArray payloads

    !batchBS = case compression of
      NoCompression ->
        buildRecordBatch producerId producerEpoch baseSeq 0 payloads
      _ ->
        let !rawRecords = buildRecords payloads
            !rawRecordsBA = bsToBA rawRecords
            (!compRecordsBA, !attr) = compressBatch compression rawRecordsBA
        in if attr == 0
          then buildRecordBatch producerId producerEpoch baseSeq 0 payloads
          else wrapRecordBatch producerId producerEpoch baseSeq attr n
                 (baToBS compRecordsBA)

    !batchLen = BS.length batchBS

    !prefixBuilder =
      int16 produceApiKey
      <> int16 produceApiVersion
      <> int32 correlationId
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

  in toLazyByteString (int32 bodySize) <> fullBody

-- Helpers for compression path (ByteArray ↔ ByteString).
-- These exist only because compressBatch still uses ByteArray.
baToBS :: ByteArray -> ByteString
baToBS ba = Data.Bytes.toByteString (Bytes ba 0 (sizeofByteArray ba))

bsToBA :: ByteString -> ByteArray
bsToBA = Data.Bytes.toByteArrayClone . Data.Bytes.fromByteString

-- | Non-idempotent, no compression.
produceRequest ::
     Int16 -> ByteString -> Int -> TopicName -> Int32
  -> UnliftedArray ByteArray -> BSL.ByteString
produceRequest acksVal cid timeout topic partition payloads =
  buildProducePayload acksVal cid timeout topic partition
    (-1) (-1) (-1) NoCompression payloads

-- | Idempotent, no compression.
produceRequestIdempotent ::
     Int16 -> ByteString -> Int -> TopicName -> Int32
  -> Int64 -> Int16 -> Int32
  -> UnliftedArray ByteArray -> BSL.ByteString
produceRequestIdempotent acksVal cid timeout topic partition
    producerId producerEpoch baseSeq payloads =
  buildProducePayload acksVal cid timeout topic partition
    producerId producerEpoch baseSeq NoCompression payloads

-- | With compression.
produceRequestCompressed ::
     Int16 -> ByteString -> Int -> TopicName -> Int32
  -> Int64 -> Int16 -> Int32 -> Compression
  -> UnliftedArray ByteArray -> BSL.ByteString
produceRequestCompressed = buildProducePayload
